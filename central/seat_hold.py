"""
좌석 선점(결제 단계) — 중앙  [MACRO3_V1]   docs/research/12-macro-3layer.md §4

  창 마감 → 승자 = 결제 대기 (HOLD_SEC 안에 결제) → 결제(POST /payment) → 결제 완료
                                   └→ 기한 초과 → 좌석을 돌려주고(available) 그 계정의 미결제 횟수 +1

결제 대기 중에도 좌석은 다른 사람에게 '확정'으로 보인다 (seat-status confirmed + paid=false + hold_until).
미결제 횟수는 2차 판정(second_layer ③ 좌석 선점)이 읽는다.

Redis 판정 경로(STATE_BACKEND=redis)에서만 켠다. 결제 대기와 만료 처리는 Redis 에 두어
여러 워커 중 누구든 처리할 수 있고, 만료 하나는 정확히 한 워커만 처리한다(ZREM 반환값 = 처리 권한).

[Redis 키]
  hold:{seat}   해시 {rid: 승자 요청, until: 결제 기한(epoch 초), paid: "1"}
  holds         결제 기한 ZSET (member=seat, score=until)
  l2:req:{rid}  요청 → 계정 (2차가 판정할 때 남긴다)
"""
import asyncio
import os
import time

import redis.asyncio as aioredis
from prometheus_client import Counter

HOLD_SEC = float(os.environ.get("HOLD_SEC", "120"))

hold_total = Counter("central_hold_total", "결제 대기 결과", ["result"])   # held / paid / expired

_r = None
_release_cb = None      # (seat_id, rid) -> 판정 상태·DB 에서 확정을 지우고 available 을 방송 (central 이 넘긴다)
_payload_cb = None      # seat_id -> 그 좌석의 confirmed 방송 내용 (결제 완료 재방송용)
_publish = None         # seat-status 발행
_on_unpaid = None       # 계정 -> 미결제 +1 (second_layer.add_unpaid)
_detlog = None          # detection-log 발행
_task = None


def enabled() -> bool:
    return _r is not None and HOLD_SEC > 0


async def start(redis_url, release_cb, payload_cb, publish, on_unpaid, detlog):
    global _r, _release_cb, _payload_cb, _publish, _on_unpaid, _detlog, _task
    if HOLD_SEC <= 0 or not redis_url:
        print("[central] 좌석 선점(결제 대기) 꺼짐", flush=True)
        return
    try:
        r = aioredis.from_url(redis_url, decode_responses=True)
        await r.ping()
    except Exception as e:
        print(f"[central] 좌석 선점을 켜지 못함: {e}", flush=True)
        return
    _r, _release_cb, _payload_cb, _publish, _on_unpaid, _detlog = r, release_cb, payload_cb, publish, on_unpaid, detlog
    _task = asyncio.create_task(_sweeper())
    print(f"[central] 좌석 선점(결제 대기 {HOLD_SEC:g}초) 시작 [MACRO3_V1]", flush=True)


async def annotate(payload: dict) -> dict:
    """
    confirmed 방송 직전에 부른다. 처음 보는 확정이면 결제 대기를 등록하고,
    방송 내용에 paid / hold_until 을 붙인다. 재방송(아웃박스)에도 같은 값이 붙는다(멱등).
    """
    if not enabled() or payload.get("status") != "confirmed":
        return payload
    seat, rid = payload["seat_id"], payload.get("winner_request_id")
    try:
        key = f"hold:{seat}"
        until = time.time() + HOLD_SEC
        if await _r.hsetnx(key, "rid", rid):
            pipe = _r.pipeline(transaction=False)
            pipe.hset(key, "until", until)
            pipe.zadd("holds", {seat: until})
            pipe.expire(key, int(HOLD_SEC) + 3600)
            await pipe.execute()
            hold_total.labels(result="held").inc()
        h = await _r.hgetall(key)
        if h.get("rid") == rid:
            payload = {**payload, "paid": h.get("paid") == "1", "hold_until": float(h.get("until") or until)}
    except Exception as e:
        print(f"[central] 결제 대기 등록 실패 (seat={seat}): {e}", flush=True)
    return payload


async def pay(seat_id: str, request_id: str, account_id) -> tuple:
    """결제. 반환 (HTTP 코드, 내용)."""
    if not enabled():
        return 200, {"status": "paid", "seat_id": seat_id, "hold": "off"}
    key = f"hold:{seat_id}"
    h = await _r.hgetall(key)
    if not h or h.get("rid") != request_id:
        return 409, {"error": "not_winner_or_released", "seat_id": seat_id}
    owner = await _r.get(f"l2:req:{request_id}")
    if owner and account_id != owner:
        return 403, {"error": "account_mismatch", "seat_id": seat_id}
    if h.get("paid") == "1":
        return 200, {"status": "paid", "seat_id": seat_id}
    if time.time() > float(h.get("until") or 0):
        return 410, {"error": "hold_expired", "seat_id": seat_id}
    # 만료 처리와 겹치지 않게: holds 에서 먼저 빼는 쪽이 이긴다
    if not await _r.zrem("holds", seat_id):
        return 410, {"error": "hold_expired", "seat_id": seat_id}
    await _r.hset(key, "paid", "1")
    await _r.persist(key)
    hold_total.labels(result="paid").inc()
    if _payload_cb:
        p = await _payload_cb(seat_id)
        if p:
            await _publish({**p, "paid": True, "hold_until": float(h.get("until") or 0)})
    return 200, {"status": "paid", "seat_id": seat_id}


async def _sweeper():
    while True:
        try:
            due = await _r.zrangebyscore("holds", "-inf", time.time(), start=0, num=50)
            for seat in due:
                if await _r.zrem("holds", seat):      # 이 워커가 처리 권한을 얻었다
                    await _expire(seat)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print(f"[central] 결제 기한 청소 오류: {e}", flush=True)
        await asyncio.sleep(1.0)


async def _expire(seat: str):
    key = f"hold:{seat}"
    h = await _r.hgetall(key)
    if not h or h.get("paid") == "1":
        return
    rid = h.get("rid")
    acct = await _r.get(f"l2:req:{rid}") if rid else None
    await _release_cb(seat, rid)
    await _r.delete(key)
    hold_total.labels(result="expired").inc()
    if acct:
        await _on_unpaid(acct)
    if _detlog:
        await _detlog({"event_type": "hold_expired", "seat_id": seat, "request_id": rid,
                       "account_id": acct, "logged_at": time.time()})
    print(f"[central] 결제 기한 초과 → 좌석 반환 {seat} (계정 {acct})", flush=True)


async def clear_all():
    """초기화 스크립트용: 결제 대기를 모두 지운다."""
    if _r is None:
        return
    async for k in _r.scan_iter(match="hold:*", count=1000):
        await _r.delete(k)
    await _r.delete("holds")
