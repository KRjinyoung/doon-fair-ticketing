"""
2차 판정 — 중앙 (전국 PoP 를 모은 현재 상태)  [MACRO3_V1]   docs/research/12-macro-3layer.md §3

1차(PoP)는 자기 PoP 로 들어온 요청 하나만 본다. 2차는 판정 직전에 모든 PoP 의 요청과
중앙 DB(세션)를 모아서 본다. 걸린 요청은 판정 후보에서만 빠지고, 남은 요청의 시각·순서·추첨은 그대로다.

  ① 신원 대조     요청의 세션이 정말 이 계정·이 기기의 것인가 (로그인 때 sessions 에 남긴 값과 비교)
  ② 기기당 계정 수 한 기기 지문에 붙은 계정이 많고, 이 계정이 갓 만든 계정인가
                  (오래된 계정끼리 기기를 같이 쓰는 건 PC방·가족 → 제외)
  ③ 미결제 선점    결제 기한을 넘겨 좌석을 돌려준 횟수가 한도를 넘었나 (좌석 선점 봇)
  ④ 전국 합산 요청 수  판교·대전에 나눠 보낸 요청까지 합친 계정·기기별 요청 수
  ⑤ 서울 명단     3차가 봇으로 본 계정·기기
  ⑥ 2차 규칙      서울이 보낸 layer 2 규칙 (집계 값 agg.* 도 볼 수 있다)

central 은 워커 프로세스가 여러 개라, 전국 합산 숫자는 Redis 에 두고 함께 본다.
Redis 를 쓸 수 없으면 2차는 꺼진다(모두 통과 — 애매하면 통과).

모드 L2_MODE: off / monitor(판정만, 통과) / enforce(후보에서 제외). 기본 enforce.
"""
import asyncio
import copy
import json
import os
import time

import redis.asyncio as aioredis
from prometheus_client import Counter

import bot_rules as br

MODE = os.environ.get("L2_MODE", "enforce").strip().lower()
if MODE not in ("off", "monitor", "enforce"):
    MODE = "enforce"

DEFAULT_PARAMS = {
    "rate": {"account": [10.0, 30], "device": [10.0, 40]},   # [창(초), 한도] — 전국 합산
    "device_accounts": {"max": 3, "young_days": 7},         # 기기당 계정 > max 이고 계정 나이 < young_days
    "hold": {"max_unpaid": 2},                             # 미결제 반환 > max_unpaid
}
PARAMS = copy.deepcopy(DEFAULT_PARAMS)
ruleset = br.validate(copy.deepcopy(br.EMPTY))

l2_checked = Counter("central_l2_checked_total", "2차 판정을 거친 좌석 요청 수")
l2_flagged = Counter("central_l2_flagged_total", "2차가 봇으로 본 좌석 요청 수 (monitor 에서도 오름)", ["reason"])
l2_shadow = Counter("central_l2_shadow_total", "2차 그림자 규칙에 맞은 수 (막지 않음)", ["rule"])

_r = None            # Redis
_producer = None     # detection-log 발행 (central 의 Kafka producer 를 빌려 쓴다)
_tasks = set()


def apply_params(l2) -> dict:
    """규칙 묶음의 params.l2 를 기본값 위에 덮어쓴다. 형식이 틀린 칸은 기본값."""
    l2 = l2 if isinstance(l2, dict) else {}
    p = copy.deepcopy(DEFAULT_PARAMS)
    for k, v in (l2.get("rate") or {}).items():
        if k in p["rate"] and isinstance(v, (list, tuple)) and len(v) == 2 and v[0] > 0 and v[1] > 0:
            p["rate"][k] = [float(v[0]), int(v[1])]
    for sect in ("device_accounts", "hold"):
        for k, v in (l2.get(sect) or {}).items():
            if k in p[sect] and isinstance(v, (int, float)) and v >= 0:
                p[sect][k] = v
    PARAMS.clear()
    PARAMS.update(p)
    return PARAMS


def set_ruleset(rs: dict):
    global ruleset
    ruleset = rs
    apply_params(rs.get("params", {}).get("l2"))


async def start(redis_url: str, pool, producer):
    """Redis 연결 + 세션(신원·기기 지문)을 DB 에서 Redis 로 채운다. 실패하면 2차는 꺼진 채로 둔다."""
    global _r, _producer
    _producer = producer
    if MODE == "off" or not redis_url:
        print("[central] 2차 판정 꺼짐", flush=True)
        return
    try:
        r = aioredis.from_url(redis_url, decode_responses=True)
        await r.ping()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT session_id, account_id, device_fp FROM sessions")
        pipe = r.pipeline(transaction=False)
        for row in rows:
            _remember(pipe, row["session_id"], row["account_id"], row["device_fp"])
        await pipe.execute()
        _r = r
        print(f"[central] 2차 판정 시작 ({MODE}) — 세션 {len(rows)}개 적재 [MACRO3_V1]", flush=True)
    except Exception as e:
        print(f"[central] 2차 판정을 켜지 못함(모두 통과): {e}", flush=True)


def _remember(pipe, sid, acct, dfp):
    pipe.hset(f"l2:sess:{sid}", mapping={"a": acct or "", "d": dfp or ""})
    if dfp:
        pipe.sadd(f"l2:dev:{dfp}", acct)


async def on_login(sid: str, acct: str, dfp: str):
    """로그인 직후 호출. 이 세션의 계정·기기를 기억하고 기기당 계정 집합에 더한다."""
    if _r is None:
        return
    try:
        pipe = _r.pipeline(transaction=False)
        _remember(pipe, sid, acct, dfp)
        await pipe.execute()
    except Exception as e:
        print(f"[central] 2차 로그인 기록 실패: {e}", flush=True)


async def add_unpaid(acct: str):
    """결제 기한을 넘겨 좌석을 돌려줬을 때 호출 (좌석 선점)."""
    if _r is None or not acct:
        return
    try:
        await _r.incr(f"l2:unpaid:{acct}")
    except Exception:
        pass


async def check(items: list) -> list:
    """
    좌석 요청 묶음(Kafka 메시지 값들)을 한 번의 Redis 왕복으로 판정한다.
    반환: 요청마다 (막을 사유 또는 None, 그림자 규칙 id 목록, 집계 값 dict)
    """
    out = [(None, [], {}) for _ in items]
    if _r is None or MODE == "off" or not items:
        return out
    now = time.time()
    rate = PARAMS["rate"]
    pipe = _r.pipeline(transaction=False)
    plan = []   # 요청마다 파이프라인에 넣은 명령의 이름 순서
    for d in items:
        acct, dev, sid = d.get("account_id"), d.get("device_fp"), d.get("session_id")
        steps = []
        if d.get("identity_verified") and acct:
            if d.get("request_id"):
                pipe.set(f"l2:req:{d['request_id']}", acct, ex=3600)   # 결제·미결제 처리에서 계정을 찾는다
                steps.append(None)
            if sid:
                pipe.hmget(f"l2:sess:{sid}", "a", "d")
                steps.append("sess")
            pipe.get(f"l2:unpaid:{acct}")
            steps.append("unpaid")
            for kind, key in (("account", acct), ("device", dev)):
                if not key:
                    continue
                k = f"l2:rate:{kind}:{key}"
                win = rate[kind][0]
                pipe.zadd(k, {d.get("request_id") or f"{now}": now})
                pipe.zremrangebyscore(k, 0, now - win)
                pipe.zcard(k)
                pipe.expire(k, int(win) + 1)
                steps += [None, None, f"rate_{kind}", None]
            if dev:
                pipe.scard(f"l2:dev:{dev}")
                steps.append("dev_accounts")
        plan.append(steps)
    try:
        res = await pipe.execute()
    except Exception as e:
        print(f"[central] 2차 Redis 오류(이번 묶음 통과): {e}", flush=True)
        return out

    i = 0
    for n, (d, steps) in enumerate(zip(items, plan)):
        got = {}
        for name in steps:
            if name:
                got[name] = res[i]
            i += 1
        agg = {"device_accounts": got.get("dev_accounts"),
               "unpaid": int(got.get("unpaid") or 0),
               "rate_account": got.get("rate_account"),
               "rate_device": got.get("rate_device")}
        l2_checked.inc()
        block = None
        sa, sd = (got.get("sess") or [None, None])
        if sa is not None and (sa != d.get("account_id") or (sd and d.get("device_fp") and sd != d.get("device_fp"))):
            block = "identity_mismatch"
        rule_block, shadow = br.evaluate(ruleset, {**d, "agg": agg}, 2)
        block = block or rule_block
        age = d.get("account_age_days")
        da = PARAMS["device_accounts"]
        if block is None and agg["device_accounts"] is not None and agg["device_accounts"] > da["max"] \
                and age is not None and age < da["young_days"]:
            block = "device_farm"
        if block is None and agg["unpaid"] > PARAMS["hold"]["max_unpaid"]:
            block = "seat_hoarding"
        for kind in ("account", "device"):
            c = agg[f"rate_{kind}"]
            if block is None and c is not None and c > rate[kind][1]:
                block = f"rate_limit_{kind}_global"
        if block:
            l2_flagged.labels(reason=block.split(":")[0]).inc()
        for s in shadow:
            l2_shadow.labels(rule=s).inc()
        out[n] = (block, shadow, agg)
    return out


async def filter_batch(items: list) -> list:
    """판정 후보로 남길지(True/False). 걸린 요청과 그림자 적중은 detection-log 에 남긴다."""
    verdicts = await check(items)
    keep = []
    for d, (block, shadow, agg) in zip(items, verdicts):
        drop = bool(block) and MODE == "enforce"
        keep.append(not drop)
        if block or shadow:
            _spawn(_log(d, block, shadow, agg, drop))
    return keep


async def _log(d, block, shadow, agg, dropped):
    if _producer is None:
        return
    try:
        await _producer.send("detection-log", key=d.get("seat_id"), value={
            "event_type": "second_layer_block" if dropped else "second_layer_flag",
            "seat_id": d.get("seat_id"), "request_id": d.get("request_id"), "pop_id": d.get("pop_id"),
            "pop_arrival_epoch_us": d.get("pop_arrival_epoch_us"),
            "account_id": d.get("account_id"), "device_fp": d.get("device_fp"),
            "account_age_days": d.get("account_age_days"),
            "second_layer_reason": block, "second_layer_shadow": shadow, "agg": agg,
            "second_layer_mode": MODE, "ruleset_version": ruleset.get("version"),
            "logged_at": time.time()})
    except Exception as e:
        print(f"[central] 2차 detection-log 발행 실패: {e}", flush=True)


def _spawn(coro):
    t = asyncio.create_task(coro)
    _tasks.add(t)
    t.add_done_callback(_tasks.discard)


async def rules_loop(broker: str):
    """서울이 보낸 규칙 묶음을 받아 교체한다 (워커마다, 토픽을 처음부터 — PoP 와 같은 방식)."""
    from aiokafka import AIOKafkaConsumer
    while True:
        consumer = AIOKafkaConsumer("bot-rules", bootstrap_servers=broker, group_id=None,
                                    auto_offset_reset="earliest")
        try:
            await consumer.start()
            async for msg in consumer:
                try:
                    rs = br.validate(json.loads(msg.value))
                    set_ruleset(rs)
                    print(f"[central] 2차 규칙 적용: {rs['version']} (규칙 "
                          f"{sum(1 for r in rs['rules'] if r['layer'] == 2)}개, 명단 계정 "
                          f"{len(rs['blacklist']['accounts'])} · 기기 {len(rs['blacklist']['devices'])})",
                          flush=True)
                except Exception as e:
                    print(f"[central] 규칙 묶음 무시 (offset {msg.offset}): {e}", flush=True)
        except asyncio.CancelledError:
            await consumer.stop()
            raise
        except Exception as e:
            print(f"[central] bot-rules consumer 오류, 5초 후 재시도: {e}", flush=True)
            try:
                await consumer.stop()
            except Exception:
                pass
            await asyncio.sleep(5)


def status() -> dict:
    return {"mode": MODE, "enabled": _r is not None, "params": PARAMS,
            "ruleset_version": ruleset.get("version")}
