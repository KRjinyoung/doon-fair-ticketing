"""
redis_window.py -- 좌석 수집창 상태를 Redis로 옮기는 독립 모듈 (Step 1A).

central_server.py 는 이 모듈을 import 해서 seat_windows(dict) 대신 사용한다.
이 파일은 central 코드를 전혀 import 하지 않으므로 단독으로 테스트할 수 있다.

[Redis 키]
  sw:{seat}:z   수집창 ZSET.  member=요청 JSON, score=corrected_epoch_ms(보정된 도착 시각)
                점수가 낮은 K개만 유지한다(트림) -> 좌석당 메모리 상한.
  sw:{seat}:n   그 좌석에 들어온 요청 수 카운터 (경쟁자 수 지표 / requests_so_far 용)
  sw:{seat}:c   확정 결과 JSON.  이 키가 있으면 "이미 확정된 좌석" -> 즉시반려
  sw:open       열린 창 목록 ZSET.  member=seat_id, score=창 마감 시각(ms).
                (a) 첫 요청 판별  (b) 마감 권한 선점  (c) 고아 창 청소기의 조회 대상

[공정성 불변식]
  승자 기준은 항상 score = corrected_epoch_ms (PoP 원본 시각 + 시계 오프셋) 이다.
  단, 시계 최대 오차(clock_error_ms) 안의 동점은 마감 Lua 가 추첨한다.  [TIE_LOTTERY_V1]
  "중앙이 요청을 처리한 순서"는 판정에 절대 영향을 주지 않는다.
  동점이면 ZSET 규칙(member 문자열 사전순)으로 결정되며, 이는 결정적(deterministic)이다.

[원자성]
  요청 추가(Lua A)와 창 마감(Lua B)은 각각 Lua 스크립트 하나로 실행된다.
  Redis 는 스크립트를 쪼개지 않고 실행하므로,
  "확정 여부 확인 -> 추가" 사이에 마감이 끼어드는 TOCTOU 레이스가 생기지 않는다.
  (메모리 dict 버전은 await 가 없어서 우연히 안전했지만, Redis 호출은 await 가
   끼므로 같은 프로세스 안의 타이머와도 레이스가 생긴다.)
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable, Optional

import redis.asyncio as aioredis
from redis.exceptions import NoScriptError

# [REDIS_WINDOW_V2]  (Step 2: DB 앞단 Stream 버퍼 + 방송 대기 목록 추가)
PREFIX = "sw"
OPEN_KEY = f"{PREFIX}:open"
STREAM_KEY = f"{PREFIX}:confirm:stream"      # 확정 결과 -> DB 반영 대기열 (Redis Stream)
GROUP = "db-writer"                          # 이 Stream 을 읽는 소비자 그룹
UNBROADCAST_KEY = f"{PREFIX}:unbroadcast"    # 확정은 됐지만 seat-status 방송이 확인되지 않은 좌석 ZSET(score=확정 시각 ms)


def k_confirmed(seat_id: str) -> str:
    return f"{PREFIX}:{seat_id}:c"


def k_zset(seat_id: str) -> str:
    return f"{PREFIX}:{seat_id}:z"


def k_count(seat_id: str) -> str:
    return f"{PREFIX}:{seat_id}:n"


# ---------------------------------------------------------------------------
# Lua A: 요청 추가
#   KEYS[1]=confirmed  KEYS[2]=zset  KEYS[3]=counter  KEYS[4]=open zset
#   ARGV[1]=member(JSON)  ARGV[2]=score  ARGV[3]=keep K  ARGV[4]=seat_id
#   ARGV[5]=창 마감 시각(ms)  ARGV[6]=창 키 TTL(sec, 안전장치)
#   반환: {0, confirmed_json}          이미 확정 -> 즉시반려
#         {1, requests_so_far, opened} 접수. opened=1 이면 이 요청이 창을 연 첫 요청
# ---------------------------------------------------------------------------
LUA_ADD = """
local c = redis.call('GET', KEYS[1])
if c then
  return {0, c}
end
local added = redis.call('ZADD', KEYS[2], 'NX', ARGV[2], ARGV[1])
if added == 1 then
  -- 점수가 낮은(=빠른) K개만 남긴다. 승자는 항상 최저 점수라 트림해도 안전하다.
  redis.call('ZREMRANGEBYRANK', KEYS[2], tonumber(ARGV[3]), -1)
  redis.call('INCR', KEYS[3])
end
local n = tonumber(redis.call('GET', KEYS[3]) or '0')
local opened = redis.call('ZADD', KEYS[4], 'NX', ARGV[5], ARGV[4])
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[6]))
redis.call('EXPIRE', KEYS[3], tonumber(ARGV[6]))
return {1, n, opened}
"""

# ---------------------------------------------------------------------------
# Lua B: 창 마감
#   KEYS 는 Lua A 와 동일.  ARGV[1]=seat_id  ARGV[2]=keep K  ARGV[3]=confirmed_at(ISO 문자열)
#   반환: confirmed JSON 문자열(이 호출이 마감을 수행함) / nil(이미 닫혔거나 권한 없음)
#
#   ZREM sw:open 의 반환값이 "마감 권한"이다. 로컬 타이머와 청소기가 동시에 와도
#   한 쪽만 1을 받아 진행하고, 다른 쪽은 nil 을 받아 아무것도 하지 않는다.
#   JSON 은 cjson 으로 다시 인코딩하지 않고 문자열을 이어 붙인다 (float 정밀도 보존).
# ---------------------------------------------------------------------------
LUA_CLOSE = """
local c = redis.call('GET', KEYS[1])
if c then
  redis.call('ZREM', KEYS[4], ARGV[1])
  return nil
end
if redis.call('ZREM', KEYS[4], ARGV[1]) == 0 then
  return nil
end
local top = redis.call('ZRANGE', KEYS[2], 0, tonumber(ARGV[2]) - 1)
if #top == 0 then
  return nil
end
local total = tonumber(redis.call('GET', KEYS[3]) or '0')
if total < #top then total = #top end
-- 시계 최대 오차 안의 동점은 추첨 (central_server.pick_winner 와 같은 식).  [TIE_LOTTERY_V2]
-- ① 같은 계정(account_key)은 가장 빠른 1건만 후보  ② 실제로 1등이었을 수 있는 요청만:
--    t_i - e_i <= min_j(t_j + e_j)
-- seed = sha1(좌석|정렬한 요청ID들), 승자 = 정렬된 묶음[(seed 앞 8자리 % 묶음 크기) + 1]
local function err(e) return tonumber(e.clock_error_ms) or 0 end
local cand, seen = {}, {}
for i = 1, #top do
  local e = cjson.decode(top[i])
  local a = e.account_key
  if a == cjson.null or a == '' then a = nil end
  if not (a and seen[a]) then
    if a then seen[a] = true end
    cand[#cand + 1] = {e = e, i = i}
  end
end
local upper = nil
for k = 1, #cand do
  local u = cand[k].e.corrected_epoch_ms + err(cand[k].e)
  if upper == nil or u < upper then upper = u end
end
local grp = {}
for k = 1, #cand do
  local e = cand[k].e
  if e.corrected_epoch_ms - err(e) <= upper then
    grp[#grp + 1] = {id = tostring(e.request_id), i = cand[k].i}
  end
end
local w = 1
local lottery = ''
if #grp > 1 then
  table.sort(grp, function(a, b) return a.id < b.id end)
  local ids = {}
  for k = 1, #grp do ids[k] = grp[k].id end
  local seed = redis.sha1hex(ARGV[1] .. '|' .. table.concat(ids, ','))
  w = grp[(tonumber(string.sub(seed, 1, 8), 16) % #grp) + 1].i
  lottery = ',"lottery":{"group":["' .. table.concat(ids, '","') .. '"],"seed":"' .. seed .. '"}'
end
local val = '{"confirmed_at":"' .. ARGV[3] .. '","total":' .. string.format('%d', total)
        .. ',"winner":' .. top[w] .. ',"top":[' .. table.concat(top, ',') .. ']' .. lottery .. '}'
redis.call('SET', KEYS[1], val)
redis.call('DEL', KEYS[2], KEYS[3])
-- Step 2: ARGV[4]=='1' 이면 같은 스크립트 안에서 (확정 키 기록 + DB 반영 큐 적재 + 방송 대기 등록)을
-- 한꺼번에 한다. 셋 중 일부만 남는 상태가 생기지 않는다.
if ARGV[4] == '1' then
  local wd = cjson.decode(top[w])
  redis.call('XADD', KEYS[5], '*',
             'seat', ARGV[1],
             'rid', tostring(wd.request_id),
             'pop', tostring(wd.pop_id),
             'ts', string.format('%.17g', wd.corrected_epoch_ms),
             'at', ARGV[3])
  local t = redis.call('TIME')
  redis.call('ZADD', KEYS[6], t[1] * 1000 + math.floor(t[2] / 1000), ARGV[1])
end
return val
"""


@dataclass
class AddResult:
    rejected: bool                      # True = 이미 확정된 좌석 (즉시반려)
    winner_request_id: Optional[str]    # rejected 일 때 기존 승자
    requests_so_far: int                # 접수 후 이 좌석의 누적 요청 수 (근사치, 아래 주석 참고)
    opened: bool                        # True = 이 요청이 창을 열었다 -> 호출자가 타이머 시작


def entry_member(entry: dict) -> str:
    """요청 하나를 ZSET member 문자열로 만든다. 같은 요청은 항상 같은 문자열 -> NX 로 중복 무시."""
    return json.dumps(entry, sort_keys=True, separators=(",", ":"))


class RedisWindowStore:
    def __init__(self, url: str, keep: int = 20, key_ttl_sec: int = 3600, max_connections: int = 32):
        self.url = url
        self.keep = keep
        self.key_ttl_sec = key_ttl_sec
        self.max_connections = max_connections
        self.r: aioredis.Redis | None = None
        self._sha_add: str = ""
        self._sha_close: str = ""

    async def start(self) -> None:
        # BlockingConnectionPool: 커넥션이 모자라면 에러 대신 대기한다 (HTTP 조회가 몰려도 안전).
        pool = aioredis.BlockingConnectionPool.from_url(
            self.url, decode_responses=True, max_connections=self.max_connections, timeout=5
        )
        self.r = aioredis.Redis(connection_pool=pool)
        await self.r.ping()
        await self._load_scripts()

    async def _load_scripts(self) -> None:
        self._sha_add = await self.r.script_load(LUA_ADD)
        self._sha_close = await self.r.script_load(LUA_CLOSE)

    async def stop(self) -> None:
        if self.r is not None:
            await self.r.aclose()

    # ------------------------------------------------------------------ 추가
    async def add_batch(
        self, items: Iterable[tuple[str, dict, float]]
    ) -> list[AddResult]:
        """
        items = [(seat_id, entry, deadline_ms), ...]
        파이프라인으로 한 번에 보낸다(왕복 1회). 한 파이프라인 안에서 명령은 보낸 순서대로
        실행되므로 같은 좌석의 요청 순서가 유지된다.
        deadline_ms 는 이 요청이 "창을 여는 첫 요청"일 때만 쓰이는 마감 시각(ms)이다.

        requests_so_far 는 근사치다: 트림으로 밀려난 요청이 크래시 후 재전달되면 다시
        카운트될 수 있다. 승자 판정에는 영향이 없고 지표/표시에만 영향이 있다.
        """
        items = list(items)
        if not items:
            return []
        raw = None
        for attempt in (0, 1):
            pipe = self.r.pipeline(transaction=False)
            for seat_id, entry, deadline_ms in items:
                pipe.evalsha(
                    self._sha_add, 4,
                    k_confirmed(seat_id), k_zset(seat_id), k_count(seat_id), OPEN_KEY,
                    entry_member(entry),
                    repr(float(entry["corrected_epoch_ms"])),
                    self.keep,
                    seat_id,
                    repr(float(deadline_ms)),
                    self.key_ttl_sec,
                )
            try:
                raw = await pipe.execute()
                break
            except NoScriptError:
                # Redis 가 재시작/FLUSH 되어 스크립트 캐시가 비었다. 다시 올리고 한 번 재시도.
                # (Lua A 는 NX/멱등이라 재시도해도 같은 결과)
                if attempt == 1:
                    raise
                await self._load_scripts()
        out: list[AddResult] = []
        for res in raw:
            if int(res[0]) == 0:
                winner = json.loads(res[1])["winner"]["request_id"]
                out.append(AddResult(True, winner, 0, False))
            else:
                out.append(AddResult(False, None, int(res[1]), bool(int(res[2]))))
        return out

    # ------------------------------------------------------------------ 마감
    async def close_window(self, seat_id: str, confirmed_at: str, buffer: bool = False) -> Optional[dict]:
        """
        창을 닫고 승자를 확정한다. 이 호출이 마감을 수행했으면 확정 dict 를, 이미 닫혔거나
        마감 권한이 없으면 None 을 돌려준다 (None 이면 호출자는 아무것도 하지 않는다).
        반환 dict: {"confirmed_at", "total", "winner", "top"}

        buffer=True 이면 확정 키 기록과 함께 (a) DB 반영 Stream 적재 (b) 방송 대기 목록 등록이
        같은 Lua 안에서 원자적으로 일어난다. (Step 2)
        """
        for attempt in (0, 1):
            try:
                raw = await self.r.evalsha(
                    self._sha_close, 6,
                    k_confirmed(seat_id), k_zset(seat_id), k_count(seat_id), OPEN_KEY,
                    STREAM_KEY, UNBROADCAST_KEY,
                    seat_id, self.keep, confirmed_at, "1" if buffer else "0",
                )
                return json.loads(raw) if raw else None
            except NoScriptError:
                if attempt == 1:
                    raise
                await self._load_scripts()
        return None

    # ------------------------------------------------------------- 청소기용
    async def sweep_due(self, now_ms: float, grace_ms: float = 1000.0, limit: int = 100) -> list[str]:
        """마감 시각 + grace 가 지났는데 아직 열려 있는 창(고아 창)의 seat_id 목록."""
        return await self.r.zrangebyscore(
            OPEN_KEY, "-inf", now_ms - grace_ms, start=0, num=limit
        )

    # ------------------------------------------------------------ 조회 (HTTP)
    async def seat_state(self, seat_id: str) -> dict:
        pipe = self.r.pipeline(transaction=False)
        pipe.get(k_confirmed(seat_id))
        pipe.zscore(OPEN_KEY, seat_id)
        pipe.get(k_count(seat_id))
        confirmed, open_score, n = await pipe.execute()
        if confirmed:
            c = json.loads(confirmed)
            return {"status": "confirmed", "winner": c["winner"], "all_requests": c["top"],
                    "lottery": c.get("lottery")}  # [TIE_LOTTERY_V1]
        if open_score is not None:
            return {"status": "pending", "requests_so_far": int(n or 0)}
        return {"status": "open"}

    async def pending_count(self) -> int:
        return int(await self.r.zcard(OPEN_KEY))

    async def overview(self) -> dict[str, str]:
        """{seat_id: 'pending'|'confirmed'} -- 기존 /seats-overview 와 같은 모양."""
        out: dict[str, str] = {}
        async for key in self.r.scan_iter(match=f"{PREFIX}:*:c", count=1000):
            out[key[len(PREFIX) + 1:-2]] = "confirmed"
        for seat_id in await self.r.zrange(OPEN_KEY, 0, -1):
            out.setdefault(seat_id, "pending")
        return out

    # ----------------------------------------------------- Postgres -> Redis
    async def hydrate_confirmed(self, rows: Iterable[dict]) -> int:
        """
        Postgres confirmed_seats 를 Redis 확정 키로 채운다 (SET NX: 이미 있으면 덮어쓰지 않음).
        rows: {"seat_id","winner_request_id","winner_pop_id","winner_timestamp_ms","confirmed_at"}
        최종 기록은 Postgres, 핫패스는 Redis -- Redis 가 비워져도 여기서 복구된다.
        모든 워커가 시작 시 호출해도 안전하다(멱등).
        """
        pipe = self.r.pipeline(transaction=False)
        n = 0
        for r in rows:
            val = json.dumps(
                {
                    "confirmed_at": r.get("confirmed_at", ""),
                    "total": 0,
                    "winner": {
                        "request_id": r["winner_request_id"],
                        "pop_id": r["winner_pop_id"],
                        "corrected_epoch_ms": r["winner_timestamp_ms"],
                        "pop_arrival_epoch_ms": r["winner_timestamp_ms"],
                    },
                    "top": [],
                },
                separators=(",", ":"),
            )
            pipe.set(k_confirmed(r["seat_id"]), val, nx=True)
            n += 1
        if n:
            await pipe.execute()
        return n

    # =========================================================== Step 2: DB 앞단 Stream
    async def ensure_group(self) -> None:
        """소비자 그룹을 만든다 (없을 때만). Redis 가 비워졌다가 다시 쓰일 때도 호출해 복구한다."""
        try:
            await self.r.xgroup_create(STREAM_KEY, GROUP, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    @staticmethod
    def _entries(res) -> list[tuple[str, dict]]:
        return [(eid, fields) for eid, fields in res]

    async def read_batch(self, consumer: str, count: int, block_ms: int | None) -> list[tuple[str, dict]]:
        """새 항목을 최대 count 개 읽는다. block_ms=None 이면 기다리지 않는다."""
        res = await self.r.xreadgroup(GROUP, consumer, {STREAM_KEY: ">"}, count=count, block=block_ms)
        if not res:
            return []
        return self._entries(res[0][1])

    async def claim_stale(self, consumer: str, min_idle_ms: int, count: int) -> list[tuple[str, dict]]:
        """
        읽어 갔지만 ACK 되지 않은 채 min_idle_ms 이상 지난 항목을 가져온다.
        (소비자가 죽었거나 DB 반영이 실패한 항목을 다른/같은 소비자가 이어받는 경로)
        """
        res = await self.r.xautoclaim(STREAM_KEY, GROUP, consumer, min_idle_time=min_idle_ms,
                                      start_id="0-0", count=count)
        return self._entries(res[1]) if res and len(res) > 1 else []

    async def ack_del(self, ids: list[str]) -> None:
        """DB 반영에 성공한 항목을 확인(ACK)하고 Stream 에서 지운다 (Stream 이 무한히 커지지 않게)."""
        if not ids:
            return
        pipe = self.r.pipeline(transaction=False)
        pipe.xack(STREAM_KEY, GROUP, *ids)
        pipe.xdel(STREAM_KEY, *ids)
        await pipe.execute()

    async def stream_len(self) -> int:
        return int(await self.r.xlen(STREAM_KEY))

    async def stream_pending(self) -> int:
        try:
            info = await self.r.xpending(STREAM_KEY, GROUP)
            return int(info["pending"]) if info else 0
        except aioredis.ResponseError:
            return 0

    # --------------------------------------------------------- 방송 대기 목록 (아웃박스)
    async def mark_broadcast(self, seat_id: str) -> None:
        """seat-status 방송이 성공했으므로 대기 목록에서 지운다."""
        await self.r.zrem(UNBROADCAST_KEY, seat_id)

    async def unbroadcast_due(self, now_ms: float, grace_ms: float = 2000.0, limit: int = 100) -> list[str]:
        """확정된 지 grace_ms 이상 지났는데 방송 확인이 없는 좌석 (방송 직전 크래시/발행 실패 복구용)."""
        return await self.r.zrangebyscore(UNBROADCAST_KEY, "-inf", now_ms - grace_ms, start=0, num=limit)

    async def get_confirmed(self, seat_id: str) -> Optional[dict]:
        raw = await self.r.get(k_confirmed(seat_id))
        return json.loads(raw) if raw else None
