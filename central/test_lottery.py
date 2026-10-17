"""
동점 추첨 묶음 검증 — Python(pick_winner, 메모리 경로)과 Redis Lua(LUA_CLOSE)가 같은 정의를 따르는가  [TIE_LOTTERY_V2]

  docker run --rm --network <redis 가 있는 망> -e TEST_REDIS_URL=redis://<host>:6379/15 \
    -v "$PWD/central":/app <central 이미지> python -I test_lottery.py

정의
  ① 같은 계정의 요청은 보정 시각이 가장 빠른 하나만 후보로 남긴다 (계정이 없는 요청은 각각 후보).
  ② 후보 i 가 실제로 가장 먼저 도착했을 수 있는 조건: t_i - e_i <= min_{j≠i}(t_j + e_j)
     (t = 보정 시각, e = 그 PoP 시계의 보장 오차). 이 조건을 만족하는 후보끼리만 추첨한다.
"""
import asyncio
import os
import random
import sys
import uuid

os.environ.setdefault("STATE_BACKEND", "memory")
os.environ.setdefault("DATABASE_URL", "postgresql://unused@localhost/unused")   # 연결하지 않는다 (import 만)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import central_server as cs  # noqa: E402
from redis_window import RedisWindowStore  # noqa: E402

URL = os.environ.get("TEST_REDIS_URL", "redis://localhost:6379/15")


def entry(t, e, acct=None, rid=None):
    return {"request_id": rid or str(uuid.uuid4()), "pop_id": "pop-test", "pop_arrival_epoch_ms": int(t),
            "pop_arrival_epoch_us": None, "corrected_epoch_ms": float(t), "clock_error_ms": float(e),
            "account_key": acct}


def by_definition(entries):
    seen, cand = set(), []
    for x in sorted(entries, key=lambda x: (x["corrected_epoch_ms"], x["request_id"])):
        a = x.get("account_key")
        if a and a in seen:
            continue
        if a:
            seen.add(a)
        cand.append(x)
    return sorted(x["request_id"] for x in cand
                  if all(x["corrected_epoch_ms"] - x["clock_error_ms"] <= o["corrected_epoch_ms"] + o["clock_error_ms"]
                         for o in cand if o is not x))


def py_group(seat, entries):
    ranked = sorted(entries, key=lambda x: (x["corrected_epoch_ms"], x["request_id"]))
    w, lot = cs.pick_winner(seat, ranked)
    return (sorted(lot["group"]) if lot else [w["request_id"]]), w["request_id"]


async def lua_group(store, seat, entries):
    await store.add_batch([(seat, x, 0.0) for x in entries])
    res = await store.close_window(seat, "2026-10-17T00:00:00")
    lot = res.get("lottery")
    return (sorted(lot["group"]) if lot else [res["winner"]["request_id"]]), res["winner"]["request_id"]


async def main():
    store = RedisWindowStore(URL, keep=40)
    await store.start()
    await store.r.flushdb()
    fails = 0

    def check(name, entries, py, lua, want):
        nonlocal fails
        ok = py[0] == want and lua[0] == want and py[1] == lua[1] and py[1] in want
        fails += not ok
        if not ok or name:
            label = {x["request_id"]: x.get("_n", x["request_id"][:6]) for x in entries}
            show = lambda g: [label[i] for i in g]  # noqa: E731
            print(f"{'통과' if ok else '실패'} {name or ''}: 정의 {show(want)} | Python {show(py[0])} 승자 {label[py[1]]}"
                  f" | Lua {show(lua[0])} 승자 {label[lua[1]]}")

    # ① 반례: A 는 오차가 커서 B·C 와 모두 겹치지만, B 가 C 보다 확실히 먼저다 → C 는 1등일 수 없다
    cases = {
        "반례 (A·B·C)": [dict(entry(0.0, 2.4, "a"), _n="A"), dict(entry(0.1, 1.4, "b"), _n="B"),
                       dict(entry(3.0, 1.4, "c"), _n="C")],
        # ② 같은 계정이 같은 좌석에 3번 → 그 계정은 가장 빠른 1건만 후보
        "같은 계정 3번 (X)": [dict(entry(0.0, 1.0, "x"), _n="X1"), dict(entry(0.2, 1.0, "x"), _n="X2"),
                          dict(entry(0.4, 1.0, "x"), _n="X3"), dict(entry(0.3, 1.0, "y"), _n="Y")],
    }
    for k, (name, es) in enumerate(cases.items()):
        seat = f"T-{k}"
        check(name, es, py_group(seat, es), await lua_group(store, seat, es), by_definition(es))

    # ③ 무작위 500 사례 (요청 2~8건, 시각 0~5ms, 오차 0~2.5ms, 계정 중복·익명 섞음)
    rng = random.Random(7)
    n_lot = 0
    for k in range(500):
        es = [entry(rng.uniform(0, 5), rng.choice([0.0, rng.uniform(0, 2.5)]), rng.choice([None, "p", "q", "r", "s", "t", "u"]))
              for _ in range(rng.randint(2, 8))]
        seat = f"R-{k}"
        want = by_definition(es)
        n_lot += len(want) > 1
        check(None, es, py_group(seat, es), await lua_group(store, seat, es), want)
    print(f"무작위 500 사례: 추첨 {n_lot}건, 불일치 {fails}건 (반례 포함 전체)")
    await store.r.flushdb()
    await store.stop()
    sys.exit(1 if fails else 0)


asyncio.run(main())
