#!/usr/bin/env python3
"""
stress.py -- 일부러 어렵게: AI 규칙(회차 간 도착 흩어짐 ≤ 임계값)이 어디서부터 깨지나  [STRESS_V1]

  python3 sim/stress.py [--k 5] [--thr 12.6] [--n 400]

같은 사람 행동 코드(sim/humans.py 의 Human.first_click)로 회차를 수천 번 흉내 내서 잰다
(실제 HTTP 없이 시각만 뽑는다. 실험판 회차 하나에 2분이라 수천 회차는 이 방법으로만 가능).

[봇 쪽] 사람처럼 보이려고 매 회차 0~J ms 무작위 지연을 섞는 봇 (학습 때 본 적 없는 L3)
  - J 가 얼마여야 AI 를 피하나 → 그때 봇은 사람 중 몇 %보다 빨리 도착하나
[사람 쪽] 점점 기계처럼 일정한 사람
  - 어느 수준부터 사람이 걸리나 (오탐)
"""
import argparse
import os
import random
import statistics
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import humans as H  # noqa: E402

STEALTH_BASE = (0.025, 0.040)   # 실험판에서 잰 스텔스 봇 도착 (오픈 후 25~40ms)
HUMAN_SCENARIOS = [
    ("기본 (실험판과 같음)", {}),
    ("모두 좌석을 미리 골라 둠", {"preselect": 1.0}),
    ("+ 반응 시간 흔들림 절반", {"preselect": 1.0, "rt_sigma": 0.175}),
    ("티켓팅 고수: 미리 골라 두고 0초에 연타", {"preselect": 1.0, "anticipate": 1.0}),
    ("고수 + 아주 빠른 연타(80~120ms)", {"preselect": 1.0, "anticipate": 1.0, "mash_gap": (0.08, 0.12)}),
]


def human_arrivals(n, rounds, seed):
    """사람 n명 x 회차 rounds 의 첫 도착(오픈 후 s). 참여 안 한 회차는 빠진다."""
    out = []
    for i in range(1, n + 1):
        h = H.Human(i + seed * 100000, ["http://a:1"])
        arr = []
        for r in range(rounds):
            rng = random.Random(f"{i}-{seed}-{r}")
            if rng.random() > H.P["participate"]:
                continue
            pre = rng.random() < H.P["preselect"]
            lat = rng.uniform(*(H.P["lat_mobile"] if h.mobile else H.P["lat_desktop"]))
            arr.append(h.first_click(rng, pre) + lat)
        out.append(arr)
    return out


def bot_arrivals(n, rounds, jitter, seed):
    rng = random.Random(f"bot-{jitter}-{seed}")
    return [[rng.uniform(*STEALTH_BASE) + rng.uniform(0, jitter) for _ in range(rounds)] for _ in range(n)]


def any_window(arr, k, thr):
    """연속 k 회 창 중 하나라도 흩어짐(모표준편차) ≤ thr 이면 걸린다 (사람 쪽, 엄격한 기준)."""
    return any(statistics.pstdev(arr[i:i + k]) * 1000 <= thr for i in range(len(arr) - k + 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--k", type=int, default=5, help="몇 회 연속으로 보나 (seoul/train_rules.py MIN_ROUNDS)")
    ap.add_argument("--thr", type=float, default=12.6, help="규칙 임계값(ms). 실험판 38회차로 배운 값")
    ap.add_argument("--n", type=int, default=400)
    a = ap.parse_args()
    base = dict(H.P)

    print(f"규칙: 연속 {a.k}회 도착 흩어짐 ≤ {a.thr}ms. 계정 {a.n}개 x 16회차.\n")
    print(f"[사람 쪽] 점점 기계처럼 일정한 사람 — 어느 {a.k}회 창이라도 걸린 사람 비율")
    for name, over in HUMAN_SCENARIOS:
        H.P.clear(); H.P.update(base, **over)
        hs = [x for x in human_arrivals(a.n, 16, seed=31) if len(x) >= a.k]
        hit = sum(any_window(x, a.k, a.thr) for x in hs)
        print(f"  {name:32} {hit:4}/{len(hs)} = {hit / len(hs):5.1%}")
    H.P.clear(); H.P.update(base)
    firsts = sorted(x for h in human_arrivals(a.n, 16, seed=32) for x in h)

    print(f"\n[봇 쪽] 매 회차 0~J ms 무작위 지연을 섞는 봇 (최근 {a.k}회로 판단) — AI 를 피하는 대가")
    print(f"  {'J(ms)':>6} {'탐지':>6} {'봇 도착 중앙':>10} {'봇보다 늦은 사람':>14}")
    for j in [0, 5, 10, 20, 30, 50, 100, 200]:
        bs = bot_arrivals(a.n, 16, j / 1000, seed=33)
        det = sum(statistics.pstdev(b[-a.k:]) * 1000 <= a.thr for b in bs) / len(bs)
        med = statistics.median(x for b in bs for x in b)
        print(f"  {j:>6} {det:>6.0%} {med * 1000:>8.0f}ms {sum(f > med for f in firsts) / len(firsts):>13.0%}")


if __name__ == "__main__":
    main()
