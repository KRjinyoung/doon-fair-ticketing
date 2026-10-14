#!/usr/bin/env python3
"""
stress.py -- 일부러 어렵게: AI 규칙(회차 간 도착 흩어짐 ≤ 임계값)이 어디서부터 깨지나  [STRESS_V1]

  python3 sim/stress.py [--thr 199] [--n 400]

같은 사람 행동 코드(sim/humans.py 의 Human.first_click)로 회차를 수천 번 흉내 내서 잰다
(실제 HTTP 없이 시각만 뽑는다. 실험판 회차 하나에 2분이라 수천 회차는 이 방법으로만 가능).

[봇 쪽] 사람처럼 보이려고 매 회차 0~J ms 무작위 지연을 섞는 봇 (학습 때 본 적 없는 L3)
  - J 가 얼마여야 AI 를 피하나 → 그때 봇은 사람 중 몇 %보다 빨리 도착하나 (= 매크로를 쓸 이유)
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


def flagged(arr, k, thr):
    """처음 k 회 관찰로 판단: 회차 간 흩어짐(모표준편차) ≤ thr 이면 봇."""
    return len(arr) >= k and statistics.pstdev(arr[:k]) * 1000 <= thr


def rate(accts, k, thr):
    judged = [a for a in accts if len(a) >= k]
    return sum(flagged(a, k, thr) for a in judged) / len(judged)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--thr", type=float, default=199, help="배포된 규칙의 임계값(ms)")
    ap.add_argument("--n", type=int, default=400)
    a = ap.parse_args()
    base = dict(H.P)

    print(f"규칙: 회차 간 도착 흩어짐 ≤ {a.thr:.0f}ms (실험판 v2 그대로). 계정 {a.n}개씩.\n")
    print("[사람 쪽] 얼마나 일정해지면 사람이 걸리나 (오탐률: 3회 관찰 / 6회 관찰)")
    hard = None
    for name, over in HUMAN_SCENARIOS:
        H.P.clear(); H.P.update(base, **over)
        hs = human_arrivals(a.n, 12, seed=1)
        sp = sorted(statistics.pstdev(x[:6]) * 1000 for x in hs if len(x) >= 6)
        print(f"  {name:32} 오탐 {rate(hs, 3, a.thr):5.1%} / {rate(hs, 6, a.thr):5.1%}   "
              f"사람 흩어짐 하위 5% {sp[len(sp) // 20]:5.0f}ms")
        hard = hs if over.get("anticipate") == 1.0 and "mash_gap" not in over else hard
    H.P.clear(); H.P.update(base)
    normal = human_arrivals(a.n, 12, seed=2)
    firsts = sorted(x for h in normal for x in h)

    print("\n[봇 쪽] 무작위 지연 0~J ms 를 섞는 봇 — AI 를 피하는 대가")
    print(f"  {'J(ms)':>6} {'탐지 3회':>8} {'탐지 6회':>8} {'봇 도착 중앙':>10} {'봇보다 늦은 사람':>14}")
    for j in [0, 50, 100, 200, 400, 700, 1000, 1500, 2000]:
        bs = bot_arrivals(a.n, 12, j / 1000, seed=3)
        med = statistics.median(x for b in bs for x in b)
        beat = sum(f > med for f in firsts) / len(firsts)
        print(f"  {j:>6} {rate(bs, 3, a.thr):>8.0%} {rate(bs, 6, a.thr):>8.0%} {med * 1000:>8.0f}ms {beat:>13.0%}")

    # 까다로운 사람(고수)을 학습에 넣으면 AI 는 임계값을 어디로 옮기나: 봇(J=0) 최대 ~ 고수 최소 사이 가운데
    bots0 = [statistics.pstdev(b[:3]) * 1000 for b in bot_arrivals(a.n, 12, 0, seed=4)]
    hard_sp = [statistics.pstdev(h[:3]) * 1000 for h in hard if len(h) >= 3]
    thr2 = (max(bots0) + sorted(hard_sp)[len(hard_sp) // 100]) / 2
    print(f"\n[재학습] '티켓팅 고수'를 학습 데이터에 넣으면 임계값 → 약 {thr2:.0f}ms")
    print(f"  고수 오탐 {rate(hard, 3, thr2):.1%} / {rate(hard, 6, thr2):.1%}   기본 사람 오탐 {rate(normal, 3, thr2):.1%}")
    for j in [0, 50, 100, 200]:
        bs = bot_arrivals(a.n, 12, j / 1000, seed=5)
        print(f"  J={j:>4}ms 봇 탐지 {rate(bs, 3, thr2):.0%} / {rate(bs, 6, thr2):.0%}")


if __name__ == "__main__":
    main()
