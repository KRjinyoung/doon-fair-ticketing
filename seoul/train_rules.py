#!/usr/bin/env python3
"""
train_rules.py -- 3차(서울) AI: 회차를 거듭한 '시간 분포'로 1·2차가 못 잡는 봇 계정을 찾는다.  (서울에서 실행)

  python3.11 train_rules.py rounds.jsonl --archive ~/data/detection-log --upto 8 --out rules.json

[왜 시간 분포인가]
  1·2차는 요청 하나를 그 자리에서 본다. 스텔스 봇은 헤더가 진짜 크롬과 같아 거기서는 안 보인다.
  '오픈 후 몇 ms 만에 눌렀나' 하나로 자르면 미리 연타하는 진짜 팬이 걸린다 (카운트다운이 있으면
  사람도 0초에 맞춰 누른다, Repp 2005). 그래서 반응 속도 자체는 쓰지 않는다. (설계서 08 §9)
  대신 계정마다 여러 회차의 도착 시각을 모아 '기계처럼 똑같은가'를 본다.
    봇   : 매번 같은 코드가 같은 순간에 쏜다 → 회차 간 흩어짐이 수 ms
    사람 : 기기 시계, 손, 망설임이 매번 다르다 → 수백 ms 흩어진다 (빨리 누르는 사람도)

[흐름]
  1) 회차 1~upto 의 v0 통과 좌석 요청을 계정별로 묶고, '연속 5회차' 창마다 특징을 만든다.
     학습은 모든 5회 창으로 한다 (사람이 짧은 기록에서도 걸리지 않는 선을 배우게).
     판단은 계정의 최근 5회 창으로 한다. 5회 미만 본 계정은 판단하지 않는다.
  2) 큰 모델(그래디언트 부스팅, LightGBM 과 같은 방식)과 깊이 3 결정나무(사람이 읽는 규칙)를 함께 배우고,
     두 답이 얼마나 같은지(충실도)로 짧은 규칙이 큰 모델만큼 맞는지 확인한다.
     계정을 5묶음으로 나눠 '학습에 안 쓴 계정'에서 탐지율·오탐·충실도를 잰다 (교차 검증).
  3) 규칙에 걸린 계정 목록을 bot-rules 형식으로 쓴다 → 운영자 승인 → publish_rules.py 로 전국 PoP.
  4) upto 이후 회차에 그 목록을 돌려 '다음 회차부터 막혔나'를 보여준다.

[특징 두 벌]
  행동만 : 회차 간 도착 흩어짐(ms), 회차당 요청 수(사람은 지면 다시 누른다), Referer 비율
  +계정  : 위 + 계정 나이, 같은 기기의 계정 수 — 시연용으로 '심어 둔' 값이라 결과를 따로 보여준다.
[쓰지 않는 것] pop_id, IP, 좌석 — 지역으로 판단하면 공정성 주제와 어긋난다. 반응 속도 절대값.
"""
import argparse
import json
import os
import sys
from collections import Counter, defaultdict

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import StratifiedKFold
from sklearn.tree import DecisionTreeClassifier, _tree

sys.path[:0] = [os.path.dirname(os.path.abspath(__file__)),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pop")]
import bot_rules  # noqa: E402
from round_report import load_log  # noqa: E402

KO = {"spread_ms": "연속 5회 도착 흩어짐(ms)", "req_per_round": "회차당 요청 수",
      "referer_ratio": "Referer 비율", "account_age_days": "계정 나이(일)", "accounts_on_device": "같은 기기 계정 수"}
SETS = {"행동만": ["spread_ms", "req_per_round", "referer_ratio"],
        "+계정": ["spread_ms", "req_per_round", "referer_ratio", "account_age_days", "accounts_on_device"]}
MIN_ROUNDS = 5  # 3회면 사람·봇 꼬리가 겹친다(stress: 사람 창 1만 개 중 29개). 5회면 0개, 여유 4.7ms


def requests_by_round(rounds, log, window):
    """[(회차, 오픈 후 ms, 정답, 요청)] — v0 통과 + 로그인한 좌석 요청만."""
    out = []
    for rd in rounds:
        bot_of = {b["account_id"]: b.get("bot") for b in rd["bots"] if b.get("account_id")}
        t0 = rd["open_at_ms"] * 1000
        for r in log:
            t = r.get("pop_arrival_epoch_us") or 0
            if t0 - 2e6 <= t < t0 + window * 1e6 and r.get("account_id") and r.get("first_layer_passed") is not False:
                out.append((rd["round"], (t - t0) / 1000, bot_of.get(r["account_id"], "human"), r))
    return out


def _window(rows, extra):
    """연속 회차 몇 개(rows = [(회차 첫 도착 ms, [요청들])])의 특징."""
    allr = [r for _, rs in rows for r in rs]
    return {"spread_ms": float(np.std([f for f, _ in rows])),
            "req_per_round": len(allr) / len(rows),
            "referer_ratio": float(np.mean(["referer" in r["headers"]["values"] for r in allr])), **extra}


def account_features(reqs):
    """계정 -> 최근 5회 창의 특징 + windows(모든 연속 5회 창). 5회 미만이면 windows 가 비어 있다."""
    per = defaultdict(lambda: defaultdict(list))
    dev = defaultdict(set)
    for rnd, ms, who, r in reqs:
        per[r["account_id"]][rnd].append((ms, r))
        dev[r.get("device_fp")].add(r["account_id"])
    feats = {}
    for acct, rounds in per.items():
        rows = [(min(x[0] for x in rounds[k]), [x[1] for x in rounds[k]]) for k in sorted(rounds)]
        r0 = rows[0][1][0]
        extra = {"account_age_days": r0.get("account_age_days"), "accounts_on_device": len(dev[r0.get("device_fp")])}
        wins = [_window(rows[i:i + MIN_ROUNDS], extra) for i in range(len(rows) - MIN_ROUNDS + 1)]
        feats[acct] = {"n_rounds": len(rows), **(wins[-1] if wins else _window(rows, extra)),
                       "windows": wins, "first_ms": [f for f, _ in rows]}
    return feats


def window_matrix(feats, accts, cols, label):
    """계정들의 모든 5회 창 -> (X, y, 창마다 계정)."""
    rows = [(k, w) for k in accts for w in feats[k]["windows"]]
    return (np.array([[w[c] for c in cols] for _, w in rows], dtype=float),
            np.array([int(label[k]) for k, _ in rows]), [k for k, _ in rows])


def fit(X, y):
    # 계정이 수십 개라 잎 최소 크기 기본값(20)이면 거의 나누지 못한다
    big = HistGradientBoostingClassifier(max_iter=200, min_samples_leaf=3, random_state=0).fit(X, y)
    # 짧은 규칙은 정답으로 직접 배운다. 큰 모델의 답을 흉내 내게 하면 큰 모델이 값을 굵게 묶은 구간
    # (실험판에서 7~29ms 를 한 칸으로)을 그대로 물려받아 선이 사람 쪽으로 밀렸다.
    # 큰 모델은 '짧은 규칙이 큰 모델과 같은 답을 내나(충실도)'를 재는 기준으로 쓴다.
    small = DecisionTreeClassifier(max_depth=3, min_samples_leaf=3, random_state=0).fit(X, y)
    return big, small


def tree_text(tree, names):
    """'봇' 잎으로 가는 길 = 규칙 한 줄."""
    t, out = tree.tree_, []
    bot_col = list(tree.classes_).index(1) if 1 in tree.classes_ else None

    def walk(node, conds):
        if t.feature[node] == _tree.TREE_UNDEFINED:
            if bot_col is not None and np.argmax(t.value[node][0]) == bot_col:
                out.append(" 그리고 ".join(conds) or "항상")
            return
        f, thr = KO[names[t.feature[node]]], t.threshold[node]
        walk(t.children_left[node], conds + [f"{f} ≤ {thr:.3g}"])
        walk(t.children_right[node], conds + [f"{f} > {thr:.3g}"])
    walk(0, [])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("rounds")
    ap.add_argument("--archive", default="~/data/detection-log")
    ap.add_argument("--window", type=float, default=30)
    ap.add_argument("--upto", type=int, help="이 회차까지로 학습 (기본: 전체의 2/3)")
    ap.add_argument("--use", default="행동만", choices=list(SETS), help="어느 특징 묶음의 규칙을 저장할지")
    ap.add_argument("--out", default="rules.json")
    a = ap.parse_args()

    rounds = [json.loads(l) for l in open(a.rounds) if l.strip()]
    upto = a.upto or rounds[max(1, len(rounds) * 2 // 3) - 1]["round"]
    reqs = requests_by_round(rounds, load_log(a.archive), a.window)
    label = {r["account_id"]: who for _, _, who, r in reqs}
    feats = account_features([q for q in reqs if q[0] <= upto])
    judged = [k for k, f in feats.items() if f["n_rounds"] >= MIN_ROUNDS]
    print(f"학습 회차 1~{upto}: 계정 {len(feats)}개 중 {MIN_ROUNDS}회 이상 본 {len(judged)}개를 판단 "
          f"(봇 {sum(label[k] != 'human' for k in judged)} / 사람 {sum(label[k] == 'human' for k in judged)})")

    print(f"\n연속 {MIN_ROUNDS}회차 도착 흩어짐 (5회 창마다 표준편차, ms) — 시간 분포가 보여주는 차이")
    for who in sorted({label[k] for k in judged}):
        v = sorted(w["spread_ms"] for k in judged if label[k] == who for w in feats[k]["windows"])
        print(f"  {who:16} 최소 {v[0]:7.1f}  중앙 {v[len(v) // 2]:7.1f}  최대 {v[-1]:7.1f}  (창 {len(v)})")

    isbot = {k: label[k] != "human" for k in judged}
    y = np.array([int(isbot[k]) for k in judged])
    chosen = None
    for name, cols in SETS.items():
        X = np.array([[feats[k][c] for c in cols] for k in judged], dtype=float)  # 최근 5회 창
        # 교차 검증: 학습에 안 쓴 계정에서 잰다. 사람은 '어느 5회 창이라도' 걸리면 오탐으로 센다.
        pred_small, pred_big, hum_any = np.zeros_like(y), np.zeros_like(y), np.zeros_like(y)
        for tr, te in StratifiedKFold(5, shuffle=True, random_state=0).split(X, y):
            Xw, yw, _ = window_matrix(feats, [judged[i] for i in tr], cols, isbot)
            big, small = fit(Xw, yw)
            pred_small[te], pred_big[te] = small.predict(X[te]), big.predict(X[te])
            for i in te:
                Xi, _, _ = window_matrix(feats, [judged[i]], cols, isbot)
                hum_any[i] = int(small.predict(Xi).any())
        Xw, yw, _ = window_matrix(feats, judged, cols, isbot)
        big, small = fit(Xw, yw)
        flagged = [k for k, p in zip(judged, small.predict(X)) if p == 1]
        hum = y == 0
        print(f"\n=== 특징: {name} ({', '.join(KO[c] for c in cols)}) ===")
        print(f"  [교차 검증, 처음 보는 계정] 봇 계정 탐지 {pred_small[~hum].mean():.0%}  "
              f"사람 오탐(어느 5회 창이라도) {hum_any[hum].mean():.0%} ({hum_any[hum].sum()}/{hum.sum()})  "
              f"큰 모델과 충실도 {np.mean(pred_small == pred_big):.0%}")
        print("  AI 가 만든 규칙 (이 중 하나라도 맞으면 봇 계정):")
        for line in tree_text(small, cols):
            print(f"    - {line}  (최근 {MIN_ROUNDS}회 기준)")
        print(f"  지금 걸리는 계정 {len(flagged)}개: " + ", ".join(f"{w} {n}" for w, n in Counter(label[k] for k in flagged).items()))

        ruleset = {"version": f"v1-ai-{name}-r{upto}", "approved_by": None,
                   "rules": [{"id": "ai-timing", "why": " / ".join(tree_text(small, cols)) + f" (최근 {MIN_ROUNDS}회 기준)",
                              "all": [{"f": "account_id", "op": "in", "v": sorted(flagged)}]}] if flagged else []}
        bot_rules.validate(ruleset)

        # 다음 회차부터 막혔나: upto 이후 회차에 PoP 와 같은 match 를 돌린다
        tab = defaultdict(Counter)
        for rnd, _, who, r in reqs:
            if rnd > upto:
                hit = bot_rules.match(ruleset, r) is not None
                tab[who][hit] += 1
                if who == "human":
                    tab["human@" + r["pop_id"]][hit] += 1
        print(f"  [배포 후] 회차 {upto + 1}~ 좌석 요청에 돌린 결과 (막힘 / 전체):")
        for who in sorted(tab):
            c = tab[who]
            print(f"    {who:22} {c[True]:4} / {c[True] + c[False]:<4}" + ("  ← 오탐" if who.startswith("human") else ""))
        if name == a.use:
            chosen = ruleset

    json.dump(chosen, open(a.out, "w", encoding="utf-8"), ensure_ascii=False, indent=1)
    print(f"\n'{a.use}' 규칙을 {a.out} 에 저장했습니다. 운영자가 읽고 승인하면 publish_rules.py 로 배포합니다.")


if __name__ == "__main__":
    main()
