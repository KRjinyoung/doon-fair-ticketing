#!/usr/bin/env python3
"""
처음 보는 공격으로 방어 3가지를 채점한다  [MACRO3_EVAL]   (doon-ai3 이미지에서)

  python sim/eval_attacks.py <학습 회차> <학습 detection-log> <얼린 규칙 묶음> <평가 회차> <평가 detection-log>

평가 회차는 실험판에서 AI 자동 반영을 멈추고, 규칙 묶음을 비운 채(1·2차 기본 규칙만) 돌렸다.
그래서 평가 데이터는 학습에도 규칙 생성에도 들어가지 않았다. 방어 ②③의 AI 부분은 학습 기간에
얼려 둔 모델·규칙을 평가 기록에 그대로 대 본다.

  ① 1·2차 규칙만     : 실제로 돌린 결과 (PoP v0 규칙 + 중앙 2차)
  ② 시간만 보는 AI    : 예전 방식 (docs/research/09) — 연속 5회차 첫 도착 흩어짐 ≤ 12.6ms
  ③ 3단계 전체        : ① + 얼린 AI 규칙(1차) + 얼린 기기 명단 + 얼린 3차 모델(봇 확률 ≥ 0.9)
"""
import json
import os
import sys
from collections import defaultdict

import numpy as np

sys.path[:0] = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "seoul"),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pop")]
import ai3  # noqa: E402
import bot_rules  # noqa: E402
from sklearn.ensemble import HistGradientBoostingClassifier  # noqa: E402

OLD_SPREAD_MS, OLD_WIN = 12.6, 5


def load(p):
    return [json.loads(l) for l in open(p) if l.strip().startswith("{")]


tr_rounds, tr_events = load(sys.argv[1]), load(sys.argv[2])
frozen = bot_rules.validate(json.load(open(sys.argv[3])))
ev_rounds, ev_events = load(sys.argv[4]), load(sys.argv[5])

# ---------------------------------------------------------------- 얼린 3차 모델 (학습 기간 데이터만)
tr_prof = ai3.profiles(tr_events, [r["open_at_ms"] / 1000 for r in tr_rounds])
accts = [a for a in tr_prof if tr_prof[a]["label"] is not None]
X = np.array([[np.nan if tr_prof[a]["f"][c] is None else tr_prof[a]["f"][c] for c in ai3.MODEL_FEATURES]
              for a in accts], float)
y = np.array([tr_prof[a]["label"] for a in accts])
use = [j for j in range(len(ai3.MODEL_FEATURES)) if np.isfinite(X[:, j]).sum() >= 4]
model = HistGradientBoostingClassifier(max_iter=200, min_samples_leaf=3, random_state=0).fit(X[:, use], y)
print(f"얼린 모델: 학습 계정 {len(accts)} (봇 {int(y.sum())}, 사람 {int((y == 0).sum())}), "
      f"feature {[ai3.MODEL_FEATURES[j] for j in use]}")

# ---------------------------------------------------------------- 평가 대상
opens = [r["open_at_ms"] / 1000 for r in ev_rounds]
kind = {}
for r in ev_rounds:
    for b in r["bots"]:
        if b.get("account_id"):
            kind[b["account_id"]] = b["bot"]
    for h in r.get("humans", []):
        if h.get("account_id"):
            kind[h["account_id"]] = "사람(고수)" if h.get("group") == "fast" else "사람(보통)"


def rnd(t):
    return next((i for i, o in enumerate(opens) if o - 2 <= t < o + 45), None)


first = defaultdict(dict)         # 계정 → {회차: 첫 도착 ms}
caught1, caught_rule, caught_dev = {}, {}, {}
for e in ev_events:
    a, t = e.get("account_id"), ai3.ts_of(e)
    if not a or a not in kind or t is None:
        continue
    i = rnd(t)
    if i is None:
        continue
    et = e.get("event_type")
    if et in ("seat_request", "first_layer_block"):
        ms = (t - opens[i]) * 1000
        first[a][i] = min(first[a].get(i, ms), ms)
        rec = {"headers": e.get("headers") or {}, "account_id": None, "device_fp": e.get("device_fp")}
        block, _ = bot_rules.evaluate(dict(frozen, blacklist={"accounts": [], "devices": []}), rec, 1)
        if block:
            caught_rule.setdefault(a, i)
        if e.get("device_fp") in set(frozen["blacklist"]["devices"]):
            caught_dev.setdefault(a, i)
        if et == "first_layer_block" and not (e.get("first_layer_reason") or "").startswith(("blacklist", "rule")):
            caught1.setdefault(a, i)
    elif et == "second_layer_block":
        caught1.setdefault(a, i)

# ② 예전 시간 AI
caught2 = {}
for a, fm in first.items():
    rs = sorted(fm)
    for j in range(len(rs) - OLD_WIN + 1):
        if np.std([fm[x] for x in rs[j:j + OLD_WIN]]) <= OLD_SPREAD_MS:
            caught2[a] = rs[j + OLD_WIN - 1]
            break

# ③ 얼린 3차 모델: 회차가 쌓일 때마다 그때까지의 기록으로 확률 계산
caught_m = {}
for k in range(1, len(opens) + 1):
    cut = opens[k - 1] + 45
    prof = ai3.profiles([e for e in ev_events if (ai3.ts_of(e) or 0) < cut], opens[:k])
    cand = [a for a in prof if a in kind and a not in caught_m]
    if not cand:
        continue
    Xe = np.array([[np.nan if prof[a]["f"][c] is None else prof[a]["f"][c] for c in ai3.MODEL_FEATURES]
                   for a in cand], float)
    for a, p in zip(cand, model.predict_proba(Xe[:, use])[:, 1]):
        if p >= ai3.P_BOT:
            caught_m[a] = k - 1


def first_of(*ds, a):
    v = [(d[a], name) for d, name in ds if a in d]
    return min(v) if v else None


# ---------------------------------------------------------------- 표
groups = defaultdict(list)
for a, k in kind.items():
    groups[k].append(a)
med_h = sorted(np.median(list(first[a].values())) for a in groups["사람(보통)"] + groups["사람(고수)"] if first[a])
print(f"\n평가 {len(opens)}회차 · 처음 보는 봇 계정 {sum(1 for k in kind.values() if not k.startswith('사람'))}개 · "
      f"사람 보통 {len(groups['사람(보통)'])} / 고수 {len(groups['사람(고수)'])}")
print("종류 | ① 1·2차 규칙만 | ② 시간만 AI | ③ 3단계 전체 (잡은 곳) | 첫 도착 중앙값 → 이보다 느린 사람")
for k in sorted(groups, key=lambda x: (x.startswith("사람"), x)):
    A = groups[k]
    c1 = sum(a in caught1 for a in A)
    c2 = sum(a in caught2 for a in A)
    how = defaultdict(int)
    c3 = 0
    for a in A:
        f = first_of((caught1, "1·2차"), (caught_rule, "AI규칙"), (caught_dev, "기기명단"), (caught_m, "3차모델"), a=a)
        if f:
            c3 += 1
            how[f[1]] += 1
    meds = [np.median(list(first[a].values())) for a in A if first[a]]
    m = float(np.median(meds)) if meds else float("nan")
    slower = sum(1 for x in med_h if x > m) / len(med_h) if med_h else 0
    print(f"{k:12} | {c1}/{len(A)} | {c2}/{len(A)} | {c3}/{len(A)} {dict(how)} | {m:.0f}ms → {slower:.0%}")
