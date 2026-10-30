#!/usr/bin/env python3
"""
3차 약한 라벨 검증 — 학습에는 정답을 안 쓰고, 채점에만 쓴다  [MACRO3_EVAL]
  python sim/eval_labels.py sim/rounds3l2.jsonl <detection-log 덤프>   (doon-ai3 이미지에서)

  ① 라벨 채점: 1·2차 판정으로 만든 봇·사람 라벨이 실제 정답과 맞나 (정확도 / 확보율 따로)
  ② 모델 채점: 라벨 없는 계정까지 포함해 3차 확률(≥0.9)이 실제 정답과 맞나
  ③ 라벨 오염 시험: 라벨 일부를 일부러 틀리게 바꿔 다시 학습 → 결과가 얼마나 흔들리나
"""
import copy
import json
import os
import random
import sys
from collections import Counter

sys.path[:0] = [os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "seoul"),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pop")]
import ai3  # noqa: E402

rounds = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
events = [json.loads(l) for l in open(sys.argv[2]) if l.startswith("{")]
truth = {b["account_id"]: b["bot"] for rd in rounds for b in rd["bots"] if b.get("account_id")}
prof = ai3.profiles(events, [rd["open_at_ms"] / 1000 for rd in rounds])
is_bot = {a: a in truth for a in prof}
bots = [a for a in prof if is_bot[a]]
humans = [a for a in prof if not is_bot[a]]

print("== ① 라벨 채점")
lb = [a for a, p in prof.items() if p["label"] == 1]
lh = [a for a, p in prof.items() if p["label"] == 0]
print(f"  '봇' 라벨 {len(lb)}개 중 진짜 봇 {sum(is_bot[a] for a in lb)}개 → 정확도 "
      f"{sum(is_bot[a] for a in lb) / max(1, len(lb)):.0%}")
print(f"  '사람' 라벨 {len(lh)}개 중 진짜 사람 {sum(not is_bot[a] for a in lh)}개 → 정확도 "
      f"{sum(not is_bot[a] for a in lh) / max(1, len(lh)):.0%}")
print(f"  확보율: 봇 {len(bots)}개 중 라벨 {sum(1 for a in bots if prof[a]['label'] == 1)}개 "
      f"({sum(1 for a in bots if prof[a]['label'] == 1) / len(bots):.0%}), "
      f"사람 {len(humans)}개 중 라벨 {sum(1 for a in humans if prof[a]['label'] == 0)}개")
print("  봇 종류별 라벨:", dict(Counter((truth[a], {1: '봇', 0: '사람', None: '없음'}[prof[a]['label']]) for a in bots)))


def score(pr, proba):
    hit = [a for a, p in proba.items() if p >= ai3.P_BOT]
    tp = sum(is_bot[a] for a in hit)
    fp = sum(not is_bot[a] for a in hit)
    unl = [a for a in bots if pr[a]["label"] is None]
    return tp, fp, sum(1 for a in unl if proba.get(a, 0) >= ai3.P_BOT), len(unl)


print("== ② 모델 채점 (확률 ≥ 0.9 를 봇으로)")
proba, imp, _ = ai3.fit_predict(prof)
tp, fp, ut, un = score(prof, proba)
print(f"  봇 {len(bots)}개 중 {tp}개 탐지 ({tp / len(bots):.0%}), 사람 {len(humans)}명 중 {fp}명 오탐")
print(f"  라벨 없던 봇 {un}개 중 {ut}개 탐지")

print("== ③ 라벨 오염 시험 (10번 반복 평균)")
for name, frac_h, frac_b in (("사람 라벨 10% → '봇'", 0.1, 0), ("사람 라벨 20% → '봇'", 0.2, 0),
                             ("봇 라벨 30% → '사람'", 0, 0.3), ("둘 다 20%", 0.2, 0.2)):
    res = []
    for seed in range(10):
        rng = random.Random(seed)
        pr = copy.deepcopy(prof)
        H = [a for a in pr if pr[a]["label"] == 0]
        B = [a for a in pr if pr[a]["label"] == 1]
        for a in rng.sample(H, int(len(H) * frac_h)):
            pr[a]["label"] = 1
        for a in rng.sample(B, int(len(B) * frac_b)):
            pr[a]["label"] = 0
        p2, _, _ = ai3.fit_predict(pr)
        res.append(score(pr, p2)[:2] if p2 else (0, 0))
    print(f"  {name:20} 봇 탐지 평균 {sum(r[0] for r in res) / 10:.1f}/{len(bots)}, "
          f"사람 오탐 평균 {sum(r[1] for r in res) / 10:.1f}/{len(humans)}")
