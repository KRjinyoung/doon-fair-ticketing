#!/usr/bin/env python3
"""
회차별로 봇 종류마다 요청이 어디서 멈췄나  [MACRO3_V1]
  python3 sim/layer_report.py sim/rounds3l2.jsonl sim/out/detlog_now.jsonl
요청마다: 1차(PoP 403/429) / 2차(중앙이 판정 후보에서 뺌) / 3차 명단(PoP·중앙이 AI 명단으로 막음) / 판정까지 감
"""
import json
import sys
from collections import Counter, defaultdict

rounds = [json.loads(l) for l in open(sys.argv[1]) if l.strip()]
opens = [rd["open_at_ms"] / 1000 for rd in rounds]
l1, l2 = defaultdict(list), {}     # 1차: (계정, 회차) → 사유들 (PoP 가 막은 요청은 응답에 request_id 가 없다)
for l in open(sys.argv[2]):
    if not l.startswith("{"):
        continue
    d = json.loads(l)
    if d.get("event_type") == "first_layer_block":
        t = (d.get("pop_arrival_epoch_us") or 0) / 1e6
        r = next((i for i, o in enumerate(opens) if o - 2 <= t < o + 45), None)
        if r is not None:
            l1[(d.get("account_id"), r)].append(d.get("first_layer_reason") or "")
    elif d.get("event_type") == "second_layer_block":
        l2[d.get("request_id")] = d.get("second_layer_reason") or ""


def stage(reason, layer):
    if reason.startswith("blacklist"):
        return "3차명단"
    if reason.startswith("rule"):
        return "1차(AI규칙)"
    return layer

types = ["simple", "farm", "hoarder", "disguised", "poller", "browser-stealth"]
print("회차 | " + " | ".join(f"{t}" for t in types) + " | 사람 (판정까지 / 요청)")
for ri, rd in enumerate(rounds):
    cells = []
    for t in types:
        c = Counter()
        for b in rd["bots"]:
            if b.get("bot") != t:
                continue
            for q in b.get("requests") or []:
                rid = q.get("request_id")
                if q.get("status") in (403, 429):
                    reasons = l1.get((b.get("account_id"), ri)) or [""]
                    reason = q.get("reason") or reasons[0]
                    c[stage(reason or "", "1차")] += 1
                elif rid in l2:
                    c[stage(l2[rid], "2차")] += 1
                elif rid:
                    c["통과"] += 1
        cells.append(" ".join(f"{k}{v}" for k, v in sorted(c.items())))
    hs = [q for h in rd.get("humans", []) for q in h.get("sent", [])]
    hp = sum(1 for q in hs if q.get("status") == 200)
    print(f"{rd['round']:>3} | " + " | ".join(cells) + f" | {hp}/{len(hs)}")
