#!/usr/bin/env python3
"""
ai3.py -- 3차(서울 AI): 계정별 봇 확률 + 근거, 명단 배포, 1·2차 규칙 정교화  [MACRO3_V1]
          docs/research/12-macro-3layer.md §3, §5

  python3.11 ai3.py --rounds rounds.jsonl [--bootstrap kafka:9092] [--log ai3.jsonl]     # 회차마다 자동 반영
  python3.11 ai3.py --rounds rounds.jsonl --archive detlog.jsonl --once                  # 쌓인 로그로 한 번만

[무엇을 보나] 1·2차는 요청 하나, 또는 지금 이 순간의 전국 집계를 본다. 3차는 계정별 '궤적'을 본다.
  feature (계정마다)
    account_age_days   계정 나이
    device_accounts    같은 기기 지문을 쓴 계정 수 (로그에서 센다)
    concurrent_seats   한 회차에 1초 안에 노린 서로 다른 좌석 수의 최댓값
    interval_cv        회차 안 요청 간격의 변동계수 (표준편차/평균)
    interval_skew      회차 안 요청 간격 분포의 비대칭도 — 사람은 오래 망설이는 쪽으로 꼬리가 길고(양수),
                       봇의 균등 랜덤 sleep 은 대칭(0 근처)이라 다르게 나온다
    spread_ms          최근 회차들의 오픈 후 첫 도착 시각 흩어짐 (같은 코드는 매번 같은 순간에 쏜다)
  반응 속도 절대값, 접속 지역·PoP·IP 는 feature 로 쓰지 않는다.

[무엇으로 배우나] 정답표 없이 1·2차 판정에서 라벨을 만든다 (약한 지도 학습).
    봇   = 1·2차가 '확실한' 사유로 막은 적 있는 계정 (이름표·위조 토큰·기기 농장·선점·신원 불일치·빈도)
    사람 = 계정 나이 30일 이상 + 1·2차에 한 번도 안 걸린 계정 (믿을 만한 사람)
    나머지는 라벨 없음 → 모델이 확률을 매긴다. 3차 명단(blacklist)·규칙 적중은 라벨로 쓰지 않는다(자기 강화 방지).

[무엇을 내보내나] Kafka bot-rules 에 규칙 묶음 한 버전
    blacklist  : 봇 확률 ≥ 0.9 계정과 그 계정들만 쓴 기기 → 1차(PoP)·2차(중앙)가 다음 요청부터 막는다
    params     : 1·2차 한도 값 다시 계산 (믿을 만한 사람은 한 명도 안 걸리는 선까지 조인다)
    rules      : 봇들만 쓰는 헤더 순서·User-Agent 조각 → 그림자(shadow)로 내보내고,
                 다음 회차들에서 봇만 맞히면 enforce 로 승격, 사람을 맞히면 버린다
    고정 기준점: 믿을 만한 사람이 한 명이라도 걸리는 제안은 버린다.
"""
import argparse
import copy
import hashlib
import json
import math
import os
import socket
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime

import numpy as np

sys.path[:0] = [os.path.dirname(os.path.abspath(__file__)),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pop")]
import bot_rules  # noqa: E402

FEATURES = ["account_age_days", "device_accounts", "concurrent_seats", "interval_cv", "interval_skew", "spread_ms"]
KO = {"account_age_days": "계정 나이(일)", "device_accounts": "같은 기기 계정 수", "concurrent_seats": "1초 안에 노린 좌석 수",
      "interval_cv": "요청 간격 변동계수", "interval_skew": "요청 간격 비대칭도", "spread_ms": "회차 간 도착 흩어짐(ms)"}
# 모델에 넣는 feature. 계정 나이는 '믿을 만한 사람' 라벨을 정하는 데 썼으므로 모델에서는 뺀다
# (넣으면 모델이 라벨 정의를 그대로 외운다). 계정 나이는 2차(기기 농장 판정)가 쓴다.
MODEL_FEATURES = [c for c in FEATURES if c != "account_age_days"]
CERTAIN = ("abnormal_headers", "forged_token", "device_farm", "seat_hoarding", "identity_mismatch", "rate_limit")
P_BOT = 0.9
TRUST_AGE = 30
SPREAD_ROUNDS = 5
DEFAULT_L1 = {"rate": {"ip": [10.0, 30], "account": [10.0, 20], "device": [10.0, 30]}}
DEFAULT_L2 = {"rate": {"account": [10.0, 30], "device": [10.0, 40]},
              "device_accounts": {"max": 3, "young_days": 7}, "hold": {"max_unpaid": 2}}

if os.environ.get("TUNNEL_HOST"):   # 서울 pod: 판교 Kafka 이름을 역방향 터널 입구로 (collector.py 와 같음)
    _orig = socket.getaddrinfo
    socket.getaddrinfo = lambda h, *a, **k: _orig(os.environ["TUNNEL_HOST"] if h == "kafka-external" else h, *a, **k)


# --------------------------------------------------------------------------- 궤적
def ts_of(e):
    us = e.get("pop_arrival_epoch_us")
    if us:
        return us / 1e6
    la = e.get("logged_at")
    return la if isinstance(la, (int, float)) else None


def profiles(events, opens):
    """계정별 궤적 → feature, 라벨 재료, 규칙 재료."""
    opens = sorted(opens)
    lo = opens[0] - 2 if opens else 0
    acc = defaultdict(lambda: {"reqs": [], "blocks": Counter(), "unpaid": 0, "age": None, "devs": set(),
                               "orders": Counter(), "uas": Counter(), "shadow": Counter()})
    dev_accts = defaultdict(set)
    for e in events:
        t = ts_of(e)
        a = e.get("account_id")
        if t is None or t < lo or not a:
            continue
        p = acc[a]
        if e.get("account_age_days") is not None:
            p["age"] = e["account_age_days"]
        if e.get("device_fp"):
            p["devs"].add(e["device_fp"])
            dev_accts[e["device_fp"]].add(a)
        et = e.get("event_type")
        if et in ("seat_request", "first_layer_block"):
            p["reqs"].append((t, e.get("seat_id"), e.get("pop_id")))
            h = e.get("headers") or {}
            if h.get("order"):
                p["orders"][tuple(h["order"])] += 1
            ua = (h.get("values") or {}).get("user-agent") or e.get("user_agent")
            if ua:
                p["uas"][ua] += 1
            for s in e.get("first_layer_shadow") or []:
                p["shadow"][s] += 1
            if et == "first_layer_block" and e.get("first_layer_reason"):
                p["blocks"]["L1:" + e["first_layer_reason"]] += 1
        elif et in ("second_layer_block", "second_layer_flag"):
            if e.get("second_layer_reason"):
                p["blocks"]["L2:" + e["second_layer_reason"]] += 1
            for s in e.get("second_layer_shadow") or []:
                p["shadow"][s] += 1
        elif et == "hold_expired":
            p["unpaid"] += 1

    def round_of(t):
        best = None
        for o in opens:
            if o - 2 <= t < o + 45:
                best = o
        return best

    out = {}
    for a, p in acc.items():
        per = defaultdict(list)
        for t, seat, pop in p["reqs"]:
            r = round_of(t)
            if r is not None:
                per[r].append((t, seat))
        firsts, intervals, conc = [], [], 0
        for r, xs in sorted(per.items()):
            xs.sort()
            firsts.append((xs[0][0] - r) * 1000)
            intervals += [b[0] - a_[0] for a_, b in zip(xs, xs[1:])]
            for i, (t, _) in enumerate(xs):
                conc = max(conc, len({s for tt, s in xs[i:] if tt - t <= 1.0 and s}))
        f = {"account_age_days": p["age"],
             "device_accounts": max((len(dev_accts[d]) for d in p["devs"]), default=None),
             "concurrent_seats": conc or None,
             "interval_cv": None, "interval_skew": None, "spread_ms": None}
        if len(intervals) >= 3:
            iv = np.array(intervals)
            m, sd = iv.mean(), iv.std()
            f["interval_cv"] = float(sd / m) if m > 0 else None
            f["interval_skew"] = float(((iv - m) ** 3).mean() / sd ** 3) if sd > 0 else 0.0
        if len(firsts) >= 3:
            f["spread_ms"] = float(np.std(firsts[-SPREAD_ROUNDS:]))
        certain = any(k.split(":", 1)[1].startswith(CERTAIN) for k in p["blocks"])
        trusted = (p["age"] or 0) >= TRUST_AGE and not p["blocks"] and len(per) >= 2
        out[a] = {"f": f, "n_rounds": len(per), "label": 1 if certain else (0 if trusted else None),
                  "blocks": dict(p["blocks"]), "unpaid": p["unpaid"], "devs": sorted(p["devs"]),
                  "orders": p["orders"], "uas": p["uas"], "shadow": dict(p["shadow"]),
                  "reqs": p["reqs"]}
    return out


# --------------------------------------------------------------------------- 모델
def fit_predict(prof):
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.inspection import permutation_importance
    accts = [a for a, p in prof.items() if p["n_rounds"] >= 1]
    X = np.array([[np.nan if prof[a]["f"][c] is None else prof[a]["f"][c] for c in MODEL_FEATURES] for a in accts], float)
    y = np.array([-1 if prof[a]["label"] is None else prof[a]["label"] for a in accts])
    lab = y >= 0
    if len(set(y[lab])) < 2 or lab.sum() < 6:
        return {}, {}, {}
    # 아직 값이 거의 없는 feature 는 이번 학습에서 뺀다 (초반 회차엔 요청 간격·흩어짐이 비어 있다)
    use = [j for j in range(len(MODEL_FEATURES)) if np.isfinite(X[lab][:, j]).sum() >= 4]
    m = HistGradientBoostingClassifier(max_iter=200, min_samples_leaf=3, random_state=0).fit(X[lab][:, use], y[lab])
    proba = dict(zip(accts, m.predict_proba(X[:, use])[:, 1]))
    imp = permutation_importance(m, X[lab][:, use], y[lab], n_repeats=5, random_state=0).importances_mean
    importance = {c: 0.0 for c in MODEL_FEATURES}
    importance.update({MODEL_FEATURES[j]: float(v) for j, v in zip(use, imp)})
    # 계정별 근거: 중요도가 큰 feature 중 이 계정 값이 믿을 만한 사람들의 범위를 벗어난 것
    hum = X[y == 0]
    med = np.nanmedian(hum, axis=0) if len(hum) else np.full(len(MODEL_FEATURES), np.nan)
    iqr = (np.nanpercentile(hum, 75, axis=0) - np.nanpercentile(hum, 25, axis=0)) if len(hum) else med
    reasons = {}
    for a, row in zip(accts, X):
        cand = []
        for j, c in enumerate(MODEL_FEATURES):
            if np.isnan(row[j]) or np.isnan(med[j]):
                continue
            z = abs(row[j] - med[j]) / (iqr[j] if iqr[j] > 0 else (abs(med[j]) or 1.0))
            cand.append((importance[c] * z, c, row[j], med[j]))
        cand.sort(reverse=True)
        reasons[a] = [f"{KO[c]} {v:.3g} (사람 중앙 {mm:.3g})" for s, c, v, mm in cand[:3] if s > 0]
    return proba, importance, reasons


# --------------------------------------------------------------------------- 정교화
def peaks(reqs, window, by_pop):
    """창(초) 안 요청 수의 최댓값. by_pop=True 면 PoP 별(1차), 아니면 전국 합산(2차)."""
    groups = defaultdict(list)
    for t, _s, pop in reqs:
        groups[pop if by_pop else "*"].append(t)
    best = 0
    for ts in groups.values():
        ts.sort()
        j = 0
        for i in range(len(ts)):
            while ts[i] - ts[j] > window:
                j += 1
            best = max(best, i - j + 1)
    return best


def refine(prof, proba, rs, round_no):
    """1·2차 한도와 규칙을 다시 계산한다. 반환 (새 params, 새 rules, 바뀐 점 목록)."""
    bots = {a for a, p in proba.items() if p >= P_BOT and prof[a]["label"] != 0}
    humans = {a for a, p in prof.items() if p["label"] == 0}
    params = copy.deepcopy(rs.get("params") or {})
    l1 = params.setdefault("l1", copy.deepcopy(DEFAULT_L1))
    l2 = params.setdefault("l2", copy.deepcopy(DEFAULT_L2))
    l1.setdefault("rate", copy.deepcopy(DEFAULT_L1["rate"]))
    for k, v in DEFAULT_L2.items():
        l2.setdefault(k, copy.deepcopy(v))
    notes = []

    def tighten(name, cur, values_h, values_b, floor, margin=1):
        """믿을 만한 사람 최댓값 + 여유 까지 조인다. 조여서 더 잡히는 봇이 있을 때만."""
        if not values_h:
            return cur
        new = max(floor, max(values_h) + margin)
        if new < cur and any(new < v <= cur for v in values_b):
            notes.append(f"{name}: {cur} → {new} (사람 최대 {max(values_h)}, 새로 걸리는 봇 "
                         f"{sum(1 for v in values_b if new < v <= cur)})")
            return new
        return cur

    for layer, cfg, by_pop in (("1차", l1, True), ("2차", l2, False)):
        for kind in ("account", "device"):
            win, cur = cfg["rate"][kind]
            vh = [peaks(prof[a]["reqs"], win, by_pop) for a in humans]
            vb = [peaks(prof[a]["reqs"], win, by_pop) for a in bots]
            # 요청 수는 새로고침 등으로 사람마다 크게 다르므로 넉넉히: 최소 5, 사람 최대의 2배
            cfg["rate"][kind] = [win, tighten(f"{layer} {kind} 요청 한도({win:g}초)", cur, vh, vb, 5,
                                              margin=max(vh, default=0) + 1)]
    da = l2["device_accounts"]
    da["max"] = tighten("2차 기기당 계정 수", da["max"],
                        [prof[a]["f"]["device_accounts"] or 1 for a in humans],
                        [prof[a]["f"]["device_accounts"] or 1 for a in bots], 1)
    l2["hold"]["max_unpaid"] = tighten("2차 미결제 한도", l2["hold"]["max_unpaid"],
                                       [prof[a]["unpaid"] for a in humans], [prof[a]["unpaid"] for a in bots], 1)

    # ---- 규칙: 지금 그림자 규칙의 성적 → 승격 / 폐기
    rules = []
    for r in rs.get("rules") or []:
        hit_h = sum(prof[a]["shadow"].get(r["id"], 0) for a in humans if a in prof)
        hit_b = sum(prof[a]["shadow"].get(r["id"], 0) for a in bots if a in prof)
        if r.get("mode") == "shadow":
            if hit_h:
                notes.append(f"규칙 {r['id']} 폐기 (그림자 기간에 사람 {hit_h}건 적중)")
                continue
            if hit_b >= 2 and round_no - r.get("since_round", round_no) >= 2:
                r = {**r, "mode": "enforce"}
                notes.append(f"규칙 {r['id']} 승격 → enforce (그림자 기간 봇 {hit_b}건, 사람 0건)")
        rules.append(r)
    have = {json.dumps(c["all"], sort_keys=True) for c in rules}

    # ---- 새 규칙 후보: 봇들만 쓰는 헤더 순서, User-Agent 조각 (1차 = 요청 한 건으로 판단)
    def add(rid, why, cond):
        key = json.dumps(cond, sort_keys=True)
        if key in have:
            return
        rule = {"id": rid, "layer": 1, "mode": "shadow", "why": why, "all": cond, "since_round": round_no}
        # 재실행: 믿을 만한 사람의 기록에 맞으면 버린다
        test = bot_rules.validate({"version": "t", "rules": [dict(rule, mode="enforce")]})
        for a in humans:
            for o in prof[a]["orders"]:
                for ua in prof[a]["uas"] or [None]:
                    rec = {"headers": {"order": list(o), "values": {"user-agent": ua}}}
                    if bot_rules.match(test, rec):
                        return
        have.add(key)
        rules.append(rule)
        notes.append(f"새 그림자 규칙 {rid}: {why}")

    order_bots = Counter()
    for a in bots:
        for o in prof[a]["orders"]:
            order_bots[o] += 1
    human_orders = {o for a in humans for o in prof[a]["orders"]}
    for o, n in order_bots.most_common(5):
        if n >= 2 and o not in human_orders:
            add("hdr-" + hashlib.sha1("|".join(o).encode()).hexdigest()[:6], f"봇 {n}개 계정만 쓰는 헤더 순서",
                [{"f": "headers.order", "op": "eq", "v": list(o)}])

    def tokens(ua):
        for ch in "();,":
            ua = ua.replace(ch, " ")
        return {t.lower() for t in ua.split() if len(t) >= 4}
    tok_bots, tok_h = Counter(), set()
    for a in bots:
        for t in set().union(*[tokens(u) for u in prof[a]["uas"]] or [set()]):
            tok_bots[t] += 1
    for a in humans:
        for u in prof[a]["uas"]:
            tok_h |= tokens(u)
    for t, n in tok_bots.most_common(5):
        if n >= 2 and t not in tok_h:
            add("ua-" + hashlib.sha1(t.encode()).hexdigest()[:6], f"봇 {n}개 계정만 쓰는 User-Agent 조각 '{t}'",
                [{"f": "headers.values.user-agent", "op": "contains", "v": t}])
    return params, rules, notes


# --------------------------------------------------------------------------- 한 회차 분석
def analyze(events, rounds, rs):
    opens = [rd["open_at_ms"] / 1000 for rd in rounds]
    prof = profiles(events, opens)
    proba, importance, reasons = fit_predict(prof)
    if not proba:
        return None
    humans = {a for a, p in prof.items() if p["label"] == 0}
    flagged = sorted(a for a, p in proba.items() if p >= P_BOT and a not in humans)
    devs_h = {d for a in humans for d in prof[a]["devs"]}
    devices = sorted({d for a in flagged for d in prof[a]["devs"]} - devs_h)
    params, rules, notes = refine(prof, proba, rs, len(rounds))
    new = {"version": rs.get("version", "v0"), "approved_by": "ai3-auto-check",
           "params": params, "blacklist": {"accounts": flagged, "devices": devices}, "rules": rules}
    return {"prof": prof, "proba": proba, "importance": importance, "reasons": reasons,
            "ruleset": new, "notes": notes, "humans": humans}


def same(a, b):
    k = ("params", "blacklist", "rules")
    return json.dumps({x: a.get(x) for x in k}, sort_keys=True) == json.dumps({x: b.get(x) for x in k}, sort_keys=True)


def summarize(res, rounds):
    """사람이 읽는 한 줄 + 정답이 있으면(실험판) 봇 종류별 표."""
    truth = {}
    for rd in rounds:
        for b in rd.get("bots", []):
            if b.get("account_id"):
                truth[b["account_id"]] = b.get("bot")
    lines = [f"  feature 중요도: " + ", ".join(f"{KO[c]} {v:.3f}" for c, v in
                                              sorted(res["importance"].items(), key=lambda x: -x[1]))]
    if truth:
        tab = defaultdict(Counter)
        for a, p in res["prof"].items():
            who = truth.get(a, "human")
            stage = ("1차" if any(k.startswith("L1:") for k in p["blocks"]) else
                     "2차" if any(k.startswith("L2:") for k in p["blocks"]) else
                     "3차" if res["proba"].get(a, 0) >= P_BOT else "통과")
            tab[who][stage] += 1
        for who, c in sorted(tab.items()):
            lines.append(f"  {who:16} " + "  ".join(f"{s} {c[s]}" for s in ("1차", "2차", "3차", "통과")))
    return "\n".join(lines)


# --------------------------------------------------------------------------- 실행
LOG, LOCK = [], threading.Lock()


def follow(bootstrap):
    from kafka import KafkaConsumer
    c = KafkaConsumer("detection-log", bootstrap_servers=bootstrap, group_id=None,
                      auto_offset_reset="earliest", value_deserializer=lambda v: json.loads(v))
    for m in c:
        with LOCK:
            LOG.append(m.value)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", required=True, help="회차 오픈 시각 기록 (rounds.jsonl). 실험판이면 봇 정답도 있어 표를 찍는다")
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--archive", help="Kafka 대신 detection-log 덤프(jsonl)")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--log", default="ai3.jsonl")
    a = ap.parse_args()

    if a.archive:
        LOG.extend(json.loads(l) for l in open(a.archive) if l.strip())
    else:
        threading.Thread(target=follow, args=(a.bootstrap,), daemon=True).start()
    producer = None
    if not a.once:
        from kafka import KafkaProducer
        producer = KafkaProducer(bootstrap_servers=a.bootstrap)
    current = {"version": "v0", "params": {}, "blacklist": {"accounts": [], "devices": []}, "rules": []}
    seen, ver = 0, 0
    print("[서울 AI 3차] 시작 — 라벨은 1·2차 판정에서, 명단·규칙·한도를 회차마다 다시 계산", flush=True)
    while True:
        if not os.path.exists(a.rounds):   # 첫 회차가 끝나기 전
            time.sleep(1)
            continue
        rounds = [json.loads(l) for l in open(a.rounds) if l.strip()]
        if len(rounds) == seen and not a.once:
            time.sleep(1)
            continue
        seen = len(rounds)
        if not a.once:
            time.sleep(25)   # 결제 기한 만료(hold_expired)와 2차 기록이 들어올 틈
        t0 = time.time()
        with LOCK:
            events = list(LOG)
        try:
            res = analyze(events, rounds, current)
        except Exception as e:   # 분석이 실패해도 루프는 멈추지 않고 지금 규칙을 유지한다
            print(f"[서울 AI 3차] 분석 실패, 이전 규칙 유지: {e!r}", flush=True)
            if a.once:
                raise
            continue
        last = rounds[-1]["round"] if rounds else 0
        if res is None:
            print(f"[서울 AI 3차] {last}회차 후: 라벨이 아직 부족 (봇·사람 둘 다 필요)", flush=True)
            if a.once:
                return
            continue
        rec = {"after_round": last, "at": datetime.now().isoformat(timespec="seconds"),
               "flagged": len(res["ruleset"]["blacklist"]["accounts"]),
               "devices": len(res["ruleset"]["blacklist"]["devices"]),
               "importance": res["importance"], "notes": res["notes"],
               "top": sorted(([acct, round(float(p), 3), res["reasons"].get(acct, [])]
                              for acct, p in res["proba"].items() if p >= P_BOT), key=lambda x: -x[1])[:10]}
        if same(res["ruleset"], current):
            rec["deployed"] = False
            print(f"[서울 AI 3차] {last}회차 후 {time.time() - t0:.1f}초: 변화 없음 (명단 {rec['flagged']})", flush=True)
        else:
            ver += 1
            new = dict(res["ruleset"], version=f"v{ver}-ai3-r{last}", approved_at=rec["at"])
            bot_rules.validate(json.loads(json.dumps(new)))
            if producer:
                producer.send("bot-rules", key=b"ruleset", value=json.dumps(new, ensure_ascii=False).encode()).get(10)
            current = new
            rec |= {"deployed": True, "version": new["version"], "seconds": round(time.time() - t0, 2)}
            print(f"[서울 AI 3차] {last}회차 후 → {new['version']} 배포 {rec['seconds']}초 | 명단 계정 "
                  f"{rec['flagged']} · 기기 {rec['devices']} | 규칙 {len(new['rules'])}개", flush=True)
            for n in res["notes"]:
                print(f"    정교화: {n}", flush=True)
        print(summarize(res, rounds), flush=True)
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        if a.once:
            return


if __name__ == "__main__":
    main()
