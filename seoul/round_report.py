#!/usr/bin/env python3
"""
round_report.py -- 수집 회차별로 "누가 1차 필터에 걸렸나"를 표로 보여준다.  (서울에서 실행)

  python3.11 round_report.py rounds.jsonl            # 보관 로그 기본 위치 ~/data/detection-log
  python3.11 round_report.py rounds.jsonl --archive DIR --window 30

정답표 = 판교 bots/rounds.py 가 남긴 rounds.jsonl (scp 로 가져온다).
  rounds 의 bots[].account_id  -> 그 봇 종류 (simple / disguised / browser-stealth)
  그 밖의 로그인 계정          -> human
  계정 없음                    -> anonymous
각 회차는 오픈 2초 전부터 --window 초까지의 detection-log 를 센다 (seat.html 은 30초 열린다.
기기 시계가 빠른 사람은 실제 오픈보다 조금 일찍 도착할 수 있어 앞쪽도 본다).

판정 열
  통과      : 1차 필터를 지나 좌석 요청까지 간 것
  v0 차단   : 하드코딩 규칙(v0)에 걸린 것
  AI규칙    : 서울이 배포한 규칙(reason 이 rule:<id>)에 걸린 것
사람 행의 '차단'은 곧 오탐이다. monitor 모드면 실제로 막지는 않고 '걸렸다'는 뜻이다.
"""
import argparse
import glob
import gzip
import json
import os
from collections import Counter, defaultdict


def load_log(archive):
    seen, rows = set(), []
    for f in sorted(glob.glob(os.path.join(os.path.expanduser(archive), "*", "*[0-9].jsonl.gz"))):
        for line in gzip.open(f, "rt"):
            r = json.loads(line)
            k = (r["_kafka"]["partition"], r["_kafka"]["offset"])
            if k not in seen:  # 수집기는 at-least-once 라 중복이 있을 수 있다
                seen.add(k)
                rows.append(r)
    return rows


def verdict(r):
    if r.get("first_layer_passed") is not False:
        return "통과"
    return "AI규칙" if str(r.get("first_layer_reason") or "").startswith("rule:") else "v0 차단"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("rounds")
    ap.add_argument("--archive", default="~/data/detection-log")
    ap.add_argument("--window", type=float, default=30, help="오픈 후 몇 초까지 그 회차로 셀지")
    a = ap.parse_args()

    rounds = [json.loads(l) for l in open(a.rounds) if l.strip()]
    log = load_log(a.archive)
    cols = ["통과", "v0 차단", "AI규칙"]
    total = defaultdict(Counter)
    human_pop = defaultdict(Counter)
    modes = Counter()

    for rd in rounds:
        bot_of = {b["account_id"]: b.get("bot", "?") for b in rd["bots"] if b.get("account_id")}
        t0 = rd["open_at_ms"] * 1000 - 2e6
        t1 = t0 + 2e6 + a.window * 1e6
        tab = defaultdict(Counter)
        for r in log:
            if not t0 <= (r.get("pop_arrival_epoch_us") or 0) < t1:
                continue
            acct = r.get("account_id")
            who = bot_of.get(acct, "human" if acct else "anonymous")
            v = verdict(r)
            modes[r.get("first_layer_mode")] += 1
            tab[who][v] += 1
            total[who][v] += 1
            if who == "human":
                human_pop[r.get("pop_id")][v] += 1
        missing = [b.get("bot", "?") for b in rd["bots"] if not b.get("account_id")]
        print(f"\n[회차 {rd['round']}] {rd['open_at']}" + (f"  (로그인 실패: {', '.join(missing)})" if missing else ""))
        show(tab, cols)

    print(f"\n=== 전체 ===  (1차 필터 모드: {dict(modes)})")
    show(total, cols)
    if human_pop:
        print("\n사람 요청 PoP별 (지역별 오탐이 고른지)")
        show(human_pop, cols)


def show(tab, cols):
    if not tab:
        print("  (로그 없음)")
        return
    print(f"  {'':18}" + "".join(f"{c:>9}" for c in cols) + f"{'차단율':>9}")
    for who in sorted(tab, key=str):
        c = tab[who]
        n = sum(c.values())
        blocked = n - c["통과"]
        print(f"  {str(who):18}" + "".join(f"{c[k]:>9}" for k in cols) + f"{blocked / n:>9.0%}")


if __name__ == "__main__":
    main()
