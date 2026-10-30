#!/usr/bin/env python3
"""
같은 순간 클릭 대결 집계  [DUEL_V1]  (판교에서)

  docker compose -f docker-compose.pangyo.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 --topic seat-status --from-beginning --timeout-ms 20000 > duel_status.txt
  python3 scripts/duel_report.py duel.jsonl duel_status.txt

duel.jsonl = 판교 쪽 duel.py 출력. 대전 쪽 요청은 '판교가 보낸 것이 아닌 요청'으로 구분한다.
  기존 방식(B): 두 요청 모두 판교 PoP 한 시계로 찍혔다 → 원본 도착 시각이 빠른 쪽이 이긴다(서버 도착순).
               (DOON 판정은 같은 PoP 끼리도 시계 오차 범위를 추첨하므로, 기존 방식은 원본 도착 순서로 따로 센다)
  DOON 방식(D): 중앙이 실제로 정한 승자(winner_request_id)와 추첨 여부를 그대로 센다.
"""
import json
import statistics as st
import sys


def main():
    mine = {}
    for line in open(sys.argv[1]):
        r = json.loads(line)
        if r.get("request_id"):
            mine[r["seat"]] = r["request_id"]

    final = {}
    for line in open(sys.argv[2]):
        line = line.strip()
        if not line.startswith("{"):
            continue
        m = json.loads(line)
        if str(m.get("seat_id", "")).startswith("DUEL-") and m.get("status") == "confirmed":
            final[m["seat_id"]] = m

    res = {"B": [], "D": []}
    for seat, m in sorted(final.items()):
        mode = seat.split("-")[1]
        reqs = m.get("all_requests") or []
        pg = [e for e in reqs if e["request_id"] == mine.get(seat)]
        dj = [e for e in reqs if e["request_id"] != mine.get(seat)]
        if len(pg) != 1 or len(dj) != 1:
            continue  # 한쪽 요청이 빠진 좌석은 비교할 수 없다
        p, d = pg[0], dj[0]
        if mode == "B":
            raw = lambda e: e.get("pop_arrival_epoch_us") or e["pop_arrival_epoch_ms"] * 1000
            gap = (raw(d) - raw(p)) / 1000
            winner = "pangyo" if gap > 0 else "daejeon"
            lottery = False
        else:
            gap = d["corrected_epoch_ms"] - p["corrected_epoch_ms"]
            winner = "pangyo" if m["winner_request_id"] == p["request_id"] else "daejeon"
            lottery = bool(m.get("lottery"))
        res[mode].append({"gap": gap, "winner": winner, "lottery": lottery,
                          "err": (p.get("clock_error_ms"), d.get("clock_error_ms"))})

    for mode, name in (("B", "기존 방식 (둘 다 판교 서버, 도착순)"), ("D", "DOON (각자 가까운 PoP, 보정 시각 + 추첨)")):
        rs = res[mode]
        if not rs:
            print(f"{name}: 결과 없음")
            continue
        gaps = sorted(r["gap"] for r in rs)
        dj = sum(r["winner"] == "daejeon" for r in rs)
        lot = [r for r in rs if r["lottery"]]
        print(f"\n{name}  — 좌석 {len(rs)}개")
        print(f"  대전 도착이 판교보다 늦은 정도(ms): 중앙 {st.median(gaps):.3f}, "
              f"최소 {gaps[0]:.3f}, 최대 {gaps[-1]:.3f}")
        print(f"  대전 승: {dj}/{len(rs)} ({dj / len(rs):.0%})   판교 승: {len(rs) - dj}")
        if mode == "D":
            ldj = sum(r["winner"] == "daejeon" for r in lot)
            print(f"  추첨으로 정해진 좌석: {len(lot)}  (그중 대전 승 {ldj})")
            print(f"  시계 보장 오차(ms) 예: 판교 {rs[0]['err'][0]}, 대전 {rs[0]['err'][1]}")


if __name__ == "__main__":
    main()
