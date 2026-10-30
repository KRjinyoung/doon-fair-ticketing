#!/usr/bin/env python3
"""
같은 순간 클릭 대결 — 판교와 대전이 같은 시각에 같은 좌석을 누른다  [DUEL_V1]  (표준 라이브러리만)

판교 서버와 대전 서버에서 같은 --start 로 동시에 실행한다. 좌석 i 는 start + i*gap 초에 양쪽이 함께 쏜다.
  짝수 i  DOON 방식 (DUEL-D-xxx) : 각자 가까운 PoP 로 보낸다. 대전 → 대전 PoP
  홀수 i  기존 방식 (DUEL-B-xxx) : 대전도 판교 PoP 로 보낸다(장거리). 한 곳에서 도착순으로 갈리는 대기열과 같은 조건

  판교:  python3 scripts/duel.py --side pangyo  --start 15:30:00
  대전:  python3 scripts/duel.py --side daejeon --start 15:30:00
         (대전 → 판교는 daejeon_relay.sh 의 14198 중계로 간다)

연결은 미리 열어 둔다 (이미 예매 페이지에 들어와 있는 사람과 같은 조건, TCP 연결 시간 제외).
결과는 한 줄에 하나씩 JSON 으로 출력한다. 집계는 판교에서 scripts/duel_report.py.
"""
import argparse
import http.client
import json
import time
from datetime import datetime
from urllib.parse import urlparse


def conn_for(url):
    u = urlparse(url)
    c = http.client.HTTPConnection(u.hostname, u.port or 80, timeout=10)
    c.request("GET", "/health")
    c.getresponse().read()
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--side", required=True, choices=["pangyo", "daejeon"])
    ap.add_argument("--start", required=True, help="첫 발사 시각 hh:mm:ss (오늘, 양쪽 같게)")
    ap.add_argument("--n", type=int, default=100, help="방식마다 좌석 수")
    ap.add_argument("--gap", type=float, default=1.0, help="좌석 사이 간격(초)")
    ap.add_argument("--local", default="http://localhost:13198", help="내 지역 PoP")
    ap.add_argument("--remote", default="http://localhost:14198", help="대전에서 판교 PoP 로 가는 길")
    ap.add_argument("--out", default="duel.jsonl")
    a = ap.parse_args()

    start = datetime.combine(datetime.now().date(), datetime.strptime(a.start, "%H:%M:%S").time()).timestamp()
    if start - time.time() < 10:
        raise SystemExit("시작 시각이 10초 안이거나 지났다. 넉넉히 뒤로 잡을 것")
    conns = {"local": conn_for(a.local)}
    if a.side == "daejeon":
        conns["remote"] = conn_for(a.remote)
    hdr = {"Content-Type": "application/json", "Accept-Language": "ko-KR,ko;q=0.9",
           "User-Agent": f"Mozilla/5.0 duel-{a.side}"}
    print(f"[{a.side}] {datetime.fromtimestamp(start):%H:%M:%S} 시작, 좌석 {2 * a.n}개, {a.gap}초 간격", flush=True)

    with open(a.out, "w") as f:
        for i in range(2 * a.n):
            mode = "D" if i % 2 == 0 else "B"
            seat = f"DUEL-{mode}-{i:03d}"
            via = "remote" if (a.side == "daejeon" and mode == "B") else "local"
            c = conns[via]
            t = start + i * a.gap
            while (left := t - time.time()) > 0.05:
                time.sleep(left - 0.05)
            while time.time() < t:
                pass
            fired = time.time_ns()
            try:
                c.request("POST", "/seat-request", json.dumps({"seat_id": seat}), hdr)
                r = c.getresponse()
                body = json.loads(r.read() or b"null")
                status = r.status
            except Exception as e:  # 연결이 끊겼으면 다시 열고 이 좌석은 실패로 남긴다
                conns[via] = conn_for(a.local if via == "local" else a.remote)
                body, status = {"error": str(e)}, None
            rec = {"side": a.side, "mode": mode, "seat": seat, "via": via, "fired_ns": fired,
                   "rtt_ms": round((time.time_ns() - fired) / 1e6, 3), "status": status,
                   "request_id": (body or {}).get("request_id"), "pop_id": (body or {}).get("pop_id")}
            f.write(json.dumps(rec) + "\n")
            f.flush()
    print(f"[{a.side}] 끝 → {a.out}", flush=True)


if __name__ == "__main__":
    main()
