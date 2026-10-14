#!/usr/bin/env python3
"""
매크로 봇 1·2단계 — 화면 없이 좌석 API 를 바로 친다  [BOTS_V1]  (표준 라이브러리만)

  1단계 단순 봇   : 헤더를 신경 안 쓴다. User-Agent 가 Python-urllib 이라 v0 규칙에 걸린다.
  2단계 위장 봇   : --disguise. 크롬 User-Agent 와 Accept-Language 를 붙여 v0 를 통과한다.
                    다만 페이지를 안 거치므로 Referer/Origin 이 없다 (서울 AI 가 찾을 신호).

  python3 script_bot.py --base http://localhost:18000 --at 21:00:00.000
  python3 script_bot.py --base ... --disguise --email bot01@doon.kr --at 21:00:00.000

반드시 격리된 시험 환경을 겨눈다. 운영 판교/대전에는 쏘지 않는다.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from doonload import all_seats  # noqa: E402

CHROME_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36")


def call(base, path, headers, body=None):
    req = urllib.request.Request(base + path, headers=headers,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:200]


def wait_until(hms):
    """오늘 hh:mm:ss.fff 까지 기다린다. 마지막 50ms 는 바쁜 대기로 ms 단위를 맞춘다."""
    target = datetime.combine(datetime.now().date(),
                              datetime.strptime(hms, "%H:%M:%S.%f").time()).timestamp()
    while (left := target - time.time()) > 0.05:
        time.sleep(left - 0.05)
    while time.time() < target:
        pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="시험 환경 주소, 예: http://localhost:18000")
    ap.add_argument("--disguise", action="store_true", help="2단계: 브라우저처럼 헤더 위장")
    ap.add_argument("--email", help="봇 계정 (없으면 익명)")
    ap.add_argument("--password", default="doon1234")
    ap.add_argument("--device-fp", default="macro-farm-01", help="계정 여러 개가 같은 기기를 쓴다")
    ap.add_argument("--seat", help="노릴 좌석 (없으면 남은 첫 좌석)")
    ap.add_argument("--at", help="발사 시각 hh:mm:ss.fff (없으면 즉시)")
    a = ap.parse_args()

    h = {"Content-Type": "application/json"}
    if a.disguise:
        h |= {"User-Agent": CHROME_UA, "Accept": "*/*", "Accept-Language": "ko-KR,ko;q=0.9"}

    if a.email:
        code, data = call(a.base, "/auth/login", h,
                          {"email": a.email, "password": a.password, "device_fp": a.device_fp})
        if code != 200:
            sys.exit(f"로그인 실패 {code}: {data}")
        h["Authorization"] = "Bearer " + data["token"]

    seat = a.seat
    if not seat:  # 오픈 전에 미리 빈 좌석을 골라둔다 (매크로는 고민하지 않는다)
        code, taken = call(a.base, "/seats-overview", h)
        taken = taken if code == 200 else {}
        seat = next(s for s in all_seats() if taken.get(s) in (None, "available"))

    if a.at:
        wait_until(a.at)
    t0 = time.time()
    code, data = call(a.base, "/seat-request", h, {"seat_id": seat})
    print(json.dumps({"bot": "disguised" if a.disguise else "simple", "account": a.email,
                      "seat": seat, "fired_at": datetime.fromtimestamp(t0).isoformat(timespec="milliseconds"),
                      "status": code, "reply": data if code != 200 else data.get("request_id")},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
