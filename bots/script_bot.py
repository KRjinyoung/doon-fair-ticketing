#!/usr/bin/env python3
"""
매크로 봇 — 화면 없이 좌석 API 를 바로 친다  [BOTS_V1] [MACRO3_V1]  (표준 라이브러리만)

  단순 봇   : 헤더를 신경 안 쓴다. User-Agent 가 Python-urllib 이라 1차 v0 규칙에 걸린다.
  위장 봇   : --disguise. 크롬 User-Agent 와 Accept-Language 를 붙여 1차를 통과한다.
              Referer/Origin 은 없다 (페이지를 안 거친다).
  그 밖의 행동 (rounds.py 가 봇 종류마다 조합한다)
    --device-fp random : 로그인할 때마다 기기 지문을 새로 만든다 (안티디텍트 브라우저 흉내)
    --xff random       : 요청마다 다른 IP 로 보이게 한다 (프록시 풀 흉내, 실험판 nginx 만 믿는 헤더)
    --seats N          : 오픈 순간 좌석 N 개를 한꺼번에 노린다 (선점)
    --attempts K --sleep A B : 좌석 K 개를 A~B 초 균등 랜덤 간격으로 차례로 시도한다 (반복)
    --no-pay           : 이겨도 결제하지 않는다 (좌석 선점 봇). 기본은 이기면 결제한다.

  python3 script_bot.py --base http://localhost:18000 --at 21:00:00.000
  python3 script_bot.py --base ... --disguise --email bot01@doon.kr --at 21:00:00.000

격리된 시험 환경이나, 사람과 합의한 수집 시간의 운영 판교(rounds.py)에만 쏜다.
"""

import argparse
import json
import os
import random
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


def wait_until(t):
    """epoch 초 t 까지 기다린다. 마지막 50ms 는 바쁜 대기로 ms 단위를 맞춘다."""
    while (left := t - time.time()) > 0.05:
        time.sleep(left - 0.05)
    while time.time() < t:
        pass


def rand_ip():
    return f"10.{random.randint(64, 127)}.{random.randint(0, 255)}.{random.randint(1, 254)}"


def result(base, h, seat, rid, limit=12):
    """seat-status 를 0.3초마다 본다. True=이김, False=짐, None=모름."""
    end = time.time() + limit
    while time.time() < end:
        time.sleep(0.3)
        code, d = call(base, f"/seat-status/{seat}", h)
        if code == 200 and isinstance(d, dict) and d.get("status") == "confirmed":
            return d.get("winner_request_id") == rid
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", required=True, help="시험 환경 주소, 예: http://localhost:18000")
    ap.add_argument("--disguise", action="store_true", help="브라우저처럼 헤더 위장")
    ap.add_argument("--email", help="봇 계정 (없으면 익명)")
    ap.add_argument("--password", default="doon1234")
    # scripts/seed_accounts.sh 가 봇 계정을 로그인시킨 그 기기 값과 같게 둔다 (농장)
    ap.add_argument("--device-fp", default="a1b2c3d4e5f60789", help="기기 지문. random 이면 새로 만든다")
    ap.add_argument("--xff", help="random 이면 요청마다 다른 IP 로 보이게 한다")
    ap.add_argument("--seat", help="노릴 좌석 (없으면 남은 좌석 중 무작위)")
    ap.add_argument("--seats", type=int, default=1, help="오픈 순간 한꺼번에 노릴 좌석 수")
    ap.add_argument("--attempts", type=int, default=1, help="차례로 시도할 좌석 수")
    ap.add_argument("--sleep", type=float, nargs=2, default=(0.3, 1.2), help="시도 사이 균등 랜덤 간격(초)")
    ap.add_argument("--no-pay", action="store_true", help="이겨도 결제하지 않는다")
    ap.add_argument("--label", help="출력에 남길 봇 종류 이름")
    ap.add_argument("--at", help="발사 시각 hh:mm:ss.fff (없으면 즉시)")
    a = ap.parse_args()

    h = {"Content-Type": "application/json"}
    if a.disguise:
        h |= {"User-Agent": CHROME_UA, "Accept": "*/*", "Accept-Language": "ko-KR,ko;q=0.9"}
    fp = "%016x" % random.getrandbits(64) if a.device_fp == "random" else a.device_fp

    def hx():
        return h | ({"X-Forwarded-For": rand_ip()} if a.xff == "random" else {})

    account_id = None
    if a.email:
        code, data = call(a.base, "/auth/login", hx(), {"email": a.email, "password": a.password, "device_fp": fp})
        if code != 200:
            sys.exit(f"로그인 실패 {code}: {data}")
        h["Authorization"] = "Bearer " + data["token"]
        account_id = data.get("account_id")

    # 오픈 전에 미리 빈 좌석을 골라둔다 (매크로는 고민하지 않는다)
    code, taken = call(a.base, "/seats-overview", hx())
    taken = taken if code == 200 and isinstance(taken, dict) else {}
    free = [s for s in all_seats() if taken.get(s) in (None, "available")]
    n = max(a.seats, a.attempts)
    seats = [a.seat] if a.seat else random.sample(free, min(n, len(free)))

    if a.at:
        wait_until(datetime.combine(datetime.now().date(),
                                    datetime.strptime(a.at, "%H:%M:%S.%f").time()).timestamp())
    t0 = time.time()
    sent = []
    for i, seat in enumerate(seats):
        if i and a.attempts > 1:
            time.sleep(random.uniform(*a.sleep))
        code, data = call(a.base, "/seat-request", hx(), {"seat_id": seat})
        sent.append({"seat": seat, "status": code,
                     "request_id": data.get("request_id") if code == 200 and isinstance(data, dict) else None,
                     "at_ms": int(time.time() * 1000)})
    for s in sent:
        if s["request_id"]:
            s["won"] = result(a.base, hx(), s["seat"], s["request_id"])
            if s["won"] and not a.no_pay:
                s["paid"] = call(a.base, "/payment", hx(), {"seat_id": s["seat"], "request_id": s["request_id"]})[0]
    first = sent[0] if sent else {}
    print(json.dumps({"bot": a.label or ("disguised" if a.disguise else "simple"), "account": a.email,
                      "account_id": account_id, "device_fp": fp,
                      "seat": first.get("seat"), "fired_at": datetime.fromtimestamp(t0).isoformat(timespec="milliseconds"),
                      "status": first.get("status"), "reply": first.get("request_id"), "requests": sent},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
