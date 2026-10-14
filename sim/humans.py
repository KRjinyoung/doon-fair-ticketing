#!/usr/bin/env python3
"""
사람 시뮬레이터 — 가상의 사람 N명이 한 회차 오픈에 맞춰 좌석을 누른다  [SIMHUMAN_V1]
(격리 실험판 전용. bots/rounds.py --humans N 이 회차마다 부른다. 표준 라이브러리만)

  python3 sim/humans.py --open-at 1760500000 --n 30 \
      --bases http://172.17.0.1:18198,http://172.17.0.1:18199

[무엇을 흉내 내나]  — 근거와 값은 아래 P 한 곳에 모았다 (민감도 분석 때 이 값만 바꾼다)
  사람마다 고정    : 기기·브라우저(헤더 순서와 값), 접속 PoP, IP, 기기 시계 오차, 성향(미리 연타형/반응형)
  회차마다 무작위  : 참여 여부, 반응 시간, 좌석 고르는 시간, 실패 후 재시도
  헤더 모양은 실제 기기에서 찍힌 detection-log(아이폰, 파이어폭스)와 진짜 크롬(HTTP)에서 그대로 따왔다.
  봇과 '헤더만으로는' 구별이 안 되게 하는 것이 목적이다. 남는 차이는 타이밍과 계정이다.

[사람 행동 모델]
  - 화면 카운트다운은 '기기 시계' 기준이다. 기기 시계는 실제보다 clock_sd 만큼 틀린다.
  - 반응형  : 화면에서 열린 걸 보고 누른다. 반응 시간 = 로그정규(rt_median, rt_sigma).
              (Woods 2015: 단순 시각 반응 ~0.25s. 여기에 버튼까지 손이 가는 시간을 더함)
  - 미리 연타형 : 카운트다운을 보고 0초 직전부터 버튼을 연타한다 (Repp 2005: 예측 가능한 신호엔
              미리 맞춰 친다). 잠긴 동안의 클릭은 무시되고, 풀린 뒤 첫 클릭만 나간다.
  - 좌석을 미리 골라 둔 사람(preselect)이 아니면 고르는 시간이 더 든다.
  - 지면 다른 좌석을 골라 다시 누른다 (max_attempts 번까지, 오픈 30초 안에서만).
  - 좌석은 무대 가까운 1층·가운데·앞줄을 더 좋아한다 (봇은 남은 좌석에서 무작위로 고른다).

출력: 마지막 줄에 JSON 한 줄 {"humans": [{"account_id", "email", "pop", "profile", "sent": [...]}]}
"""
import argparse
import gzip
import hashlib
import http.client
import json
import math
import os
import random
import sys
import threading
import time
from urllib.parse import urlsplit

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from doonload import all_seats  # noqa: E402

P = dict(
    participate=0.85,           # 회차마다 참여할 확률
    clock_sd=0.12,              # 기기 시계 오차 표준편차(s). 폰은 NTP 로 대개 ±100ms 안팎
    anticipate=0.25,            # 미리 연타형 비율
    mash_gap=(0.12, 0.25),      # 연타 간격(s)
    rt_median=0.45, rt_sigma=0.35,          # 반응형 반응 시간(s)
    preselect=0.7,              # 오픈 전에 좌석을 골라 둔 비율
    choose_median=1.5, choose_sigma=0.5,    # 오픈 후 좌석 고르는 시간(s)
    retry_median=1.8, retry_sigma=0.5,      # 졌다는 걸 보고 다른 좌석을 다시 고르는 시간(s)
    max_attempts=3,
    open_sec=30,                # seat.html?every= 가 열려 있는 시간
    lat_mobile=(0.015, 0.045), lat_desktop=(0.003, 0.015),  # 한쪽 방향 네트워크 지연(s)
)

# 기기 프로필: (비율, 모바일?, 헤더 순서, 값). 순서는 실측 그대로다.
#   크롬 계열 순서/값: 진짜 크롬이 HTTP(보안 출처 아님)로 보낸 seat-request
#   사파리 순서/값   : 실제 아이폰 기기 (2026-10-15 대전 PoP)
#   파이어폭스        : 실제 리눅스 파이어폭스 (2026-10-14 판교 PoP)
CHROME = ["authorization", "accept-language", "user-agent", "content-type", "accept", "origin", "referer", "accept-encoding"]
SAFARI = ["accept", "origin", "content-type", "user-agent", "authorization", "referer", "accept-language", "priority", "accept-encoding"]
FIREFOX = ["user-agent", "accept", "accept-language", "accept-encoding", "referer", "content-type", "authorization", "origin"]
CHROME_AL = ["ko-KR,ko;q=0.9", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"]
PROFILES = {
    "android-chrome": (0.30, True, CHROME, "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Mobile Safari/537.36"),
    "samsung-internet": (0.10, True, CHROME, "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/29.0 Chrome/136.0.0.0 Mobile Safari/537.36"),
    "iphone-safari": (0.30, True, SAFARI, "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1"),
    "ipad-safari": (0.05, True, SAFARI, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15"),
    "windows-chrome": (0.15, False, CHROME, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36"),
    "mac-chrome": (0.05, False, CHROME, "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36"),
    "windows-firefox": (0.05, False, FIREFOX, "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0"),
}
# 가정·모바일 IP 처럼 보이는 대역 (흉내일 뿐, 학습 특징으로는 쓰지 않는다)
IP_PREFIX = ["211.36", "175.223", "39.7", "106.101", "121.134", "112.169", "59.10", "220.85"]


def lognorm(rng, median, sigma):
    return median * math.exp(rng.gauss(0, sigma))


class Human:
    """사람 한 명. 고정 성향은 번호로 정해져 회차가 바뀌어도 같다."""

    def __init__(self, i, bases):
        r = random.Random(f"human-{i}")
        self.email = f"user{i:02d}@doon.kr"
        self.profile = r.choices(list(PROFILES), [p[0] for p in PROFILES.values()])[0]
        _, self.mobile, self.order, ua = PROFILES[self.profile]
        self.ua = ua.format(v=r.choices([155, 154, 153], [0.7, 0.25, 0.05])[0])  # 크롬은 자동 업데이트라 최신이 대부분
        self.al = {"windows-firefox": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3"}.get(
            self.profile, "ko-KR,ko;q=0.9" if self.order is SAFARI else r.choice(CHROME_AL))
        self.base = bases[i % len(bases)]  # PoP 는 번갈아 — 지역별 오탐 비교가 같은 인원으로 되게
        self.ip = f"{r.choice(IP_PREFIX)}.{r.randrange(256)}.{r.randrange(1, 255)}"
        self.fp = hashlib.sha256(f"human-device-{i}".encode()).hexdigest()[:16]
        self.skew = r.gauss(0, P["clock_sd"])
        self.anticipator = r.random() < P["anticipate"]
        self.token = self.account_id = None
        self.sent = []

    # --- HTTP: 헤더를 프로필 순서 그대로 보낸다 (http.client 가 끼워 넣는 것은 끈다) ---
    def call(self, method, path, body=None, page=True):
        u = urlsplit(self.base)
        c = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
        data = json.dumps(body).encode() if body is not None else None
        c.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
        c.putheader("Host", u.netloc)
        if data is not None:
            c.putheader("Content-Length", str(len(data)))
        vals = {"user-agent": self.ua, "accept": "*/*", "accept-language": self.al,
                "accept-encoding": "gzip, deflate", "referer": f"{self.base}/seat.html?every=1",
                "origin": self.base, "priority": "u=3, i", "content-type": "application/json",
                "authorization": f"Bearer {self.token}" if self.token else None}
        for k in self.order:
            if vals[k] is None or (data is None and k in ("content-type", "origin")):
                continue
            c.putheader(k, vals[k])
        c.putheader("X-Forwarded-For", self.ip)  # 실험판 nginx 가 이 값을 접속 IP 로 쓴다
        c.endheaders(data)
        resp = c.getresponse()
        body = resp.read()
        if resp.getheader("Content-Encoding") == "gzip":  # 브라우저처럼 gzip 을 받겠다고 했으므로
            body = gzip.decompress(body)
        out = resp.status, body
        c.close()
        try:
            return out[0], json.loads(out[1] or b"null")
        except ValueError:
            return out[0], None

    def login(self):
        code, d = self.call("POST", "/auth/login", {"email": self.email, "password": "doon1234", "device_fp": self.fp})
        if code == 200:
            self.token, self.account_id = d["token"], d.get("account_id")

    def pick_seat(self, rng):
        code, taken = self.call("GET", "/seats-overview")
        free = [s for s in all_seats() if (taken or {}).get(s) in (None, "available")]
        if not free:
            return None

        def w(s):  # 1층(A~E) > 2층, 가운데 구역 C/H, 앞줄일수록 인기
            z, row, _ = s.split("-")
            return (2.0 if z in "ABCDE" else 1.0) * (1.5 if z in "CH" else 1.0) / int(row) ** 0.7
        return rng.choices(free, [w(s) for s in free])[0]

    def play(self, open_at):
        """한 회차. open_at 은 실제(정확한) 시각. 화면은 기기 시계(open_at + skew)로 열린다."""
        rng = random.Random(f"{self.email}-{open_at}")
        if rng.random() > P["participate"]:
            return
        seat = self.pick_seat(rng) if rng.random() < P["preselect"] else None
        screen_open = open_at + self.skew + rng.uniform(0, 0.05)  # 화면 잠금은 50ms 마다 확인
        if self.anticipator and seat:
            t = screen_open + rng.uniform(-0.3, 0.05)
            while t < screen_open:                     # 잠긴 동안 누른 건 무시된다
                t += rng.uniform(*P["mash_gap"])
        else:
            t = screen_open + lognorm(rng, P["rt_median"], P["rt_sigma"])
            if not seat:
                t += lognorm(rng, P["choose_median"], P["choose_sigma"])
        close = open_at + self.skew + P["open_sec"]
        for _ in range(P["max_attempts"]):
            lat = rng.uniform(*(P["lat_mobile"] if self.mobile else P["lat_desktop"]))
            time.sleep(max(0, t + lat - time.time()))
            if time.time() >= close:
                return
            seat = seat or self.pick_seat(rng)
            if not seat:
                return
            code, d = self.call("POST", "/seat-request", {"seat_id": seat})
            rid = (d or {}).get("request_id")
            self.sent.append({"seat": seat, "at_ms": int(time.time() * 1000), "status": code})
            won = self.wait_result(seat, rid)
            if won is not False:   # 이겼거나(True) 결과를 모르면(None) 그만
                return
            t = time.time() + lognorm(rng, P["retry_median"], P["retry_sigma"])
            seat = None

    def wait_result(self, seat, rid, limit=8):
        """화면처럼 0.5초마다 seat-status 를 본다. True=획득, False=짐, None=모름."""
        end = time.time() + limit
        while time.time() < end:
            time.sleep(0.5)
            code, d = self.call("GET", f"/seat-status/{seat}")
            if code == 200 and d and d.get("status") == "confirmed":
                return d.get("winner_request_id") == rid
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--open-at", type=float, required=True, help="오픈 시각 (epoch 초)")
    ap.add_argument("--n", type=int, default=30, help="사람 수 (user01.. 계정을 쓴다)")
    ap.add_argument("--bases", required=True, help="PoP 주소들, 쉼표로 구분")
    a = ap.parse_args()

    people = [Human(i, a.bases.split(",")) for i in range(1, a.n + 1)]
    for h in people:
        h.login()
    ths = [threading.Thread(target=h.play, args=(a.open_at,)) for h in people if h.token]
    for t in ths:
        t.start()
    for t in ths:
        t.join()
    print(json.dumps({"humans": [{"account_id": h.account_id, "email": h.email, "pop": h.base,
                                  "profile": h.profile, "anticipator": h.anticipator,
                                  "skew_ms": round(h.skew * 1000), "sent": h.sent} for h in people]},
                     ensure_ascii=False))


if __name__ == "__main__":
    main()
