#!/usr/bin/env python3
"""
혼합 부하 러너 [LOADMIX] — DOON 공정 티켓팅 부하/방어 시연

목표
  - 정상 사용자(사람)는 좌석을 실제로 따내고 결제까지 해서 좌석맵이 "차오르는" 것을 보여준다.
  - 매크로 60% 를 난이도 3단계로 섞어, 각 단계가 1·2·3차 방어에서 잡히는 것을 대시보드로 보여준다.

난이도 → 잡히는 곳 (코드가 세는 reason 기준)
  매크로 1차 (가장 단순): urllib, Accept-Language 없음 → 1차 PoP  abnormal_headers
  매크로 2차 (중간)      : 브라우저 헤더 순서로 1차 통과, 신규계정 여러 개가 한 기기 공유
                          → 2차 중앙  device_accounts  (전국 합산)
  매크로 3차 (지능형)    : 1·2차 통과하지만 "이미 블랙리스트에 오른" 봇 계정/기기
                          → 3차 AI 명단  blacklist:account|device   (PoP 에서 enforce)
  정상(사람)            : 실제 기기 헤더·고유 IP·고유 계정·사람 타이밍 → 전 계층 통과, 결제

구조
  - 사람: 액터 풀(스레드). 각 액터가 fill 창 동안 빈 좌석을 잡고→결과 보고→이기면 결제, 반복.
          시작 시각을 fill 초에 걸쳐 흩어 좌석이 서서히 차오르게 한다.
  - 매크로: 단발 발사(요청 1건, 결제·폴링 없음). 차단되는 것이 목적이라 가볍다.

사용 (각 PoP 자기 로컬에서)
  python3 run.py provision --base http://localhost:13198 --humans 600 --farm-groups 40
  python3 run.py run --base http://localhost:13198 --at 14:30:00 \
      --fill-seconds 45 --human-actors 400 --macro-per-tier 4000

표준 라이브러리만 사용.
"""
import argparse
import gzip
import http.client
import json
import os
import random
import sys
import threading
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from urllib.parse import urlsplit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from seats import all_seats  # noqa: E402

PW = "doon1234"
SHARED_BOT_FP = "a1b2c3d4e5f60789"   # seed_accounts.sh 가 bot01..30 을 로그인시킨 기기(블랙리스트 대상)

# 실제 브라우저 헤더 순서(sim/humans.py 실측). urllib 기본 순서와 달라 AI 헤더규칙(hdr-*)에 안 걸린다.
CHROME_ORDER = ["authorization", "accept-language", "user-agent", "content-type",
                "accept", "origin", "referer", "accept-encoding"]
CHROME_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36")

# ---------------------------------------------------------------------------
# HTTP: 브라우저처럼 정해진 순서로 헤더를 보낸다(http.client 가 끼워 넣는 것은 끈다).
# ---------------------------------------------------------------------------
def browser_call(base, method, path, token=None, body=None, xff=None):
    u = urlsplit(base)
    c = http.client.HTTPConnection(u.hostname, u.port, timeout=10)
    data = json.dumps(body).encode() if body is not None else None
    c.putrequest(method, path, skip_host=True, skip_accept_encoding=True)
    c.putheader("Host", u.netloc)
    if data is not None:
        c.putheader("Content-Length", str(len(data)))
    vals = {
        "authorization": ("Bearer " + token) if token else None,
        "accept-language": "ko-KR,ko;q=0.9",
        "user-agent": CHROME_UA,
        "content-type": "application/json",
        "accept": "*/*",
        "origin": base,
        "referer": base + "/seat.html",
        "accept-encoding": "gzip, deflate",
    }
    for k in CHROME_ORDER:
        if vals[k] is None or (data is None and k in ("content-type", "origin")):
            continue
        c.putheader(k, vals[k])
    if xff:
        c.putheader("X-Forwarded-For", xff)
    c.endheaders(data)
    r = c.getresponse()
    raw = r.read()
    if r.getheader("Content-Encoding") == "gzip":
        raw = gzip.decompress(raw)
    c.close()
    try:
        return r.status, json.loads(raw or b"null")
    except ValueError:
        return r.status, None


def simple_call(base, path, token=None, body=None, xff=None, ua="Python-urllib/3"):
    """매크로 1차: urllib 그대로. Accept-Language 없음 → 1차 abnormal_headers."""
    h = {"User-Agent": ua, "Content-Type": "application/json"}
    if token:
        h["Authorization"] = "Bearer " + token
    if xff:
        h["X-Forwarded-For"] = xff
    req = urllib.request.Request(base + path, headers=h,
                                 data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read() or b"null")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read() or b"null")
        except Exception:
            return e.code, None
    except Exception:
        return 0, None


def rand_ip(seed=None):
    r = random.Random(seed) if seed else random
    return f"{r.choice([211,175,39,106,121,112,59,220])}.{r.randrange(256)}.{r.randrange(256)}.{r.randrange(1,255)}"


def login(base, email, fp, xff=None):
    st, d = browser_call(base, "POST", "/auth/login",
                         body={"email": email, "password": PW, "device_fp": fp}, xff=xff)
    if st == 200 and isinstance(d, dict):
        return d.get("token")
    return None


def signup(central_base, email):
    st, _ = simple_call(central_base, "/auth/signup", body={"email": email, "password": PW})
    return st


# ---------------------------------------------------------------------------
# provision: 사람 계정 + 2차 농장 계정 생성 (시험용, 끝나고 삭제)
#   사람       : h00001.. (각자 고유 기기 → device_accounts 신호 안 뜸, 나이 무관)
#   2차 농장   : f{그룹}_{번호}.. (그룹당 5개가 한 기기 공유, 신규=나이0 → device_accounts 발동)
#   3차        : 기존 bot01..30 (블랙리스트). provision 불필요.
#   1차        : 익명. provision 불필요.
# ---------------------------------------------------------------------------
def provision(a):
    central = a.base  # /auth 는 nginx 가 중앙으로 보냄
    print(f"[provision] 사람 {a.humans}명 생성…", flush=True)
    ok = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futs = [ex.submit(signup, central, f"h{i:05d}@doon.kr") for i in range(1, a.humans + 1)]
        for f in futs:
            if f.result() in (200, 409):  # 409 = 이미 있음
                ok += 1
    print(f"[provision] 사람 계정 준비 {ok}/{a.humans}", flush=True)

    total_farm = a.farm_groups * a.farm_per_group
    print(f"[provision] 2차 농장 계정 {total_farm}개({a.farm_groups}기기×{a.farm_per_group}) 생성…", flush=True)
    ok = 0
    with ThreadPoolExecutor(max_workers=40) as ex:
        futs = []
        for g in range(a.farm_groups):
            for k in range(a.farm_per_group):
                futs.append(ex.submit(signup, central, f"f{g:03d}_{k}@doon.kr"))
        for f in futs:
            if f.result() in (200, 409):
                ok += 1
    print(f"[provision] 농장 계정 준비 {ok}/{total_farm}", flush=True)
    print("[provision] 완료. (3차는 기존 bot01..30 블랙리스트 사용, 1차는 익명)", flush=True)


# ---------------------------------------------------------------------------
# run: 혼합 발사
# ---------------------------------------------------------------------------
STATS = {}
_lock = threading.Lock()


def bump(key):
    with _lock:
        STATS[key] = STATS.get(key, 0) + 1


def wait_until(epoch):
    while (left := epoch - time.time()) > 0.05:
        time.sleep(min(left - 0.02, 0.5))
    while time.time() < epoch:
        pass


def human_actor(a, idx, start_at, stop_at, seats):
    """사람 한 명: 창 동안 빈 좌석을 잡고 이기면 결제, 반복."""
    base = a.base
    gidx = a.human_offset + idx            # 두 PoP 가 서로 다른 사람 계정을 쓰게(오프셋)
    email = f"h{gidx:05d}@doon.kr"
    fp = f"hdev{gidx:05d}"
    ip = rand_ip(f"h{gidx}")
    token = login(base, email, fp, xff=ip)
    if not token:
        bump("human_login_fail")
        return
    wait_until(start_at)
    rng = random.Random(f"h{idx}")
    while time.time() < stop_at:
        seat = rng.choice(seats)
        st, d = browser_call(base, "POST", "/seat-request", token=token,
                             body={"seat_id": seat}, xff=ip)
        if st != 200 or not isinstance(d, dict):
            bump(f"human_req_{st}")
            time.sleep(rng.uniform(0.3, 1.0))
            continue
        bump("human_req_ok")
        rid = d.get("request_id")
        # 결과를 화면처럼 0.5초마다 확인(최대 4초)
        won = None
        end = time.time() + 4
        while time.time() < end:
            time.sleep(0.5)
            s2, d2 = browser_call(base, "GET", f"/seat-status/{seat}", token=token, xff=ip)
            if s2 == 200 and isinstance(d2, dict) and d2.get("status") == "confirmed":
                won = (d2.get("winner_request_id") == rid)
                break
        if won:
            bump("human_win")
            if a.human_pays:
                ps, _ = browser_call(base, "POST", "/payment", token=token,
                                    body={"seat_id": seat, "request_id": rid}, xff=ip)
                bump("human_pay" if ps == 200 else f"human_pay_{ps}")
        else:
            bump("human_lose_or_unknown")
        time.sleep(rng.uniform(0.1, 0.4))  # 다음 시도까지 잠깐


def macro1(a, idx, fire_at, seats):
    """1차: urllib, Accept-Language 없음 → abnormal_headers."""
    wait_until(fire_at)
    seat = random.choice(seats)
    st, d = simple_call(a.base, "/seat-request", body={"seat_id": seat}, xff=rand_ip(),
                        ua="Python-urllib/3.11")
    reason = (d or {}).get("reason") if isinstance(d, dict) else None
    bump(f"m1_{st}_{reason}")


def macro2(a, idx, fire_at, seats, token, ip):
    """2차: 브라우저 헤더로 1차 통과, 신규계정 여러 개가 한 기기 공유 → device_accounts(중앙)."""
    wait_until(fire_at)
    seat = random.choice(seats)
    st, d = browser_call(a.base, "POST", "/seat-request", token=token,
                        body={"seat_id": seat}, xff=ip)
    # 2차 차단은 응답으로는 안 보인다(후보에서만 빠짐, 200 받음). 대시보드 central_l2 로 측정.
    bump(f"m2_{st}")


def macro3(a, idx, fire_at, seats, token):
    """3차: 블랙리스트 봇 계정(공유 기기) → PoP 에서 blacklist:* (3차)."""
    wait_until(fire_at)
    seat = random.choice(seats)
    st, d = browser_call(a.base, "POST", "/seat-request", token=token,
                        body={"seat_id": seat}, xff=rand_ip())
    reason = (d or {}).get("reason") if isinstance(d, dict) else None
    bump(f"m3_{st}_{reason}")


def run(a):
    base = a.base
    seats = all_seats()
    # 발사 기준 시각
    if a.at:
        hh, mm, ss = (a.at.split(":") + ["0", "0"])[:3]
        now = datetime.now()
        target = now.replace(hour=int(hh), minute=int(mm), second=int(ss), microsecond=0)
        if target < now:
            target += timedelta(days=1)
        t0 = target.timestamp()
    else:
        t0 = time.time() + 3
    fill = a.fill_seconds
    print(f"[run] base={base}  발사 {datetime.fromtimestamp(t0):%H:%M:%S}  "
          f"fill={fill}s  사람액터={a.human_actors}  매크로 1/2/3차={a.m1}/{a.m2}/{a.m3}", flush=True)

    # --- 2차 농장 토큰 미리 확보(한 기기당 여러 계정 로그인 → Redis device_accounts 적재) ---
    print("[run] 2차 농장 로그인(기기당 계정 적재)…", flush=True)
    farm_tokens = []
    def farm_login(g, k):
        email = f"f{g:03d}_{k}@doon.kr"
        fp = f"fdev{g:03d}"            # 그룹 전체가 한 기기 공유 → device_accounts
        tok = login(base, email, fp, xff=rand_ip(f"f{g}"))
        if tok:
            farm_tokens.append((tok, rand_ip(f"f{g}")))
    grange = range(a.farm_offset, a.farm_offset + a.farm_groups)   # PoP 별 분리
    with ThreadPoolExecutor(max_workers=40) as ex:
        list(ex.map(lambda gk: farm_login(*gk),
                    [(g, k) for g in grange for k in range(a.farm_per_group)]))
    print(f"[run] 농장 토큰 {len(farm_tokens)}개", flush=True)

    # --- 3차 블랙리스트 봇 토큰 ---
    print("[run] 3차 봇 로그인(블랙리스트)…", flush=True)
    bot_tokens = []
    for i in range(1, 31):
        tok = login(base, f"bot{i:02d}@doon.kr", SHARED_BOT_FP, xff=rand_ip(f"bot{i}"))
        if tok:
            bot_tokens.append(tok)
    print(f"[run] 봇 토큰 {len(bot_tokens)}개", flush=True)

    # --- 발사 스케줄 ---
    workers = ThreadPoolExecutor(max_workers=a.human_actors + 220)
    futs = []
    # 사람: 시작 시각을 fill 창에 흩뿌림
    for i in range(1, a.human_actors + 1):
        start_at = t0 + (fill * (i - 1) / max(1, a.human_actors)) * 0.4  # 앞 40% 구간에 걸쳐 투입
        futs.append(workers.submit(human_actor, a, i, start_at, t0 + fill, seats))
    # 매크로: fill 창 전체에 단발 흩뿌림
    def sched(n):
        return [t0 + fill * j / max(1, n) for j in range(n)]
    for j, ft in enumerate(sched(a.m1)):
        futs.append(workers.submit(macro1, a, j, ft, seats))
    if farm_tokens:
        for j, ft in enumerate(sched(a.m2)):
            tok, ip = farm_tokens[j % len(farm_tokens)]
            futs.append(workers.submit(macro2, a, j, ft, seats, tok, ip))
    if bot_tokens:
        for j, ft in enumerate(sched(a.m3)):
            futs.append(workers.submit(macro3, a, j, ft, seats, bot_tokens[j % len(bot_tokens)]))

    print(f"[run] 예약 {len(futs)}개. 발사 대기…", flush=True)
    for f in futs:
        try:
            f.result()
        except Exception as e:
            bump(f"err_{type(e).__name__}")
    workers.shutdown()
    print("\n[run] 완료. 집계:", flush=True)
    for k in sorted(STATS):
        print(f"   {k:30s} {STATS[k]}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("provision")
    p.add_argument("--base", required=True)
    p.add_argument("--humans", type=int, default=600)
    p.add_argument("--farm-groups", type=int, default=40)
    p.add_argument("--farm-per-group", type=int, default=5)

    r = sub.add_parser("run")
    r.add_argument("--base", required=True)
    r.add_argument("--at", default="")
    r.add_argument("--fill-seconds", type=float, default=45)
    r.add_argument("--human-actors", type=int, default=400)
    # 방어 깔때기: 1차에서 제일 많이, 2차 그다음, 3차 최소  (대시보드 1>2>3단계)
    r.add_argument("--m1", type=int, default=7000, help="매크로 1차(단순) — 1단계 PoP 에서 차단")
    r.add_argument("--m2", type=int, default=3500, help="매크로 2차(중간) — 2단계 중앙에서 차단")
    r.add_argument("--m3", type=int, default=1500, help="매크로 3차(지능형) — 3단계 AI 명단에서 차단")
    r.add_argument("--farm-groups", type=int, default=40)
    r.add_argument("--farm-per-group", type=int, default=5)
    r.add_argument("--human-offset", type=int, default=0, help="사람 계정 시작 오프셋(대전은 판교와 겹치지 않게)")
    r.add_argument("--farm-offset", type=int, default=0, help="농장 그룹 시작 오프셋(PoP 별 분리)")
    r.add_argument("--human-pays", action="store_true", default=True)
    r.add_argument("--no-human-pays", dest="human_pays", action="store_false")

    a = ap.parse_args()
    if a.cmd == "provision":
        provision(a)
    else:
        run(a)


if __name__ == "__main__":
    main()
