#!/usr/bin/env python3
"""
DOON 부하 생성기  [FAIRLOAD_V1][SCATTER_V1] - 표준 라이브러리만 사용

  python3 doonload.py --total 10000 --rate 400 --conn 100 --at 22:35:00

왜 curl 을 안 쓰는가
  curl 은 요청마다 프로세스와 TCP 연결을 새로 만든다. 프로세스 생성만 5~10ms 라
  2만 건이면 그것만으로 몇 분이 걸리고, 측정값이 "서버 성능" 이 아니라
  "curl 띄우는 속도" 가 되어버린다. 게다가 연결을 매번 새로 만들면 TIME_WAIT 가
  쌓여 임시 포트가 마른다.
  여기서는 연결 N 개를 열어두고 계속 재사용한다.

왜 좌석을 구간별로 공략하는가
  608 석뿐인데 초당 수백 건을 무작위로 쏘면 몇 초 만에 전부 팔린다.
  화면에는 좌석이 차오르는 장면이 안 남는다.
  구간(기본 24석)을 순서대로 공략하면 왼쪽부터 차오르는 것이 보이고,
  구간 안에서는 요청이 집중되어 경쟁 강도도 유지된다.
  양 PoP 가 같은 순서를 쓰므로 두 지역이 같은 좌석을 두고 동시에 붙는다.
"""

import argparse
import asyncio
import json
import random
import statistics
import sys
import time
from datetime import datetime

# ---------------------------------------------------------------------------
# 좌석 목록  A-1-1 ~ J-8-10  (608석)
# ---------------------------------------------------------------------------
ZONE_COLS = {"A": 6, "B": 8, "C": 10, "D": 8, "E": 6,
             "F": 6, "G": 8, "H": 10, "I": 8, "J": 6}
ROWS = 8


def all_seats():
    out = []
    for z, cols in ZONE_COLS.items():
        for r in range(1, ROWS + 1):
            for c in range(1, cols + 1):
                out.append(f"{z}-{r}-{c}")
    return out


# ---------------------------------------------------------------------------
# 아주 작은 HTTP/1.1 클라이언트 (연결 재사용)
# ---------------------------------------------------------------------------
class Conn:
    """열어둔 TCP 연결 하나로 요청을 계속 보낸다."""

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.r = self.w = None
        self.reused = 0
        self.opened = 0

    async def connect(self):
        self.r, self.w = await asyncio.open_connection(self.host, self.port)
        self.opened += 1

    async def close(self):
        if self.w:
            try:
                self.w.close()
                await self.w.wait_closed()
            except Exception:
                pass
        self.r = self.w = None

    async def post(self, path, body: bytes, headers: dict, timeout=10.0):
        if self.w is None:
            await self.connect()
        else:
            self.reused += 1

        head = [f"POST {path} HTTP/1.1",
                f"Host: {self.host}:{self.port}",
                "Content-Type: application/json",
                f"Content-Length: {len(body)}",
                "Connection: keep-alive"]
        for k, v in headers.items():
            head.append(f"{k}: {v}")
        raw = ("\r\n".join(head) + "\r\n\r\n").encode() + body

        self.w.write(raw)
        await self.w.drain()
        return await asyncio.wait_for(self._read(), timeout)

    async def _read(self):
        # 상태줄
        line = await self.r.readline()
        if not line:
            raise ConnectionError("연결이 끊김")
        try:
            status = int(line.split(b" ")[1])
        except Exception:
            raise ConnectionError(f"응답 형식 오류: {line[:60]!r}")

        length, chunked, close = None, False, False
        while True:
            h = await self.r.readline()
            if h in (b"\r\n", b"\n", b""):
                break
            low = h.lower()
            if low.startswith(b"content-length:"):
                length = int(low.split(b":")[1])
            elif low.startswith(b"transfer-encoding:") and b"chunked" in low:
                chunked = True
            elif low.startswith(b"connection:") and b"close" in low:
                close = True

        if chunked:
            body = b""
            while True:
                sz = await self.r.readline()
                n = int(sz.strip().split(b";")[0], 16)
                if n == 0:
                    await self.r.readline()
                    break
                body += await self.r.readexactly(n)
                await self.r.readline()
        elif length is not None:
            body = await self.r.readexactly(length) if length else b""
        else:
            body = b""
            close = True

        if close:
            await self.close()
        return status, body


# ---------------------------------------------------------------------------
# 로그인 - 기존 계정 50개로 토큰 확보
# ---------------------------------------------------------------------------
# 2만 개 계정을 새로 만들 필요가 없다. bcrypt 가 계정당 100ms 라 2만 개면 33분이다.
# 이미 있는 50개(user01~20 정상, bot01~30 매크로)를 재사용한다.
# bot 계정 30개는 모두 같은 device_fp 로 로그인되어 있어, 부하 중에도
# "한 기기에 계정 30개" 패턴이 데이터로 남는다.
async def get_tokens(host, port, pw, verbose=True):
    accounts = [f"user{i:02d}@doon.kr" for i in range(1, 21)] + \
               [f"bot{i:02d}@doon.kr" for i in range(1, 31)]
    tokens = []
    c = Conn(host, port)
    for em in accounts:
        fp = ("a1b2c3d4e5f60789" if em.startswith("bot")
              else "u%015d" % int(em[4:6]))
        body = json.dumps({"email": em, "password": pw, "device_fp": fp}).encode()
        try:
            st, resp = await c.post("/auth/login", body, {})
            if st == 200:
                tokens.append(json.loads(resp)["token"])
        except Exception:
            await c.close()
    await c.close()
    if verbose:
        print(f"  토큰 {len(tokens)}개 확보 (정상 20 / 매크로 30 중)")
    return tokens


# ---------------------------------------------------------------------------
# 부하
# ---------------------------------------------------------------------------
class Stats:
    def __init__(self):
        self.lat = []
        self.codes = {}
        self.errs = {}

    def add(self, code, sec):
        self.lat.append(sec)
        self.codes[code] = self.codes.get(code, 0) + 1

    def err(self, name):
        self.errs[name] = self.errs.get(name, 0) + 1


async def worker(conn, queue, tokens, stats, path):
    while True:
        seat = await queue.get()
        if seat is None:
            queue.task_done()
            return
        body = json.dumps({"seat_id": seat}).encode()
        hdr = {}
        if tokens:
            hdr["Authorization"] = "Bearer " + random.choice(tokens)
        t0 = time.perf_counter()
        try:
            st, _ = await conn.post(path, body, hdr)
            stats.add(st, time.perf_counter() - t0)
        except Exception as e:
            stats.err(type(e).__name__)
            await conn.close()
        finally:
            queue.task_done()


async def feeder(queue, seats, total, rate, jitter, wall_start, front=60):
    """
    무작위 흩뿌리기 + 절대 시각 기준 속도 제한.  [SCATTER_V1]

    *** 좌석 배열을 미리 섞는 이유 ***
    좌석을 앞에서부터 순서대로 공략하면 화면에 블록이 계단처럼 채워져 부자연스럽다.
    배열을 섞어두고 그 위에서 진행하면, 화면상으로는 좌석이 여기저기서 무작위로
    나타나면서도 25초에 걸쳐 고르게 채워진다.

    *** 섞는 시드를 고정하는 이유 ***
    두 PoP 가 같은 순서로 섞어야 같은 좌석을 동시에 노려 경쟁이 성립한다.
    시드가 다르면 서로 다른 좌석을 공략해서 경쟁 자체가 안 생긴다.
    반면 풀 안에서 어느 좌석을 고를지는 서로 다른 난수를 쓰므로 승자가 갈린다.

    *** 절대 시각을 쓰는 이유 ***
    각 프로세스의 시작 시점을 기준으로 하면, 한쪽이 조금 늦게 출발했을 때
    그 간격이 끝까지 유지되어 먼저 출발한 쪽이 모든 좌석을 가져간다.
    두 서버 시계가 chrony 로 맞춰져 있으므로 벽시계를 기준으로 삼는다.
    """
    order = list(seats)
    random.Random(20260816).shuffle(order)      # 양쪽 PoP 동일
    n = len(order)
    sent = 0

    while sent < total:
        p = sent / total
        end = max(front, int(n * min(1.0, p * 1.15)))
        start = max(0, end - front)
        await queue.put(order[random.randrange(start, min(end, n))])
        sent += 1
        if rate:
            target = wall_start + sent / rate
            if jitter:
                target += random.uniform(-jitter, jitter)
            delay = target - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
    return sent


async def run(args):
    seats = all_seats()
    print(f"  좌석 {len(seats)}석 / 무작위 흩뿌리기 (동시 {args.front}석)")

    tokens = []
    if not args.no_auth:
        tokens = await get_tokens(args.host, args.port, args.password)

    # 시작 시각 맞추기 (양 PoP 동시 발사)
    wall_start = time.time()
    if args.at:
        h, mi, sec = [int(x) for x in args.at.split(":")]
        now = datetime.now()
        tgt = now.replace(hour=h, minute=mi, second=sec, microsecond=0)
        wall_start = tgt.timestamp()
        wait = wall_start - time.time()
        if wait < 0:
            print(f"  [경고] {args.at} 은 이미 지났습니다. 즉시 시작합니다.")
        else:
            print(f"  {args.at} 까지 {wait:.1f}초 대기...")
            await asyncio.sleep(wait)

    conns = [Conn(args.host, args.port) for _ in range(args.conn)]
    queue = asyncio.Queue(maxsize=args.conn * 4)
    stats = Stats()

    t0 = time.perf_counter()
    tasks = [asyncio.create_task(worker(c, queue, tokens, stats, args.path))
             for c in conns]
    sent = await feeder(queue, seats, args.total, args.rate,
                        args.jitter / 1000.0, wall_start, args.front)
    await queue.join()
    for _ in conns:
        await queue.put(None)
    await asyncio.gather(*tasks, return_exceptions=True)
    elapsed = time.perf_counter() - t0
    for c in conns:
        await c.close()

    report(stats, sent, elapsed, conns)


def pct(v, p):
    if not v:
        return 0.0
    k = sorted(v)
    return k[min(len(k) - 1, int(len(k) * p / 100))]


def report(stats, sent, elapsed, conns):
    ok = sum(n for c, n in stats.codes.items() if 200 <= c < 300)
    fail = sum(stats.codes.values()) - ok + sum(stats.errs.values())
    opened = sum(c.opened for c in conns)
    reused = sum(c.reused for c in conns)

    print()
    print("=" * 52)
    print(f"  총 {sent}건 / 성공 {ok} / 실패 {fail}")
    print(f"  소요      {elapsed:.1f}초")
    print(f"  처리량    {sent / elapsed:,.0f} req/s")
    if stats.lat:
        print(f"  지연      p50 {pct(stats.lat,50)*1000:.0f}ms  "
              f"p95 {pct(stats.lat,95)*1000:.0f}ms  "
              f"p99 {pct(stats.lat,99)*1000:.0f}ms  "
              f"최대 {max(stats.lat)*1000:.0f}ms")
    print(f"  상태코드  " + "  ".join(f"{c}: {n}" for c, n in sorted(stats.codes.items())))
    if stats.errs:
        print(f"  오류      " + "  ".join(f"{k}: {v}" for k, v in stats.errs.items()))
    print(f"  연결      {opened}개 생성 / {reused}건 재사용")
    if opened > 0:
        print(f"            연결당 평균 {sent/opened:.0f}건  "
              f"(1에 가까우면 재사용이 안 되는 것)")
    print("=" * 52)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=13198)
    ap.add_argument("--path", default="/seat-request")
    ap.add_argument("--total", type=int, default=10000, help="총 요청 수")
    ap.add_argument("--rate", type=int, default=400, help="초당 요청 수 (0=제한없음)")
    ap.add_argument("--conn", type=int, default=100, help="재사용할 연결 수")
    ap.add_argument("--wave", type=int, default=24, help="(미사용)")
    ap.add_argument("--front", type=int, default=60,
                    help="동시에 노릴 좌석 수. 작을수록 경쟁이 치열해짐")
    ap.add_argument("--jitter", type=float, default=120.0,
                    help="요청마다 흩뿌릴 시간(ms). 0이면 규칙적")
    ap.add_argument("--at", default="", help="시작 시각 HH:MM:SS")
    ap.add_argument("--password", default="doon1234")
    ap.add_argument("--no-auth", action="store_true", help="토큰 없이 발사")
    args = ap.parse_args()

    print(f"  대상 http://{args.host}:{args.port}{args.path}")
    print(f"  총 {args.total}건 / 초당 {args.rate or '제한없음'} / 연결 {args.conn}개")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        print("\n  중단됨")


if __name__ == "__main__":
    main()
