"""
가장 최소 구성의 중앙 서버(judgment 서비스).

역할:
- 여러 PoP로부터 "같은 좌석에 대한 요청 + 그 PoP가 찍은 도착 시각"을 받는다.
- 좌석별로 수집창(WINDOW_SECONDS)을 열어서 그 시간 동안 들어온 요청들을 모은다.
- 창이 닫히면, 모인 요청 중 타임스탬프가 가장 빠른 것을 승자로 판정한다.
- 판정 결과를 Postgres에 기록한다 (지금 단계에서는 결제 없이, 판정 = 확정으로 취급).
- PoP/클라이언트가 지금 상태(대기 중 / 확정됨)를 조회할 수 있는 API를 제공한다.

*** (중요) PoP -> Central 통신 방식이 HTTP에서 Kafka로 교체됨 (2026-08-14) ***
      PoP는 더 이상 /judge를 HTTP로 직접 호출하지 않는다. 대신 pop_server.py가
      "seat-requests" 토픽에 이벤트를 발행(produce)하고, 이 서버가 그 토픽을
      구독(consume)해서 판정 로직(apply_seat_request)을 실행한다.
      판정 로직 자체(수집창/타임스탬프 비교)는 전혀 바뀌지 않았고, 그 로직을
      "누가 호출하느냐"만 HTTP 요청 핸들러 -> Kafka consumer 루프로 바뀌었다.
      /judge HTTP 엔드포인트는 디버그/수동 테스트용으로 남겨둔다.

*** (중요) 판정 기준 시각이 "원본 + PoP 오프셋"으로 바뀜 (2026-08-14) ***
      예전에는 각 PoP가 찍은 원본 타임스탬프(pop_arrival_epoch_ms)를 그대로 비교했다.
      그건 모든 PoP의 시계가 완벽히 같다는 가정이었고, 실제로는 지역 간 클럭 오차가
      실제 응답 속도 차이보다 크게 판정을 왜곡할 수 있었다.

      지금 구조:
        - 모든 PoP가 "동일한 외부 NTP 서버"를 각자 바라보며 자기 시계 오차를
          5초마다 측정하고, 그 값(clock_offset_ms_measured)을 요청에 실어 보낸다.
        - 이 서버가 판정할 때 (원본 + 오프셋) = corrected_epoch_ms 를 계산해서,
          그 보정된 시각으로 승자를 정한다.
        - 보정은 반드시 여기서만 한다. PoP가 미리 자기 시각을 고쳐서 보내면 받은 값이
          원본인지 보정된 값인지 구분할 수 없어 판정 근거를 추적할 수 없기 때문에,
          PoP는 원본과 오프셋을 따로 보내고 이 서버가 합친다.

*** (중요) DB가 SQLite -> Postgres로 교체됨, 그리고 seat-status 토픽 발행이 추가됨 (2026-08-14) ***
      - confirmed_seats 테이블은 이제 Postgres(asyncpg)에 쓴다.
      - 좌석이 확정될 때(close_window_after_delay) "seat-status" 토픽에 이벤트를 발행하는데,
        이 발행은 반드시 Postgres INSERT가 성공적으로 끝난 "다음"에만 일어난다.
        (DB에는 기록 안 됐는데 이벤트만 나가서, 그 이벤트를 구독하는 PoP들이 "존재하지 않는
        확정"을 신뢰하게 되는 상황을 막기 위함 — 순서: DB 커밋 -> 이벤트 발행.)
      - "pending" 상태(수집창이 열려있는 동안 경쟁자 수 변화)는 DB에 안 남기는 휘발성
        상태라서 DB 커밋과 무관하게 바로 발행한다.
"""

import asyncio
import hashlib
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from prometheus_client import Counter, Gauge, Histogram
from prometheus_fastapi_instrumentator import Instrumentator

import second_layer as _l2  # 2차 판정 (전국 집계)  [MACRO3_V1]

# 테스트하기 편하게 창을 넉넉히 5초로 잡는다.
# 실제 시스템에서는 P99 지연 기반으로 80~250ms 수준까지 줄여야 한다.
WINDOW_SECONDS = 5.0

# Kafka 브로커 주소와 토픽들.
# - seat-requests: PoP -> Central (PoP가 발행, Central이 구독)
# - seat-status:   Central -> 모든 PoP (Central이 발행, 각 PoP가 구독해서 로컬 캐시를 채움)
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC_SEAT_REQUESTS = "seat-requests"
# 판정 컨슈머를 몇 개 병렬로 띄울지.  [PARCONSUME_V1]
# seat-requests 는 key=seat_id 로 파티셔닝되므로 한 좌석은 항상 같은 파티션 ->
# 같은 컨슈머로만 간다. 따라서 seat_windows(프로세스 메모리)가 흩어지지 않고,
# 컨슈머를 파티션 수만큼 늘려도 좌석 상태 일관성이 유지된다.
# 파티션 수보다 크게 잡아도 남는 컨슈머는 idle 이므로 파티션 수에 맞추는 게 정석.
KAFKA_JUDGE_CONSUMERS = int(os.environ.get("KAFKA_JUDGE_CONSUMERS", "3"))
# getmany 한 번에 끌어올 최대 레코드 수. 배치로 받아 파이썬 루프 오버헤드를 줄인다.
KAFKA_JUDGE_BATCH = int(os.environ.get("KAFKA_JUDGE_BATCH", "500"))
KAFKA_TOPIC_SEAT_STATUS = "seat-status"

# *** 주의 (기술 부채): 비밀번호를 코드/환경변수에 평문으로 두는 건 데모/대회용 임시
# 조치다. 실제 운영에서는 secret manager(Vault, K8s Secret 등)로 옮겨야 한다.
# 비밀번호가 코드에 남지 않도록 기본값을 두지 않는다. compose 가 .env 의
# POSTGRES_PASSWORD 로 조립해서 넘겨준다 (없으면 기동 시 KeyError 로 바로 드러난다).
DATABASE_URL = os.environ["DATABASE_URL"]

# ----------------------------------------------------------------------------
# DB 커넥션 안전장치 (2026-08-14)
#
# *** 커넥션 수가 폭주하는 일은 구조적으로 없다 ***
# 풀이 max_size로 하드 캡되어 있어서, 접속자가 아무리 많아도 Postgres 입장에서
# 이 서버는 커넥션 N개짜리 클라이언트 하나다. 요청이 몰리면 커넥션이 늘어나는 게
# 아니라 뒤의 요청이 순서를 기다린다.
#
# 진짜 위험은 그 "기다림"이 무한이라는 것이었다. 쿼리 하나가 걸려서 커넥션을
# 안 놓으면 뒤의 요청이 영원히 대기하며 메모리에 쌓인다. 그래서 두 겹으로 막는다:
#   - 획득 타임아웃: 정해진 시간 안에 커넥션을 못 얻으면 포기하고 에러를 낸다
#   - 실행 타임아웃: 쿼리가 오래 걸리면 강제로 취소해 커넥션을 회수한다
# 둘 다 "느리게 죽는 것"보다 "빨리 실패하는 것"이 낫다는 원칙이다.
# ----------------------------------------------------------------------------
DB_POOL_MAX_SIZE = int(os.environ.get("DB_POOL_MAX_SIZE", "10"))
# *** 인증(로그인) 전용 풀 크기  [CENTRAL_STABILITY_V1] ***
# 좌석 판정과 로그인이 같은 커넥션 풀을 나눠 쓰면, 티켓 오픈 직전 로그인이 몰릴 때
# 판정용 커넥션까지 뺏겨서 좌석 확정이 밀리거나 실패한다. 풀을 물리적으로 나누면
# 로그인이 아무리 폭주해도 "로그인만 느려지고" 판정은 영향을 받지 않는다.
AUTH_POOL_MAX_SIZE = int(os.environ.get("AUTH_POOL_MAX_SIZE", "4"))

# seat-status 메시지와 메모리에 남길 경쟁자 수 상한.  [CAP_V1]
# 이 값이 없으면 좌석 하나에 몰린 경쟁자 전원이 메시지에 실려
# 모든 PoP 로 전파되고, PoP 는 그걸 메모리 캐시에 그대로 보관한다.
# 요청이 많을수록 메시지가 뚱뚱해지고 전파 비용이 커진다.
# 경쟁자 "수" 는 competitors_per_seat 지표가 따로 세므로 정보를 잃지 않는다.
ALL_REQUESTS_KEEP = int(os.environ.get("ALL_REQUESTS_KEEP", "20"))
DB_ACQUIRE_TIMEOUT = float(os.environ.get("DB_ACQUIRE_TIMEOUT", "5"))
DB_COMMAND_TIMEOUT = float(os.environ.get("DB_COMMAND_TIMEOUT", "10"))

# 전체 좌석 수 (seat.html의 ZONES 설정과 반드시 일치해야 함: 8행 x (6+8+10+8+6)x2 = 608석).
# *** 주의 (기술 부채): 지금은 프론트(seat.html ZONES)와 여기 숫자를 사람이 직접 맞춰야 한다.
TOTAL_SEATS = 608

db_pool: asyncpg.Pool | None = None
kafka_producer: AIOKafkaProducer | None = None


@asynccontextmanager
async def db_conn(timeout: float = None):
    """
    DB 커넥션을 빌려오는 공용 통로. 모든 DB 접근은 반드시 이걸 거친다.

    직접 db_pool.acquire()를 쓰지 않는 이유:
      - 타임아웃 없이 acquire하면 풀이 바닥났을 때 무한정 기다린다. 그 사이
        요청이 계속 쌓여서 결국 메모리로 서버가 죽는다. 빨리 실패하는 게 낫다.
      - 커넥션을 얻기까지 걸린 시간을 재두면, 부하가 걸릴 때 "커넥션이 모자란다"는
        신호를 대시보드에서 먼저 볼 수 있다.
    """
    t0 = time.monotonic()
    try:
        conn = await db_pool.acquire(timeout=timeout or DB_ACQUIRE_TIMEOUT)
    except (asyncio.TimeoutError, TimeoutError):
        db_acquire_timeouts.inc()
        raise
    db_acquire_wait.observe(time.monotonic() - t0)
    try:
        yield conn
    finally:
        await db_pool.release(conn)


async def init_db():
    async with db_conn() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS confirmed_seats (
                seat_id TEXT PRIMARY KEY,
                winner_request_id TEXT NOT NULL,
                winner_pop_id TEXT NOT NULL,
                winner_timestamp_ms BIGINT NOT NULL,
                confirmed_at TEXT NOT NULL
            )
            """
        )


# ===========================================================================
# 인증 전용 커넥션 풀  [CENTRAL_STABILITY_V1]
# ---------------------------------------------------------------------------
# db_conn() 은 그대로 판정용 풀(db_pool)을 쓴다. 기존 호출부는 한 곳도 바뀌지 않는다.
# 로그인/가입 코드만 auth_conn() 을 쓰면 두 경로가 서로를 굶기지 않는다.
# ===========================================================================
auth_pool: asyncpg.Pool | None = None

restored_seats_gauge = Gauge(
    "central_restored_seats",
    "서버 시작 시 Postgres에서 메모리로 복원한 확정 좌석 수",
)


@asynccontextmanager
async def auth_conn(timeout: float = None):
    """로그인/가입 전용 커넥션. 고갈되어도 좌석 판정에는 영향이 없다."""
    t0 = time.monotonic()
    try:
        conn = await auth_pool.acquire(timeout=timeout or DB_ACQUIRE_TIMEOUT)
    except (asyncio.TimeoutError, TimeoutError):
        db_acquire_timeouts.inc()
        raise
    db_acquire_wait.observe(time.monotonic() - t0)
    try:
        yield conn
    finally:
        await auth_pool.release(conn)


async def restore_confirmed_seats():
    """
    서버 시작 시 Postgres의 confirmed_seats 를 메모리(seat_windows)로 복원한다.

    *** 이게 없으면 데이터 사고가 난다 ***
    좌석 요청 핸들러는 "이미 확정된 좌석인가"를 seat_windows 로만 판단한다.
    재시작하면 이 dict 가 비어 있으므로, 이미 확정된 좌석에 새 요청이 오면
    새 수집창이 열려 다시 판정되고, DB INSERT ... ON CONFLICT DO UPDATE 때문에
    먼저 예매한 사람의 좌석이 뒤 사람에게 덮어써진다.
    화면이 비어 보이는 표시 문제가 아니라 확정 결과가 뒤집히는 문제다.

    winner 의 키 이름은 판정 로직이 쓰는 것과 같아야 한다:
      corrected_epoch_ms  <- DB 컬럼 winner_timestamp_ms
    """
    try:
        async with db_conn() as conn:
            rows = await conn.fetch(
                "SELECT seat_id, winner_request_id, winner_pop_id, winner_timestamp_ms "
                "FROM confirmed_seats"
            )
    except Exception as e:
        print(f"[central] 확정 좌석 복원 실패: {e}")
        restored_seats_gauge.set(0)
        return

    for r in rows:
        seat_windows[r["seat_id"]] = {
            "status": "confirmed",
            "buffer": [],
            "all_requests": [],
            "winner": {
                "request_id": r["winner_request_id"],
                "pop_id": r["winner_pop_id"],
                "corrected_epoch_ms": r["winner_timestamp_ms"],
                "pop_arrival_epoch_ms": r["winner_timestamp_ms"],
            },
        }

    restored_seats_gauge.set(len(rows))
    confirmed_seats_gauge.set(
        sum(1 for w in seat_windows.values() if w["status"] == "confirmed")
    )
    print(f"[central] 확정 좌석 {len(rows)}건을 Postgres에서 복원했습니다")


# ===========================================================================
# 로그인 / 인증  [CENTRAL_AUTH_V1]
# ---------------------------------------------------------------------------
# 설계 원칙 (좌석 판정을 절대 건드리지 않는다)
#   1. DB 는 auth_conn() 만 쓴다 -> 인증 풀이 고갈돼도 판정용 커넥션은 그대로다.
#   2. bcrypt 는 asyncio.to_thread 로 뺀다 -> 이벤트 루프가 100ms 씩 멈추면
#      좌석 요청 처리까지 같이 멈춘다. 로그인 1건이 서비스 전체를 세우는 셈이다.
#   3. 토큰은 RS256(비대칭). 중앙은 개인키로 서명만 하고 PoP 는 공개키로 검증만
#      한다. PoP 가 인증 때문에 중앙에 물어보는 순간 대전은 매 요청 13ms 가
#      붙어서, chrony 로 맞춘 6us 정밀도가 통째로 무의미해진다.
#   4. 키쌍은 DB 에 저장한다 -> central 이 재시작해도 같은 키를 쓰므로
#      이미 발급된 토큰이 무효가 되지 않는다.
# ===========================================================================
import jwt as _jwt
import bcrypt as _bcrypt
import uuid as _uuid
import datetime as _dt
from fastapi import HTTPException as _HTTPException, Request as _Request
from pydantic import BaseModel as _BaseModel
from prometheus_client import Counter as _Counter, Gauge as _Gauge, Histogram as _Histogram
from cryptography.hazmat.primitives import serialization as _ser
from cryptography.hazmat.primitives.asymmetric import rsa as _rsa

# 토큰 수명(분). 티켓 오픈 30분 전에 로그인해도 오픈 후까지 살아 있어야 한다.
# 짧게 잡으면 정각에 전원 재로그인이 몰려서 최악의 타이밍에 부하가 생긴다.
TOKEN_TTL_MIN = int(os.environ.get("TOKEN_TTL_MIN", "120"))
# bcrypt 강도. 12 는 약 250ms, 10 은 약 60ms. 보안과 가용성의 절충이며
# 크리덴셜 스터핑이 몰릴 때 CPU 가 버티는지가 이 값에 달려 있다.
BCRYPT_ROUNDS = int(os.environ.get("BCRYPT_ROUNDS", "10"))

_private_pem = None
_public_pem = None

auth_signup_total = _Counter("central_auth_signup_total", "가입 성공 건수")
auth_login_total = _Counter("central_auth_login_total", "로그인 시도 결과", ["result"])
auth_login_duration = _Histogram(
    "central_auth_login_duration_seconds", "로그인 처리 시간(초). bcrypt 가 대부분을 차지한다",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0),
)
auth_users_gauge = _Gauge("central_auth_users", "가입된 계정 수")
auth_sessions_gauge = _Gauge("central_auth_sessions", "누적 세션(로그인) 수")


class _SignupReq(_BaseModel):
    email: str = ""
    password: str = ""
    device_fp: str = ""


class _LoginReq(_BaseModel):
    email: str = ""
    password: str = ""
    device_fp: str = ""


def _client_ip(request):
    """
    nginx 뒤에 있으므로 request.client.host 는 도커 게이트웨이 IP 하나로만 보인다.
    nginx 가 $proxy_add_x_forwarded_for 로 맨 뒤에 붙인 값이 실제 peer IP 다.
    앞쪽 값은 클라이언트가 위조할 수 있으므로 마지막 것만 신뢰한다.
    """
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        parts = [x.strip() for x in xff.split(",") if x.strip()]
        if parts:
            return parts[-1]
    c = getattr(request, "client", None)
    return c.host if c and c.host else ""


async def init_auth_db():
    """인증 테이블 생성 + 서명 키쌍 준비. lifespan 에서 한 번만 부른다."""
    global _private_pem, _public_pem

    async with auth_conn() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS auth_keys (
                id TEXT PRIMARY KEY,
                private_pem TEXT NOT NULL,
                public_pem TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                account_id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                account_id TEXT NOT NULL,
                device_fp TEXT,
                login_ip TEXT,
                pop_hint TEXT,
                login_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )""")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_account ON sessions(account_id)")
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_sessions_login_at ON sessions(login_at)")

        row = await conn.fetchrow("SELECT private_pem, public_pem FROM auth_keys WHERE id='main'")
        if row is None:
            # 키 생성은 CPU 를 좀 쓰므로 스레드로 뺀다(기동 시 1회).
            def _gen():
                key = _rsa.generate_private_key(public_exponent=65537, key_size=2048)
                priv = key.private_bytes(
                    encoding=_ser.Encoding.PEM,
                    format=_ser.PrivateFormat.PKCS8,
                    encryption_algorithm=_ser.NoEncryption(),
                ).decode()
                pub = key.public_key().public_bytes(
                    encoding=_ser.Encoding.PEM,
                    format=_ser.PublicFormat.SubjectPublicKeyInfo,
                ).decode()
                return priv, pub

            priv, pub = await asyncio.to_thread(_gen)
            await conn.execute(
                "INSERT INTO auth_keys (id, private_pem, public_pem) VALUES ('main',$1,$2) "
                "ON CONFLICT (id) DO NOTHING", priv, pub)
            row = await conn.fetchrow(
                "SELECT private_pem, public_pem FROM auth_keys WHERE id='main'")
            print("[central] 인증 서명 키쌍을 새로 생성했습니다 (RS256)")
        else:
            print("[central] 인증 서명 키쌍을 DB에서 불러왔습니다 (RS256)")

        _private_pem = row["private_pem"]
        _public_pem = row["public_pem"]

        auth_users_gauge.set(await conn.fetchval("SELECT COUNT(*) FROM users"))
        auth_sessions_gauge.set(await conn.fetchval("SELECT COUNT(*) FROM sessions"))

    print(f"[central] 인증 준비 완료 (토큰 수명 {TOKEN_TTL_MIN}분 / bcrypt rounds {BCRYPT_ROUNDS})")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, auth_pool, kafka_producer

    # postgres 컨테이너가 central보다 늦게 준비될 수 있어 재시도하며 기다린다.
    for attempt in range(10):
        try:
            db_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=DB_POOL_MAX_SIZE,
                # 쿼리가 이 시간을 넘기면 강제 취소하고 커넥션을 회수한다.
                # 이게 없으면 걸린 쿼리 하나가 커넥션을 영구히 물고 있을 수 있다.
                command_timeout=DB_COMMAND_TIMEOUT,
                # 오래 놀고 있는 커넥션은 정리해서 DB 쪽 자원도 붙잡지 않게 한다.
                max_inactive_connection_lifetime=300.0,
            )
            # 인증 전용 풀. 같은 Postgres 인스턴스지만 커넥션을 따로 잡아둔다.
            # 로그인 러시가 판정용 커넥션을 침범하지 못하게 하는 차단벽이다.
            auth_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=AUTH_POOL_MAX_SIZE,
                command_timeout=DB_COMMAND_TIMEOUT,
                max_inactive_connection_lifetime=300.0,
            )
            print(f"[central] Postgres 연결 성공 "
                  f"(판정 풀 {DB_POOL_MAX_SIZE} / 인증 풀 {AUTH_POOL_MAX_SIZE})")
            break
        except Exception as e:
            print(f"[central] Postgres 연결 실패 (재시도 {attempt + 1}/10): {e}")
            await asyncio.sleep(3)
    await init_db()
    await restore_confirmed_seats()
    await init_auth_db()

    # seat-status 발행용 producer. kafka 컨테이너도 늦게 준비될 수 있어 재시도.
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    for attempt in range(10):
        try:
            await kafka_producer.start()
            print(f"[central] Kafka producer 시작됨 (broker={KAFKA_BROKER})")
            break
        except Exception as e:
            print(f"[central] Kafka producer 연결 실패 (재시도 {attempt + 1}/10): {e}")
            await asyncio.sleep(3)

    # 2차 판정: Redis 연결 + 세션 적재, 서울 규칙 묶음 구독  [MACRO3_V1]
    await _l2.start(os.environ.get("REDIS_URL", ""), auth_pool, kafka_producer)
    l2_rules_task = asyncio.create_task(_l2.rules_loop(KAFKA_BROKER))

    # 판정 컨슈머를 파티션 수만큼 병렬로 띄운다.  [PARCONSUME_V1]
    consumer_tasks = [
        asyncio.create_task(kafka_consume_loop(i))
        for i in range(KAFKA_JUDGE_CONSUMERS)
    ]
    print(f"[central] 판정 컨슈머 {KAFKA_JUDGE_CONSUMERS}개 기동", flush=True)
    pool_task = asyncio.create_task(track_db_pool_usage())
    # PoP 전달 지연 수신  [HEARTBEAT_V1]
    heartbeat_task = asyncio.create_task(heartbeat_consume_loop())
    # 수집창 크기 자동 산정  [WINDOW_V1]
    window_task = asyncio.create_task(window_refresh_loop())
    yield
    for t in consumer_tasks:
        t.cancel()
    l2_rules_task.cancel()
    pool_task.cancel()
    await kafka_producer.stop()
    await db_pool.close()


async def track_db_pool_usage():
    """
    asyncpg 풀은 자기 상태를 이벤트로 알려주지 않기 때문에, 주기적으로 직접 물어봐서
    Gauge에 옮겨 담는다.

    *** 갱신 주기를 1초로 맞춘 이유 (2026-08-14) ***
    Prometheus가 1초마다 긁어가는데 이 루프가 5초마다 값을 갱신하면, 5번 중 4번은
    "직전과 똑같은 낡은 값"을 수집하게 되어 대시보드가 계단처럼 끊겨 보인다.
    수집 주기와 갱신 주기를 맞춰야 그래프가 매끄럽게 흐른다.
    """
    while True:
        try:
            if db_pool is not None:
                db_pool_size_gauge.set(db_pool.get_size())
                db_pool_idle_gauge.set(db_pool.get_idle_size())
                # DB 기준 확정 좌석 수도 같이 갱신 (최대 608행이라 COUNT 비용은 무시 가능).
                async with db_conn() as conn:
                    n = await conn.fetchval("SELECT COUNT(*) FROM confirmed_seats")
                    confirmed_seats_db_gauge.set(n)
        except Exception:
            pass
        await asyncio.sleep(1)


app = FastAPI(title="Mini Central Judgment Server", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus용 /metrics.  [PARPROC_V1]
# 판정이 단일 코어(단일 프로세스)에 묶여 있었으므로(실측 CPU 100%=1코어),
# uvicorn --workers 로 여러 프로세스를 띄워 판정을 여러 코어에 분산한다.
# 그러면 prometheus_client 는 프로세스마다 지표를 따로 들고 있어, 스크레이프
# 때마다 한 워커 값만 잡혀 그래프가 요동친다. PoP 와 동일하게
# PROMETHEUS_MULTIPROC_DIR 이 있으면 전 워커 지표를 합산해 내보낸다.
_MP_DIR = os.environ.get("PROMETHEUS_MULTIPROC_DIR", "")
if not _MP_DIR:
    # 단일 프로세스: 기존과 완전히 동일하게 동작한다.
    Instrumentator().instrument(app).expose(app)
else:
    os.makedirs(_MP_DIR, exist_ok=True)
    import re as _mpre
    from fastapi import Response as _MPResponse
    from prometheus_client import CollectorRegistry as _MPReg, generate_latest as _mpgen
    from prometheus_client import multiprocess as _mpmp
    from prometheus_client import CONTENT_TYPE_LATEST as _MPCT

    def _mp_strip_pid(text):
        # 워커마다 값이 같은 Gauge(pid 라벨)는 첫 줄만 남긴다. (Counter 는 자동 합산,
        # 부분 상태 Gauge 는 livesum 으로 정의해 합산되므로 여기 안 걸린다.)
        out, seen = [], set()
        for line in text.splitlines():
            if not line or line.startswith("#"):
                out.append(line); continue
            if 'pid="' in line:
                l = _mpre.sub(r'pid="[^"]*",?\s*', '', line)
                l = l.replace('{,', '{').replace(',}', '}').replace('{}', '')
                key = l.rsplit(' ', 1)[0]
                if key in seen:
                    continue
                seen.add(key)
                out.append(l)
            else:
                out.append(line)
        return "\n".join(out) + "\n"

    @app.get("/metrics")
    async def _central_metrics_mp():
        reg = _MPReg()
        _mpmp.MultiProcessCollector(reg)
        return _MPResponse(content=_mp_strip_pid(_mpgen(reg).decode()), media_type=_MPCT)

    print(f"[central] 지표 합산 모드 (PROMETHEUS_MULTIPROC_DIR={_MP_DIR})", flush=True)

# 지금 판정 대기 중인 좌석이 몇 개인지 실시간으로 보여주는 커스텀 지표.
pending_seats_gauge = Gauge(
    "central_pending_seats", "현재 수집창이 열려 있어 판정 대기 중인 좌석 수",
    multiprocess_mode="livesum",  # 워커마다 자기 파티션 좌석만 세므로 합산해야 총계  [PARPROC_V1]
)

# 지금까지 확정된 좌석 총 개수 (숫자만 필요할 때).
confirmed_seats_gauge = Gauge(
    "central_confirmed_seats", "지금까지 확정된 좌석 총 개수",
    multiprocess_mode="livesum",  # 워커별 부분합 -> 합산  [PARPROC_V1]
)

# 좌석별로 "어떤 PoP가 이겼는지"까지 라벨로 남겨두는 지표.
confirmed_seat_detail_gauge = Gauge(
    "central_confirmed_seat_detail",
    "좌석이 확정되었는지 여부(항상 1). seat_id/winner_pop_id 라벨로 어떤 좌석을 어떤 PoP가 가져갔는지 구분",
    ["seat_id", "winner_pop_id"],
)

# ----------------------------------------------------------------------------
# 관측(Observability)용 메트릭 — Grafana 대시보드가 이 값들을 읽어서 그린다.
#
# Counter와 Gauge의 차이가 중요하다:
#  - Counter는 "누적 개수"라서 절대 줄어들지 않는다. 값 자체보다 rate()로 미분해서
#    "초당 몇 건인지"를 보는 데 쓴다 (예: 요청 처리율).
#  - Gauge는 "지금 이 순간의 값"이라 오르내린다 (예: 현재 대기 중인 좌석 수).
#  - Histogram은 분포를 버킷으로 나눠 담아서, 평균이 아니라 p95/p99 같은
#    꼬리 지연(tail latency)을 볼 수 있게 해준다. 티켓팅처럼 "대부분 빠른데 일부가
#    느린" 상황은 평균만 보면 문제를 놓치기 때문에 반드시 분위수로 봐야 한다.
# ----------------------------------------------------------------------------

# Kafka: central이 seat-requests에서 실제로 꺼내 처리한 메시지 누적 수.
# PoP쪽 발행 카운터와 이 값을 rate()로 비교하면 "발행량 대비 소비량"이 보이고,
# 소비가 뒤처지면 central이 병목이라는 뜻이 된다.
kafka_consumed_total = Counter(
    "central_kafka_consumed_total", "central이 seat-requests 토픽에서 소비한 메시지 누적 수"
)

# Kafka: central이 seat-status로 내보낸 이벤트 수 (pending/confirmed로 라벨 구분).
kafka_published_total = Counter(
    "central_kafka_published_total", "central이 seat-status 토픽에 발행한 이벤트 누적 수", ["status"]
)

kafka_publish_errors_total = Counter(
    "central_kafka_publish_errors_total", "seat-status 발행이 실패한 누적 횟수"
)

# 판정 요청 접수 결과. already_confirmed가 늘어난다는 건 "이미 끝난 좌석에
# 뒤늦게 도착한 요청"이 많다는 뜻이라, 사용자 입장에서 헛클릭이 얼마나 나는지 보여준다.
judgment_requests_total = Counter(
    "central_judgment_requests_total", "판정 대상으로 접수된 요청 누적 수", ["result"]
)

# 판정이 완료(수집창 마감)된 좌석 누적 수.
judgment_completed_total = Counter(
    "central_judgment_completed_total", "수집창이 닫혀 승자가 결정된 좌석 누적 수"
)

# *** 좌석 경쟁 강도 *** — 좌석 하나를 두고 몇 명이 붙었는지의 분포.
# 이 시스템의 존재 이유(공정한 경쟁)를 가장 직접적으로 보여주는 지표.
competitors_per_seat = Histogram(
    "central_competitors_per_seat",
    "좌석 하나의 수집창에서 경쟁한 요청 수 분포",
    buckets=[1, 2, 3, 5, 10, 20, 50, 100, 500],
)

# *** 1등과 2등의 타임스탬프 격차(ms) *** — 이 값이 작을수록 "간발의 차"로
# 승부가 갈렸다는 뜻이고, 클럭 동기화 정확도가 판정에 직접 영향을 주는 구간이다.
# (오프셋 오차가 이 격차보다 크면 판정이 뒤집힐 수 있으므로, 공정성의 핵심 근거 지표.)
tie_lottery_total = Counter(
    "central_tie_lottery_total",
    "시계 최대 오차 안에서 동점이 나 추첨으로 승자를 정한 좌석 수  [TIE_LOTTERY_V1]",
)
winner_margin_ms = Histogram(
    "central_winner_margin_ms",
    "승자와 2위의 도착 타임스탬프 차이(ms)",
    buckets=[1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000],
)

# Postgres 쓰기 성능. 좌석 확정 경로에 직접 들어있는 유일한 DB 쓰기라서,
# 여기가 느려지면 확정 이벤트 발행 전체가 밀린다.
db_write_duration = Histogram(
    "central_db_write_duration_seconds",
    "confirmed_seats INSERT에 걸린 시간(초)",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

db_write_errors_total = Counter(
    "central_db_write_errors_total", "Postgres 쓰기가 실패한 누적 횟수"
)

# asyncpg 커넥션 풀 상태. max_size(5)에 근접하면 DB 커넥션이 병목이라는 신호이고,
# 그때가 pgbouncer 같은 커넥션 풀러를 도입할 시점이다.
db_pool_size_gauge = Gauge("central_db_pool_size", "asyncpg 커넥션 풀이 들고 있는 전체 커넥션 수")
db_pool_idle_gauge = Gauge("central_db_pool_idle", "asyncpg 커넥션 풀에서 놀고 있는 커넥션 수")

# *** 중요 *** 위의 confirmed_seats_gauge는 메모리(seat_windows) 기준이라 서버가
# 재시작되면 0으로 리셋된다. 이 지표는 Postgres를 직접 COUNT한 "진짜" 확정 좌석 수라서
# 재시작 이후에도 정확하다. 대시보드의 잔여석/확정석 숫자는 반드시 이 쪽을 써야 한다.
confirmed_seats_db_gauge = Gauge(
    "central_confirmed_seats_db", "Postgres에 실제로 기록된 확정 좌석 수 (재시작에도 유지되는 값)"
)

# *** 커넥션을 얻기까지 기다린 시간 *** — 병목을 가장 먼저 알려주는 지표다.
# 커넥션 수 자체는 상한이 있어 절대 폭주하지 않으므로, 부하가 걸리면
# "커넥션이 늘어나는" 게 아니라 "대기 시간이 길어지는" 형태로 나타난다.
db_acquire_wait = Histogram(
    "central_db_acquire_wait_seconds",
    "커넥션 풀에서 커넥션을 얻기까지 기다린 시간(초)",
    buckets=[0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 3.0, 5.0],
)

# 대기하다 못 얻고 포기한 횟수. 1 이상이면 풀 크기를 늘리거나 쿼리를 줄여야 한다.
db_acquire_timeouts = Counter(
    "central_db_acquire_timeouts_total", "커넥션 획득이 타임아웃된 누적 횟수"
)

# 좌석 확정 INSERT를 재시도한 횟수. 확정은 실패하면 안 되는 지점이라 재시도를 붙였는데,
# 이 값이 오르면 DB가 불안정하다는 뜻이다.
db_write_retries = Counter(
    "central_db_write_retries_total", "좌석 확정 INSERT를 재시도한 누적 횟수"
)

# 좌석별 수집창 상태를 메모리에 들고 있는다.
# { seat_id: {"status": "pending"|"confirmed", "buffer": [...], "winner": {...}} }
seat_windows: dict[str, dict] = {}


class JudgeRequest(BaseModel):
    seat_id: str
    request_id: str
    pop_id: str
    pop_arrival_epoch_ms: int
    # 같은 시각의 마이크로초 값. 있으면 판정에 이 값을 쓴다.  [ARRIVAL_US_V1]
    pop_arrival_epoch_us: Optional[int] = None
    # PoP가 공통 기준 NTP 서버 대비 측정한 자기 시계 오차. 판정 시 위 원본에 더한다.
    # 기본값 0.0인 이유: 오프셋을 안 보내는 구버전 PoP나 수동 테스트 요청이 와도
    # "보정 없음"으로 취급해 판정이 멈추지 않게 한다.
    clock_offset_ms_measured: float = 0.0
    # 보정 후에도 남는 그 PoP 시계의 최대 오차. 동점 추첨 범위.  [TIE_LOTTERY_V1]
    clock_error_ms: float = 0.0


def arrival_ms_precise(epoch_ms: int, epoch_us: Optional[int]) -> float:
    """
    판정에 쓸 원본 도착 시각(ms, 소수점 아래 µs 포함).  [ARRIVAL_US_V1]

    PoP 가 ms 로 자른 값만 쓰면 같은 1ms 안의 요청들이 동점이 되고, 그 동점을
    µs 단위 시계 오프셋이 가른다 — 실제 순서와 무관하게 오프셋이 작은 PoP 가 늘
    이기는 지역 편향이다. 그래서 us 값이 있으면 그것을 쓴다.
    us 가 없거나(구버전 PoP, 토픽에 남은 옛 메시지) ms 와 1ms 이상 어긋나면 ms 로 판정한다.
    """
    if epoch_us is not None:
        precise = epoch_us / 1000.0
        if abs(precise - epoch_ms) < 1.0:
            return precise
    return float(epoch_ms)


async def publish_seat_status(payload: dict):
    """
    seat-status 토픽에 발행하는 공용 헬퍼. confirmed 이벤트는 반드시 DB 커밋 이후에
    호출되어야 한다 (호출부에서 순서를 보장함 — 이 함수 자체는 순서를 모른다).
    """
    try:
        await kafka_producer.send_and_wait(
            KAFKA_TOPIC_SEAT_STATUS, key=payload["seat_id"], value=payload
        )
        kafka_published_total.labels(status=payload.get("status", "unknown")).inc()
    except Exception as e:
        kafka_publish_errors_total.inc()
        print(f"[central] seat-status 발행 실패: {e}")


def pick_winner(seat_id: str, ranked: list) -> tuple:
    """
    보정 시각 오름차순 목록에서 승자를 고른다. 반환: (승자, 추첨 기록 또는 None).  [TIE_LOTTERY_V1]

    *** 시계 오차 안의 동점은 추첨한다 ***
    두 요청의 [보정 시각 ± 그 PoP 시계 최대 오차] 구간이 겹치면, 시계로는 누가 먼저인지
    보장할 수 없다. 그때 "작은 값이 승자"로 정하면 측정 잡음이 승자를 정하고, 한 PoP 의
    시계가 늘 조금 늦게 재면 그 지역이 동점에서 늘 진다 — 지역 편향이다.
    그래서 가장 빠른 요청과 구간이 겹치는 요청들을 묶어 추첨한다.

    추첨은 누구나 다시 계산할 수 있다:
        seed = sha1("좌석|요청ID들을 정렬해 쉼표로 이은 것"),  승자 = 정렬된 묶음[int(seed 앞 8자리, 16) % 묶음 크기]
    Redis 경로(redis_window.LUA_CLOSE)도 같은 식을 쓴다.
    """
    first = ranked[0]
    limit = first["corrected_epoch_ms"] + (first.get("clock_error_ms") or 0.0)
    group = sorted((e for e in ranked if e["corrected_epoch_ms"] - (e.get("clock_error_ms") or 0.0) <= limit),
                   key=lambda e: e["request_id"])
    if len(group) < 2:
        return first, None
    ids = [e["request_id"] for e in group]
    seed = hashlib.sha1(f"{seat_id}|{','.join(ids)}".encode()).hexdigest()
    tie_lottery_total.inc()
    return group[int(seed[:8], 16) % len(group)], {"group": ids, "seed": seed}


def apply_seat_request(
    seat_id: str,
    request_id: str,
    pop_id: str,
    pop_arrival_epoch_ms: int,
    clock_offset_ms_measured: float = 0.0,
    pop_arrival_epoch_us: Optional[int] = None,
    clock_error_ms: float = 0.0,
) -> dict:
    """
    판정 로직의 실제 본체. HTTP 핸들러(/judge)와 Kafka consumer 양쪽에서
    똑같이 호출한다 — "누가 이 함수를 호출하느냐"만 다를 뿐, 로직은 하나로 통일되어 있다.
    """
    # 이미 확정된 좌석이면 더 볼 것도 없이 바로 거절
    if seat_id in seat_windows and seat_windows[seat_id]["status"] == "confirmed":
        judgment_requests_total.labels(result="already_confirmed").inc()
        return {
            "status": "already_confirmed",
            "seat_id": seat_id,
            "winner_request_id": seat_windows[seat_id]["winner"]["request_id"],
        }

    # *** 여기가 클럭 보정이 실제로 적용되는 유일한 지점 ***
    # PoP가 보낸 원본 시각에, 그 PoP가 공통 기준 NTP 서버 대비 측정한 자기 시계
    # 오차를 더한다. 오프셋이 양수면 그 PoP 시계가 기준보다 느렸다는 뜻이므로,
    # 더해줘야 "기준 시계로 환산한 실제 도착 시각"이 된다.
    # 원본도 함께 보관하는 이유: 판정 후에 "무엇을 얼마나 보정해서 이 결과가 나왔는지"를
    # 되짚을 수 있어야 하기 때문 (보정값만 남기면 검증이 불가능해진다).
    corrected_epoch_ms = (
        arrival_ms_precise(pop_arrival_epoch_ms, pop_arrival_epoch_us) + clock_offset_ms_measured
    )

    entry = {
        "request_id": request_id,
        "pop_id": pop_id,
        # PoP가 찍은 보정 전 원본 시각.
        "pop_arrival_epoch_ms": pop_arrival_epoch_ms,
        "pop_arrival_epoch_us": pop_arrival_epoch_us,
        # 실제 판정에 쓰이는 보정된 시각.
        "corrected_epoch_ms": corrected_epoch_ms,
        # 보정 후에도 남는 시계 최대 오차. 동점 추첨 범위.  [TIE_LOTTERY_V1]
        "clock_error_ms": clock_error_ms,
    }

    if seat_id not in seat_windows:
        # 이 좌석에 대한 첫 요청 -> 창을 새로 연다
        seat_windows[seat_id] = {"status": "pending", "buffer": [entry], "winner": None}
        _win_touch()  # 요청이 시작됐으므로 창 크기를 고정  [WINDOW_V1]
        asyncio.create_task(close_window_after_delay(seat_id))
        pending_seats_gauge.set(sum(1 for w in seat_windows.values() if w["status"] == "pending"))
    else:
        # 이미 열려있는 창에 그냥 추가
        seat_windows[seat_id]["buffer"].append(entry)

    # pending 상태는 DB에 안 남기는 휘발성 상태라서, DB 커밋과 무관하게 바로 발행한다.
    # (fire-and-forget: 예약 흐름을 이 발행 때문에 기다리게 하지 않는다.)
    requests_so_far = len(seat_windows[seat_id]["buffer"])
    asyncio.create_task(
        publish_seat_status(
            {"seat_id": seat_id, "status": "pending", "requests_so_far": requests_so_far}
        )
    )

    judgment_requests_total.labels(result="accepted").inc()
    return {"status": "accepted", "seat_id": seat_id, "request_id": request_id}


@app.post("/judge")
async def receive_request(req: JudgeRequest):
    """
    *** (중요) 정상 운영 경로에서는 이제 이 엔드포인트를 PoP가 호출하지 않는다 ***
    PoP -> Central 요청 전달은 Kafka(seat-requests 토픽)로 이루어진다.
    이 엔드포인트는 디버그/수동 테스트 용도로만 남겨둔다.
    """
    return apply_seat_request(
        req.seat_id,
        req.request_id,
        req.pop_id,
        req.pop_arrival_epoch_ms,
        req.clock_offset_ms_measured,
        req.pop_arrival_epoch_us,
        req.clock_error_ms,
    )


async def kafka_consume_loop(consumer_idx: int = 0):
    """
    *** (중요) PoP가 발행한 좌석 요청을 실제로 판정 로직에 태우는 진입점 ***
    kafka 컨테이너가 central보다 늦게 준비될 수 있어서(docker-compose의
    depends_on은 "시작"만 보장하지 "준비 완료"는 보장 안 함), 연결 실패 시
    5초 대기 후 재시도하는 루프로 감싼다.

    *** 왜 여러 개를 띄우나  [PARCONSUME_V1] ***
    예전에는 이 루프가 딱 하나였다. seat-requests 파티션이 3개여도 컨슈머가
    하나뿐이면 한 컨슈머가 세 파티션을 혼자 받아 한 건씩 직렬 처리하므로,
    파티션을 나눈 의미가 없고 판정율이 한 코어에 묶였다(실측 ~2.5k/s).
    같은 group_id 로 컨슈머를 파티션 수만큼 띄우면 Kafka 가 파티션을 각
    컨슈머에 배분하고, getmany 로 배치 소비해 fetch 대기를 서로 겹칠 수 있다.
    key=seat_id 파티셔닝 덕분에 한 좌석은 늘 같은 컨슈머로만 가므로
    seat_windows(공유 dict)가 꼬이지 않는다.
    """
    while True:
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC_SEAT_REQUESTS,
            bootstrap_servers=KAFKA_BROKER,
            group_id="central-judge-group",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            print(f"[central] judge consumer #{consumer_idx} 시작됨 "
                  f"(broker={KAFKA_BROKER}, topic={KAFKA_TOPIC_SEAT_REQUESTS})")
            while True:
                # 배치로 끌어온다. 한 파티션의 여러 건을 한 번에 받아
                # 파이썬 async 오버헤드를 줄이고, 다른 컨슈머의 fetch 와 겹친다.
                batches = await consumer.getmany(
                    timeout_ms=200, max_records=KAFKA_JUDGE_BATCH
                )
                for _tp, msgs in batches.items():
                    # 2차 판정: 걸린 요청은 판정 후보에서만 뺀다 (순서·추첨은 그대로)  [MACRO3_V1]
                    keep = await _l2.filter_batch([m.value for m in msgs])
                    for msg, ok in zip(msgs, keep):
                        data = msg.value
                        kafka_consumed_total.inc()
                        if not ok:
                            continue
                        apply_seat_request(
                            data["seat_id"],
                            data["request_id"],
                            data["pop_id"],
                            data["pop_arrival_epoch_ms"],
                            # .get()으로 읽는 이유: 오프셋을 안 실어 보내는 구버전
                            # PoP가 아직 떠 있거나, 토픽에 예전 메시지가 남아 있어도
                            # 판정이 KeyError로 멈추지 않게 한다 (보정 없이 판정).
                            data.get("clock_offset_ms_measured", 0.0),
                            data.get("pop_arrival_epoch_us"),
                            data.get("clock_error_ms", 0.0),
                        )
        except asyncio.CancelledError:
            await consumer.stop()
            raise
        except Exception as e:
            print(f"[central] judge consumer #{consumer_idx} 오류, 5초 후 재시도: {e}")
            await consumer.stop()
            await asyncio.sleep(5)


async def close_window_after_delay(seat_id: str):
    # 고정값 대신 실측 지연에서 산정한 값을 쓴다.  [WINDOW_V1]
    # 창을 여는 순간의 값을 그대로 써서, 좌석마다 창 길이가 달라지지 않게 한다.
    await asyncio.sleep(_win_current())

    window = seat_windows[seat_id]
    buffer = window["buffer"]

    # *** 보정된 시각(원본 + 그 PoP의 클럭 오프셋) 기준으로 정렬 -> 가장 빠른 게 승자 ***
    # 원본이 아니라 보정값으로 비교해야 한다. 원본으로 비교하면 시계가 조금 빠른
    # 지역이 실제로는 늦게 눌렀는데도 이기는 일이 생긴다.
    buffer_sorted = sorted(buffer, key=lambda e: e["corrected_epoch_ms"])
    # 시계 오차 안의 동점은 추첨. Redis 경로와 같은 후보(앞 ALL_REQUESTS_KEEP 개)로 고른다.
    winner, lottery = pick_winner(seat_id, buffer_sorted[:ALL_REQUESTS_KEEP])  # [TIE_LOTTERY_V1]

    window["status"] = "confirmed"
    window["winner"] = winner
    window["lottery"] = lottery
    window["all_requests"] = buffer_sorted[:ALL_REQUESTS_KEEP]  # [CAP_V1]
    pending_seats_gauge.set(sum(1 for w in seat_windows.values() if w["status"] == "pending"))
    confirmed_seats_gauge.set(sum(1 for w in seat_windows.values() if w["status"] == "confirmed"))
    confirmed_seat_detail_gauge.labels(seat_id=seat_id, winner_pop_id=winner["pop_id"]).set(1)

    # 이 좌석에 몇 명이 붙었는지, 그리고 1등과 2등이 얼마나 아슬아슬했는지 기록.
    judgment_completed_total.inc()
    competitors_per_seat.observe(len(buffer_sorted))
    if len(buffer_sorted) >= 2:
        # margin도 보정된 시각 기준으로 재야 의미가 있다 (실제 판정이 그 값으로 갈렸으므로).
        margin = buffer_sorted[1]["corrected_epoch_ms"] - buffer_sorted[0]["corrected_epoch_ms"]
        winner_margin_ms.observe(margin)

    confirmed_at = datetime.now(timezone.utc).isoformat()

    # *** (중요) Postgres 쓰기가 먼저, seat-status 발행은 그 다음 ***
    # DB 커밋이 성공한 게 확인된 뒤에만 이벤트를 내보낸다. 순서를 바꾸면 "DB엔 없는데
    # 이벤트만 나간" 상태가 생겨서, 이 이벤트를 구독하는 PoP들이 존재하지 않는 확정을
    # 신뢰하게 될 수 있다. INSERT가 예외를 던지면 아래 발행 코드는 실행되지 않는다.
    # DB 쓰기가 실패하면 여기서 예외를 잡아 카운터만 올리고 "그대로 함수를 빠져나간다".
    # 아래의 발행 코드로 절대 내려가지 않게 하는 것이 핵심 — 이 return이 곧
    # "DB에 안 남았으면 이벤트도 안 나간다"는 불변식(invariant)을 지키는 장치다.
    # *** 여기는 실패하면 안 되는 지점이라 재시도를 붙인다 ***
    # 확정이 DB에 안 남으면 이벤트도 안 나가고(아래 return), 그 좌석은 영영
    # 미확정으로 남아 아무도 살 수 없게 된다. 일시적인 DB 문제로 좌석이
    # 증발하는 걸 막기 위해 짧게 세 번까지 다시 시도한다.
    # (INSERT가 ON CONFLICT DO UPDATE라서 여러 번 실행돼도 결과가 같다 — 재시도 안전.)
    written = False
    for attempt in range(3):
        try:
            with db_write_duration.time():
                async with db_conn() as conn:
                    await conn.execute(
                        """
                        INSERT INTO confirmed_seats (seat_id, winner_request_id, winner_pop_id, winner_timestamp_ms, confirmed_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (seat_id) DO UPDATE SET
                            winner_request_id = EXCLUDED.winner_request_id,
                            winner_pop_id = EXCLUDED.winner_pop_id,
                            winner_timestamp_ms = EXCLUDED.winner_timestamp_ms,
                            confirmed_at = EXCLUDED.confirmed_at
                        """,
                        seat_id,
                        winner["request_id"],
                        winner["pop_id"],
                        # 실제로 판정을 결정한 보정된 시각. BIGINT 컬럼이라 정수로 맞춘다.
                        round(winner["corrected_epoch_ms"]),
                        confirmed_at,
                    )
            written = True
            break
        except Exception as e:
            db_write_errors_total.inc()
            if attempt < 2:
                db_write_retries.inc()
                print(f"[central] Postgres 쓰기 실패 (seat_id={seat_id}), 재시도 {attempt + 1}/2: {e}")
                await asyncio.sleep(0.2 * (attempt + 1))
            else:
                print(f"[central] Postgres 쓰기 최종 실패 (seat_id={seat_id}), seat-status 발행 안 함: {e}")

    if not written:
        # DB에 안 남았으므로 이벤트도 내보내지 않는다.
        # 이 불변식(DB 커밋 -> 이벤트 발행)을 깨면 PoP들이 존재하지 않는 확정을 믿게 된다.
        return

    # DB 커밋 성공을 확인한 이 시점에만 seat-status 토픽에 confirmed 이벤트를 발행한다.
    await publish_seat_status(
        {
            "seat_id": seat_id,
            "status": "confirmed",
            "winner_request_id": winner["request_id"],
            "winner_pop_id": winner["pop_id"],
            # 판정을 결정한 보정된 시각. all_requests 안에 각 요청의 원본/오프셋도
            # 함께 들어 있어서, PoP나 모니터링 화면에서 근거를 그대로 볼 수 있다.
            "winner_timestamp_ms": winner["corrected_epoch_ms"],
            "confirmed_at": confirmed_at,
            "all_requests": buffer_sorted[:ALL_REQUESTS_KEEP],  # [CAP_V1]
            # 동점 추첨을 했으면 묶음과 seed. 누구나 같은 식으로 승자를 다시 계산할 수 있다.
            "lottery": lottery,  # [TIE_LOTTERY_V1]
        }
    )


@app.get("/seat-status/{seat_id}")
def get_seat_status(seat_id: str):
    if seat_id not in seat_windows:
        return {"status": "open", "seat_id": seat_id}

    window = seat_windows[seat_id]
    if window["status"] == "pending":
        return {
            "status": "pending",
            "seat_id": seat_id,
            "requests_so_far": len(window["buffer"]),
        }

    return {
        "status": "confirmed",
        "seat_id": seat_id,
        "winner_request_id": window["winner"]["request_id"],
        "winner_pop_id": window["winner"]["pop_id"],
        "all_requests": window["all_requests"],
        "lottery": window.get("lottery"),  # [TIE_LOTTERY_V1]
    }


@app.post("/auth/signup")
async def auth_signup(req: _SignupReq):
    """
    가입. 실명/전화번호는 받지 않는다 — 봇 탐지에 필요한 건 신원이 아니라
    '가입 시각'과 '행동 궤적'이고, 개인정보를 줄일수록 심사에서도 안전하다.
    """
    email = (req.email or "").strip().lower()
    if not email or "@" not in email:
        raise _HTTPException(status_code=400, detail="올바른 이메일을 입력하세요")
    if len(req.password or "") < 4:
        raise _HTTPException(status_code=400, detail="비밀번호는 4자 이상이어야 합니다")

    pw = req.password.encode()
    pw_hash = await asyncio.to_thread(
        lambda: _bcrypt.hashpw(pw, _bcrypt.gensalt(rounds=BCRYPT_ROUNDS)).decode())

    account_id = "acc-" + _uuid.uuid4().hex[:16]
    try:
        async with auth_conn() as conn:
            await conn.execute(
                "INSERT INTO users (account_id, email, password_hash) VALUES ($1,$2,$3)",
                account_id, email, pw_hash)
            auth_users_gauge.set(await conn.fetchval("SELECT COUNT(*) FROM users"))
    except Exception as e:
        if "duplicate" in str(e).lower() or "unique" in str(e).lower():
            raise _HTTPException(status_code=409, detail="이미 가입된 이메일입니다")
        raise

    auth_signup_total.inc()
    return {"account_id": account_id, "email": email}


@app.post("/auth/login")
async def auth_login(req: _LoginReq, request: _Request):
    """
    로그인. session_id 는 서버가 만들고 device_fp 는 받아서 토큰에 봉인한다.
    두 값 모두 JWT 클레임으로 들어가므로 클라이언트가 이후에 바꿀 수 없다.
    봇이 지문을 바꾸려면 재로그인해야 하고, 재로그인은 sessions 에 기록이 남는다.
    """
    t0 = time.monotonic()
    email = (req.email or "").strip().lower()

    async with auth_conn() as conn:
        row = await conn.fetchrow(
            "SELECT account_id, password_hash, created_at FROM users WHERE email=$1", email)

    ok = False
    if row is not None:
        ok = await asyncio.to_thread(
            _bcrypt.checkpw, (req.password or "").encode(), row["password_hash"].encode())

    if not ok:
        auth_login_total.labels(result="fail").inc()
        auth_login_duration.observe(time.monotonic() - t0)
        raise _HTTPException(status_code=401, detail="이메일 또는 비밀번호가 올바르지 않습니다")

    session_id = "ses-" + _uuid.uuid4().hex[:16]
    device_fp = (req.device_fp or "")[:64]
    ip = _client_ip(request)

    async with auth_conn() as conn:
        await conn.execute(
            "INSERT INTO sessions (session_id, account_id, device_fp, login_ip) "
            "VALUES ($1,$2,$3,$4)", session_id, row["account_id"], device_fp, ip)
        auth_sessions_gauge.set(await conn.fetchval("SELECT COUNT(*) FROM sessions"))
    await _l2.on_login(session_id, row["account_id"], device_fp)  # [MACRO3_V1]

    now = _dt.datetime.now(_dt.timezone.utc)
    created = row["created_at"]
    if created.tzinfo is None:
        created = created.replace(tzinfo=_dt.timezone.utc)
    age_days = max(0, (now - created).days)

    token = _jwt.encode(
        {
            "sub": row["account_id"],   # 계정 (가입 시 1회 발급)
            "sid": session_id,          # 이번 접속 (로그인마다 새로)
            "dfp": device_fp,           # 브라우저 지문 (받아서 봉인)
            "age": age_days,            # 계정 나이(일) — 위조 불가한 봇 신호
            "lip": ip,                  # 로그인 IP — 위조 가능해서 보조 신호로만  [MACRO3_V1]
            "iat": int(now.timestamp()),
            "exp": int((now + _dt.timedelta(minutes=TOKEN_TTL_MIN)).timestamp()),
        },
        _private_pem, algorithm="RS256")

    auth_login_total.labels(result="ok").inc()
    auth_login_duration.observe(time.monotonic() - t0)
    return {
        "token": token,
        "account_id": row["account_id"],
        "email": email,
        "session_id": session_id,
        "account_age_days": age_days,
        "expires_in": TOKEN_TTL_MIN * 60,
    }


@app.get("/auth/public-key")
async def auth_public_key():
    """
    PoP 가 기동 시 한 번 가져가서 캐시한다. 요청마다 가져오는 게 아니므로
    판정 지연에 영향이 없다. 개인키는 절대 나가지 않는다.
    """
    if not _public_pem:
        raise _HTTPException(status_code=503, detail="인증 키가 아직 준비되지 않았습니다")
    return {"alg": "RS256", "public_pem": _public_pem}


@app.get("/auth/stats")
async def auth_stats():
    """운영 점검용 요약."""
    async with auth_conn() as conn:
        users = await conn.fetchval("SELECT COUNT(*) FROM users")
        sessions = await conn.fetchval("SELECT COUNT(*) FROM sessions")
        recent = await conn.fetchval(
            "SELECT COUNT(*) FROM sessions WHERE login_at > NOW() - INTERVAL '1 hour'")
    return {
        "users": users, "sessions": sessions, "logins_1h": recent,
        "token_ttl_min": TOKEN_TTL_MIN, "bcrypt_rounds": BCRYPT_ROUNDS,
        "key_ready": bool(_public_pem),
    }


@app.get("/seats-overview")
def seats_overview():
    """
    좌석 화면 전체를 그릴 때 쓰는 API. (central 자체 디버그/모니터용 — PoP는 이제
    이 엔드포인트를 안 쓰고 Kafka seat-status 구독으로 받은 로컬 캐시를 쓴다.)
    """
    return {
        seat_id: window["status"]  # "pending" 또는 "confirmed"
        for seat_id, window in seat_windows.items()
    }


@app.get("/seat-count")
async def seat_count():
    """
    메인 페이지(index.html)의 '잔여석' 표시용 API.
    confirmed 수는 메모리(seat_windows)가 아니라 Postgres에서 직접 COUNT 한다 —
    서버가 재시작되면 seat_windows는 비워지지만 DB(confirmed_seats)는 남아있기 때문에,
    재시작 이후에도 정확한 값을 돌려주려면 DB가 진짜 source of truth여야 한다.
    """
    async with db_conn() as conn:
        confirmed = await conn.fetchval("SELECT COUNT(*) FROM confirmed_seats")

    pending = sum(1 for w in seat_windows.values() if w["status"] == "pending")

    return {
        "total": TOTAL_SEATS,
        "confirmed": confirmed,
        "pending": pending,
        "remaining": TOTAL_SEATS - confirmed,
    }


@app.get("/confirmed-seats")
async def list_confirmed_seats():
    async with db_conn() as conn:
        rows = await conn.fetch("SELECT * FROM confirmed_seats")
    return [dict(r) for r in rows]


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/time")
def get_current_time():
    """
    *** 이 서버는 더 이상 마스터 시계가 아니다 (2026-08-14) ***
    예전에는 PoP들이 이 엔드포인트를 Cristian's Algorithm의 기준으로 삼아 자기 시계
    오차를 쟀지만, 지금은 모든 PoP가 동일한 외부 NTP 서버를 직접 바라본다.
    중앙서버를 기준으로 삼으면 이 서버 자신의 시계 오차와 판교까지의 네트워크
    비대칭이 모든 판정의 기준에 섞여 들어가기 때문이다.

    이 엔드포인트는 이제 디버그용으로만 남는다 (중앙 시계가 지금 어디 있는지 확인).
    """
    now = datetime.now(timezone.utc)
    return {"central_epoch_ms": int(now.timestamp() * 1000)}


@app.get("/debug/windows")
def debug_windows():
    """
    지금 이 순간 모든 좌석의 수집창 상태를 그대로 보여준다.
    """
    result = []
    for seat_id, window in seat_windows.items():
        result.append(
            {
                "seat_id": seat_id,
                "status": window["status"],
                "buffer": window["buffer"],
                "winner": window["winner"],
            }
        )
    return result


# 모니터링 화면(정적 파일) 서빙 — 반드시 API 라우트들을 다 정의한 뒤 마지막에 마운트한다.
app.mount("/", StaticFiles(directory="static", html=True), name="static")


# ===========================================================================
# 전달 지연 수신 및 기록  [HEARTBEAT_V1]
# ---------------------------------------------------------------------------
# 각 PoP 가 보낸 시각과 중앙이 받은 시각의 차이가 곧 전달 지연이다.
# 이 값이 수집창 길이를 정하는 근거가 된다.
#
# 평균이 아니라 상위 백분위를 봐야 한다. 평균으로 창을 잡으면
# 절반의 요청이 창 밖으로 밀려나 판정에서 누락된다.
# ===========================================================================
import os as _hb_os
from prometheus_client import Histogram as _HBHist, Gauge as _HBGauge

HEARTBEAT_TOPIC = _hb_os.environ.get("KAFKA_TOPIC_HEARTBEAT", "pop-heartbeat")

pop_delay_ms = _HBHist(
    "central_pop_delay_ms",
    "PoP 에서 중앙까지 메시지가 전달되는 데 걸린 시간(밀리초)",
    ["pop_id"],
    buckets=(1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000),
)
pop_delay_last_ms = _HBGauge(
    "central_pop_delay_last_ms",
    "각 PoP 의 가장 최근 전달 지연(밀리초)",
    ["pop_id"],
)
pop_heartbeat_age_seconds = _HBGauge(
    "central_pop_heartbeat_age_seconds",
    "각 PoP 의 마지막 신호 이후 경과 시간(초)",
    ["pop_id"],
)

_hb_last_seen = {}


async def heartbeat_consume_loop():
    while True:
        consumer = None
        try:
            consumer = AIOKafkaConsumer(
                HEARTBEAT_TOPIC,
                bootstrap_servers=_hb_os.environ.get("KAFKA_BROKER", "kafka:9092"),
                group_id=None,
                auto_offset_reset="latest",
                value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            )
            await consumer.start()
            print("[central] PoP 전달 지연 측정 시작")
            async for msg in consumer:
                d = msg.value
                pid = d.get("pop_id", "unknown")
                sent = d.get("sent_epoch_ms")
                if not sent:
                    continue
                delay = (time.time_ns() // 1_000_000) - int(sent)
                if delay < 0:
                    continue
                pop_delay_ms.labels(pop_id=pid).observe(delay)
                _win_observe(pid, delay)  # 수집창 산정용 표본  [WINDOW_V1]
                pop_delay_last_ms.labels(pop_id=pid).set(delay)
                _hb_last_seen[pid] = time.time()
        except asyncio.CancelledError:
            if consumer:
                await consumer.stop()
            return
        except Exception as e:
            print(f"[central] 전달 지연 측정 오류, 5초 후 재시도: {e}")
        finally:
            if consumer:
                try:
                    await consumer.stop()
                except Exception:
                    pass
        await asyncio.sleep(5)


@app.get("/pop-delay")
async def get_pop_delay():
    now = time.time()
    out = {}
    for pid, seen in _hb_last_seen.items():
        age = now - seen
        pop_heartbeat_age_seconds.labels(pop_id=pid).set(age)
        out[pid] = {"last_seen_sec_ago": round(age, 1)}
    return {"pops": out, "topic": HEARTBEAT_TOPIC}


for _hb_i, _hb_r in enumerate(list(app.routes)):
    if getattr(_hb_r, "endpoint", None) is get_pop_delay:
        if _hb_i != 0:
            app.routes.insert(0, app.routes.pop(_hb_i))
        break


# ===========================================================================
# 수집창 크기 산정  [WINDOW_V1]
# ---------------------------------------------------------------------------
# 창은 가장 먼 지역의 전달 지연만큼만 열려 있으면 된다.
# heartbeat 로 실측한 지연의 상위 백분위에 여유를 더해 산정한다.
#
# 평균이 아니라 p99 를 쓰는 이유
#   평균으로 잡으면 절반의 요청이 창 밖으로 밀려나 판정에서 누락된다.
#   누락되는 쪽은 항상 먼 지역이므로 그대로 지역 편향이 된다.
#
# 부하 중 창을 바꾸면 안 되는 이유
#   1) 되먹임 - 부하로 지연이 늘면 창이 커지고, 좌석이 더 오래 잠겨
#      요청이 더 쌓이고, 창이 또 커진다
#   2) 판정 규칙 불일치 - 같은 순간 열린 좌석이 서로 다른 창으로 판정되면
#      결과를 설명할 수 없다
#   3) 재현 불가 - 같은 부하를 두 번 돌려도 결과가 달라진다
# ===========================================================================
import os as _win_os
from prometheus_client import Gauge as _WinGauge

WINDOW_MIN_MS = float(_win_os.environ.get("WINDOW_MIN_MS", "20"))
WINDOW_MAX_MS = float(_win_os.environ.get("WINDOW_MAX_MS", "500"))
WINDOW_MARGIN = float(_win_os.environ.get("WINDOW_MARGIN", "1.5"))
WINDOW_REFRESH_SEC = float(_win_os.environ.get("WINDOW_REFRESH_SEC", "30"))
WINDOW_IDLE_UNFREEZE_SEC = float(_win_os.environ.get("WINDOW_IDLE_UNFREEZE_SEC", "600"))

_win_ms = max(WINDOW_MIN_MS, min(WINDOW_MAX_MS, WINDOW_SECONDS * 1000.0))
_win_frozen = False
_win_last_seat_req = 0.0
_win_samples = {}

window_ms_gauge = _WinGauge("central_seat_window_ms", "현재 좌석 수집창 길이(밀리초)")
window_frozen_gauge = _WinGauge("central_seat_window_frozen", "수집창 갱신 동결 여부 (1=동결)")
window_ms_gauge.set(_win_ms)
window_frozen_gauge.set(0)


def _win_current() -> float:
    return _win_ms / 1000.0


def _win_touch():
    global _win_frozen, _win_last_seat_req
    _win_last_seat_req = time.time()
    if not _win_frozen:
        _win_frozen = True
        window_frozen_gauge.set(1)
        print(f"[central] 좌석 요청 감지 - 수집창 {_win_ms:.1f}ms 로 고정", flush=True)


def _win_observe(pop_id: str, delay_ms: float):
    s = _win_samples.setdefault(pop_id, [])
    s.append(delay_ms)
    if len(s) > 300:
        del s[: len(s) - 300]


def _win_p99(values):
    if not values:
        return None
    v = sorted(values)
    return v[min(len(v) - 1, int(len(v) * 0.99))]


async def window_refresh_loop():
    global _win_ms, _win_frozen
    while True:
        try:
            await asyncio.sleep(WINDOW_REFRESH_SEC)
            if _win_frozen and (time.time() - _win_last_seat_req) > WINDOW_IDLE_UNFREEZE_SEC:
                _win_frozen = False
                window_frozen_gauge.set(0)
                print("[central] 요청이 없어 수집창 갱신을 재개합니다", flush=True)
            if _win_frozen:
                continue
            worst = None
            for pid, vals in _win_samples.items():
                p = _win_p99(vals)
                if p is None:
                    continue
                if worst is None or p > worst[1]:
                    worst = (pid, p)
            if worst is None:
                continue
            new_ms = max(WINDOW_MIN_MS, min(WINDOW_MAX_MS, worst[1] * WINDOW_MARGIN))
            if abs(new_ms - _win_ms) >= 1.0:
                print(f"[central] 수집창 {_win_ms:.1f}ms -> {new_ms:.1f}ms "
                      f"(기준 {worst[0]} p99 {worst[1]:.1f}ms)", flush=True)
                _win_ms = new_ms
                window_ms_gauge.set(_win_ms)
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"[central] 수집창 갱신 오류: {e}", flush=True)


@app.get("/admin/window")
async def admin_window_get():
    out = {"window_ms": round(_win_ms, 1), "frozen": _win_frozen,
           "margin": WINDOW_MARGIN, "min_ms": WINDOW_MIN_MS,
           "max_ms": WINDOW_MAX_MS, "samples": {}}
    for pid, vals in _win_samples.items():
        p = _win_p99(vals)
        out["samples"][pid] = {"count": len(vals),
                               "p99_ms": round(p, 1) if p is not None else None,
                               "max_ms": round(max(vals), 1) if vals else None}
    return out


@app.post("/admin/window/freeze")
async def admin_window_freeze():
    global _win_frozen
    _win_frozen = True
    window_frozen_gauge.set(1)
    return {"frozen": True, "window_ms": round(_win_ms, 1)}


@app.post("/admin/window/unfreeze")
async def admin_window_unfreeze():
    global _win_frozen
    _win_frozen = False
    window_frozen_gauge.set(0)
    return {"frozen": False, "window_ms": round(_win_ms, 1)}


for _win_i, _win_r in enumerate(list(app.routes)):
    if getattr(_win_r, "path", "").startswith("/admin/window"):
        app.routes.insert(0, app.routes.pop(_win_i))


# =============================================================================
# [REDIS_V1] 수집창 상태를 Redis 로 옮기는 백엔드
#
# STATE_BACKEND=redis 일 때만 활성화된다. 기본값(memory)에서는 이 블록이 아무것도 바꾸지 않는다.
# 되돌리려면 환경변수만 memory 로 돌리면 된다.
#
# 이 블록이 하는 일 (기존 함수를 고치지 않고 파일 맨 끝에서 덮어쓴다):
#   1. kafka_consume_loop 를 다시 정의한다
#        - getmany 배치를 Redis 파이프라인 한 번(왕복 1회)으로 판정
#        - 오프셋은 수동 커밋: Redis 반영에 성공한 뒤에만 커밋한다
#          (Redis 장애 시 메시지는 Kafka 에 남고, 재전달돼도 ZADD NX 가 중복을 무시한다)
#   2. lifespan 을 감싼다: Redis 연결 -> Postgres confirmed 를 Redis 로 복원(SETNX)
#        -> 그 뒤에야 컨슈머가 소비를 시작한다 (복원 전에 소비하면 이중 확정 위험)
#   3. 창 마감: 프로세스 내 타이머(정상 경로) + 청소기(고아 창 복구). 마감 권한은 Redis Lua 가
#      선점하므로 타이머와 청소기, 여러 워커가 동시에 와도 한 번만 수행된다.
#   4. HTTP 조회(/seat-status, /seats-overview, /seat-count, /debug/windows)를 Redis 기준으로 교체
#
# 공정성 불변식: 승자 기준은 corrected_epoch_ms(PoP 원본 시각 + 시계 오프셋) 이다.
#
# [Step 2] REDIS_DB_BUFFER=1 을 함께 켜면 DB 앞단 버퍼가 활성화된다:
#   * 창 마감 Lua 가 (확정 키 기록 + DB 반영 Stream 적재 + 방송 대기 등록)을 원자적으로 수행
#   * seat-status 'confirmed' 방송은 DB 를 기다리지 않고 바로 나간다
#   * DB 쓰기는 배치 소비자(워커마다 1개, 같은 소비자 그룹)가 Stream 에서 모아 unnest 단일
#     INSERT 로 반영하고, 성공한 항목만 ACK 한다. 실패/크래시한 항목은 XAUTOCLAIM 으로 재처리
#   * 방송이 실패하거나 방송 직전에 크래시해도 청소기가 방송 대기 목록(sw:unbroadcast)을 보고
#     다시 방송한다 (PoP 캐시 갱신은 멱등)
# 꺼져 있으면(기본) Step 1 과 동일: Redis 확정 -> DB 쓰기 -> seat-status 방송.
# =============================================================================
_REDIS_BACKEND = os.environ.get("STATE_BACKEND", "memory").strip().lower() == "redis"

if _REDIS_BACKEND:
    from redis_window import RedisWindowStore as _RedisWindowStore

    REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
    # 마감 시각 + 이 시간이 지나도 열려 있는 창은 고아 창으로 보고 청소기가 닫는다.
    REDIS_SWEEP_GRACE_MS = float(os.environ.get("REDIS_SWEEP_GRACE_MS", "1000"))
    REDIS_SWEEP_INTERVAL_SEC = float(os.environ.get("REDIS_SWEEP_INTERVAL_SEC", "0.5"))
    REDIS_CONSUMER_RETRY_SEC = float(os.environ.get("REDIS_CONSUMER_RETRY_SEC", "5"))

    # ---- Step 2: DB 앞단 버퍼 ----
    _REDIS_DB_BUFFER = os.environ.get("REDIS_DB_BUFFER", "0").strip().lower() in ("1", "true", "yes", "on")
    REDIS_DB_BATCH_MAX = int(os.environ.get("REDIS_DB_BATCH_MAX", "500"))       # 한 번에 반영할 최대 건수
    REDIS_DB_SLOT_MS = float(os.environ.get("REDIS_DB_SLOT_MS", "50"))          # 첫 항목 이후 더 모으는 시간(ms)
    REDIS_DB_CLAIM_IDLE_MS = int(os.environ.get("REDIS_DB_CLAIM_IDLE_MS", "5000"))  # 이 시간 넘게 ACK 없으면 재처리
    REDIS_REBROADCAST_GRACE_MS = float(os.environ.get("REDIS_REBROADCAST_GRACE_MS", "2000"))

    dbbuf_batches_total = Counter("central_dbbuf_batches_total", "DB 앞단 버퍼가 Postgres 에 반영한 배치 수")
    dbbuf_rows_total = Counter("central_dbbuf_rows_total", "DB 앞단 버퍼가 Postgres 에 반영한 행 수")
    dbbuf_batch_size = Histogram("central_dbbuf_batch_size", "배치 하나의 행 수",
                                 buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500, 1000))
    dbbuf_persist_errors_total = Counter("central_dbbuf_persist_errors_total", "배치 반영 실패 횟수")
    dbbuf_rebroadcast_total = Counter("central_dbbuf_rebroadcast_total", "방송 대기 목록에서 다시 방송한 횟수")
    dbbuf_stream_len_gauge = Gauge("central_dbbuf_stream_len", "DB 반영 대기 Stream 길이")
    dbbuf_pending_gauge = Gauge("central_dbbuf_pending", "읽어 갔지만 아직 ACK 되지 않은 항목 수")

    _store = None
    _redis_ready = None
    _redis_bg_tasks: set = set()

    def _redis_spawn(coro):
        # create_task 의 결과를 붙잡아 두지 않으면 가비지 컬렉션으로 태스크가 사라질 수 있다.
        t = asyncio.create_task(coro)
        _redis_bg_tasks.add(t)
        t.add_done_callback(_redis_bg_tasks.discard)
        return t

    def _redis_entry(data: dict) -> dict:
        raw = data["pop_arrival_epoch_ms"]
        raw_us = data.get("pop_arrival_epoch_us")
        off = data.get("clock_offset_ms_measured", 0.0)
        # 이 서버의 central_server.py 에 마이크로초 정밀도 패치(arrival_ms_precise)가 있으면 그것을 쓴다.
        # 없으면 기존처럼 ms 원본을 쓴다. (기존 apply_seat_request 와 같은 식: 정밀 도착시각 + 오프셋)
        precise = globals().get("arrival_ms_precise")
        base = precise(raw, raw_us) if precise is not None else raw
        return {
            "request_id": data["request_id"],
            "pop_id": data["pop_id"],
            # PoP가 찍은 보정 전 원본 시각.
            "pop_arrival_epoch_ms": raw,
            "pop_arrival_epoch_us": raw_us,
            # 실제 판정에 쓰이는 보정된 시각
            "corrected_epoch_ms": base + off,
            # 보정 후에도 남는 시계 최대 오차. 마감 Lua 의 동점 추첨 범위.  [TIE_LOTTERY_V1]
            "clock_error_ms": data.get("clock_error_ms", 0.0),
        }

    async def _redis_apply_batch(msgs) -> None:
        """Kafka 메시지 배치를 Redis 파이프라인 1회로 판정에 반영한다."""
        win_sec = _win_current()          # 초 단위 (기존 close_window_after_delay 와 동일하게 사용)
        deadline_ms = time.time() * 1000.0 + win_sec * 1000.0
        items = []
        for m in msgs:
            d = m.value
            items.append((d["seat_id"], _redis_entry(d), deadline_ms))

        results = await _store.add_batch(items)

        last_count: dict = {}
        opened_any = False
        for (seat_id, _e, _d), res in zip(items, results):
            kafka_consumed_total.inc()
            if res.rejected:
                judgment_requests_total.labels(result="already_confirmed").inc()
                continue
            judgment_requests_total.labels(result="accepted").inc()
            if res.opened:
                opened_any = True
                _win_touch()  # 창 크기를 고정  [WINDOW_V1]
                _redis_spawn(_redis_close_after(seat_id, win_sec))
            last_count[seat_id] = res.requests_so_far

        # pending 방송은 좌석당 배치의 마지막 값만 보낸다 (카운트는 증가만 하므로 정보 손실 없음).
        for seat_id, n in last_count.items():
            _redis_spawn(
                publish_seat_status(
                    {"seat_id": seat_id, "status": "pending", "requests_so_far": n}
                )
            )
        if opened_any:
            pending_seats_gauge.set(await _store.pending_count())

    async def _redis_close_after(seat_id: str, delay_sec: float) -> None:
        try:
            await asyncio.sleep(delay_sec)
            await _redis_close(seat_id)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # 타이머가 실패해도 창은 Redis 에 남아 있으므로 청소기가 이어서 닫는다.
            print(f"[central] redis 창 마감 실패 (seat_id={seat_id}), 청소기가 재시도: {e}")

    def _redis_confirmed_payload(seat_id: str, c: dict) -> dict:
        w = c["winner"]
        return {
            "seat_id": seat_id,
            "status": "confirmed",
            "winner_request_id": w["request_id"],
            "winner_pop_id": w["pop_id"],
            "winner_timestamp_ms": w["corrected_epoch_ms"],
            "confirmed_at": c["confirmed_at"],
            "all_requests": c["top"],
            "lottery": c.get("lottery"),  # [TIE_LOTTERY_V1]
        }

    async def _redis_publish_confirmed(payload: dict) -> bool:
        """publish_seat_status 와 같은 발행이지만 성공 여부를 돌려준다 (실패 시 방송 대기 목록에 남기기 위해)."""
        try:
            await kafka_producer.send_and_wait(
                KAFKA_TOPIC_SEAT_STATUS, key=payload["seat_id"], value=payload
            )
            kafka_published_total.labels(status="confirmed").inc()
            return True
        except Exception as e:
            kafka_publish_errors_total.inc()
            print(f"[central] seat-status(confirmed) 발행 실패 (seat_id={payload.get('seat_id')}): {e}")
            return False

    async def _redis_close(seat_id: str) -> None:
        confirmed_at = datetime.now(timezone.utc).isoformat()
        buf = _REDIS_DB_BUFFER
        res = await _store.close_window(seat_id, confirmed_at, buffer=buf)
        if res is None:
            return  # 이미 다른 쪽(타이머/청소기/다른 워커)이 닫았다
        winner, top, total = res["winner"], res["top"], res["total"]

        judgment_completed_total.inc()
        competitors_per_seat.observe(total)
        if len(top) >= 2:
            winner_margin_ms.observe(top[1]["corrected_epoch_ms"] - top[0]["corrected_epoch_ms"])
        confirmed_seat_detail_gauge.labels(seat_id=seat_id, winner_pop_id=winner["pop_id"]).set(1)
        pending_seats_gauge.set(await _store.pending_count())

        if buf:
            # [Step 2] DB 를 기다리지 않고 바로 방송한다. DB 반영은 Stream 소비자가 맡는다.
            # 방송이 실패하거나 이 직후 크래시해도 방송 대기 목록에 남아 청소기가 다시 보낸다.
            if await _redis_publish_confirmed(_redis_confirmed_payload(seat_id, res)):
                await _store.mark_broadcast(seat_id)
            return

        # ---- Step 1 경로 (REDIS_DB_BUFFER=0): DB 쓰기가 먼저, seat-status 방송은 그 다음 ----
        written = False
        for attempt in range(3):
            try:
                with db_write_duration.time():
                    async with db_conn() as conn:
                        await conn.execute(
                            """
                            INSERT INTO confirmed_seats (seat_id, winner_request_id, winner_pop_id, winner_timestamp_ms, confirmed_at)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (seat_id) DO UPDATE SET
                                winner_request_id = EXCLUDED.winner_request_id,
                                winner_pop_id = EXCLUDED.winner_pop_id,
                                winner_timestamp_ms = EXCLUDED.winner_timestamp_ms,
                                confirmed_at = EXCLUDED.confirmed_at
                            """,
                            seat_id,
                            winner["request_id"],
                            winner["pop_id"],
                            round(winner["corrected_epoch_ms"]),
                            confirmed_at,
                        )
                written = True
                break
            except Exception as e:
                db_write_errors_total.inc()
                if attempt < 2:
                    db_write_retries.inc()
                    print(f"[central] Postgres 쓰기 실패 (seat_id={seat_id}), 재시도 {attempt + 1}/2: {e}")
                    await asyncio.sleep(0.2 * (attempt + 1))
                else:
                    print(f"[central] Postgres 쓰기 최종 실패 (seat_id={seat_id}), seat-status 발행 안 함: {e}")

        if not written:
            try:
                await _store.r.sadd("sw:unpersisted", seat_id)
            except Exception:
                pass
            return

        await publish_seat_status(
            {
                "seat_id": seat_id,
                "status": "confirmed",
                "winner_request_id": winner["request_id"],
                "winner_pop_id": winner["pop_id"],
                "winner_timestamp_ms": winner["corrected_epoch_ms"],
                "confirmed_at": confirmed_at,
                "all_requests": top,
                "lottery": res.get("lottery"),  # [TIE_LOTTERY_V1]
            }
        )

    # ------------------------------------------------ [Step 2] DB 앞단 배치 소비자
    _DBBUF_SQL = """
        INSERT INTO confirmed_seats (seat_id, winner_request_id, winner_pop_id, winner_timestamp_ms, confirmed_at)
        SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::bigint[], $5::text[])
        ON CONFLICT (seat_id) DO UPDATE SET
            winner_request_id = EXCLUDED.winner_request_id,
            winner_pop_id = EXCLUDED.winner_pop_id,
            winner_timestamp_ms = EXCLUDED.winner_timestamp_ms,
            confirmed_at = EXCLUDED.confirmed_at
    """

    async def _redis_persist_batch(entries) -> int:
        # 같은 좌석이 한 배치에 두 번 들어 있으면 'ON CONFLICT DO UPDATE ... second time' 오류가 나므로
        # 좌석당 마지막 항목만 남긴다.
        rows: dict = {}
        for _id, f in entries:
            rows[f["seat"]] = f
        seats = list(rows)
        with db_write_duration.time():
            async with db_conn() as conn:
                await conn.execute(
                    _DBBUF_SQL,
                    seats,
                    [rows[x]["rid"] for x in seats],
                    [rows[x]["pop"] for x in seats],
                    [round(float(rows[x]["ts"])) for x in seats],
                    [rows[x]["at"] for x in seats],
                )
        return len(seats)

    async def _redis_db_writer() -> None:
        """
        Stream 에서 확정 결과를 모아 Postgres 에 배치로 반영한다.
        - 첫 항목을 받으면 REDIS_DB_SLOT_MS 만큼 더 모은 뒤(최대 REDIS_DB_BATCH_MAX) 한 번에 INSERT
        - 반영에 성공한 항목만 ACK+삭제. 실패하면 ACK 하지 않아 REDIS_DB_CLAIM_IDLE_MS 후 재처리된다
        - 워커마다 하나씩 떠도 되고(같은 소비자 그룹), 죽은 소비자의 항목은 XAUTOCLAIM 으로 이어받는다
        """
        await _redis_ready.wait()
        name = f"{os.environ.get('HOSTNAME', 'central')}-{os.getpid()}"
        last_claim = 0.0
        grouped = False
        while True:
            try:
                if not grouped:
                    await _store.ensure_group()
                    grouped = True
                entries = await _store.read_batch(name, REDIS_DB_BATCH_MAX, 1000)
                if entries and len(entries) < REDIS_DB_BATCH_MAX and REDIS_DB_SLOT_MS > 0:
                    await asyncio.sleep(REDIS_DB_SLOT_MS / 1000.0)
                    entries += await _store.read_batch(name, REDIS_DB_BATCH_MAX - len(entries), None)
                if not entries and time.monotonic() - last_claim > 1.0:
                    last_claim = time.monotonic()
                    entries = await _store.claim_stale(name, REDIS_DB_CLAIM_IDLE_MS, REDIS_DB_BATCH_MAX)
                if not entries:
                    continue

                n = None
                for attempt in range(3):
                    try:
                        n = await _redis_persist_batch(entries)
                        break
                    except Exception as e:
                        db_write_errors_total.inc()
                        dbbuf_persist_errors_total.inc()
                        print(f"[central] DB 배치 반영 실패 ({len(entries)}건), 시도 {attempt + 1}/3: {e}")
                        await asyncio.sleep(0.2 * (attempt + 1))
                if n is None:
                    # ACK 하지 않는다 -> Stream 에 남아 있다가 claim 으로 다시 처리된다
                    await asyncio.sleep(1.0)
                    continue
                await _store.ack_del([i for i, _ in entries])
                dbbuf_batches_total.inc()
                dbbuf_rows_total.inc(n)
                dbbuf_batch_size.observe(n)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Redis 가 비워지거나(flushall) 잠시 끊긴 경우 포함: 그룹을 다시 만들고 이어간다
                print(f"[central] DB 앞단 소비자 오류, 재시도: {e}")
                grouped = False
                await asyncio.sleep(0.5)

    async def _redis_rebroadcast_due() -> None:
        for seat_id in await _store.unbroadcast_due(time.time() * 1000.0, REDIS_REBROADCAST_GRACE_MS):
            c = await _store.get_confirmed(seat_id)
            if c is None:
                await _store.mark_broadcast(seat_id)
                continue
            if await _redis_publish_confirmed(_redis_confirmed_payload(seat_id, c)):
                await _store.mark_broadcast(seat_id)
                dbbuf_rebroadcast_total.inc()

    async def _redis_sweeper() -> None:
        """마감 시각이 지났는데 열려 있는 창(타이머를 가진 워커가 죽은 경우 등)을 닫는다."""
        tick = 0
        while True:
            try:
                await asyncio.sleep(REDIS_SWEEP_INTERVAL_SEC)
                due = await _store.sweep_due(time.time() * 1000.0, REDIS_SWEEP_GRACE_MS)
                for seat_id in due:
                    await _redis_close(seat_id)
                if _REDIS_DB_BUFFER:
                    await _redis_rebroadcast_due()
                tick += 1
                if tick % 10 == 0:
                    pending_seats_gauge.set(await _store.pending_count())
                    if _REDIS_DB_BUFFER:
                        dbbuf_stream_len_gauge.set(await _store.stream_len())
                        dbbuf_pending_gauge.set(await _store.stream_pending())
                    n = 0
                    async for _k in _store.r.scan_iter(match="sw:*:c", count=1000):
                        n += 1
                    confirmed_seats_gauge.set(n)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[central] redis 청소기 오류: {e}")

    async def kafka_consume_loop(consumer_idx: int = 0):  # noqa: F811  (기존 정의를 덮어씀)
        """
        [REDIS_V1] 판정 컨슈머. 기존과 같은 group_id/배치 크기를 쓰되
        (1) 배치 전체를 Redis 파이프라인 1회로 처리하고 (2) 성공한 뒤에만 오프셋을 커밋한다.
        """
        await _redis_ready.wait()   # Postgres -> Redis 복원이 끝나기 전에는 소비하지 않는다
        while True:
            consumer = AIOKafkaConsumer(
                KAFKA_TOPIC_SEAT_REQUESTS,
                bootstrap_servers=KAFKA_BROKER,
                group_id="central-judge-group",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=False,
            )
            try:
                await consumer.start()
                print(f"[central] judge consumer #{consumer_idx} 시작됨 [REDIS_V1] "
                      f"(broker={KAFKA_BROKER}, topic={KAFKA_TOPIC_SEAT_REQUESTS})")
                while True:
                    batches = await consumer.getmany(
                        timeout_ms=200, max_records=KAFKA_JUDGE_BATCH
                    )
                    msgs = [m for ms in batches.values() for m in ms]
                    if not msgs:
                        continue
                    # 2차 판정: 걸린 요청은 판정 후보에서만 뺀다 (순서·추첨·Lua 는 그대로)  [MACRO3_V1]
                    keep = await _l2.filter_batch([m.value for m in msgs])
                    dropped = len(msgs) - sum(keep)
                    msgs = [m for m, ok in zip(msgs, keep) if ok]
                    if dropped:
                        kafka_consumed_total.inc(dropped)
                    if msgs:
                        await _redis_apply_batch(msgs)   # 예외가 나면 커밋하지 않는다
                    await consumer.commit()
            except asyncio.CancelledError:
                await consumer.stop()
                raise
            except Exception as e:
                print(f"[central] judge consumer #{consumer_idx} 오류, {REDIS_CONSUMER_RETRY_SEC:g}초 후 재시도: {e}")
                try:
                    await consumer.stop()
                except Exception:
                    pass
                await asyncio.sleep(REDIS_CONSUMER_RETRY_SEC)

    # ---------------------------------------------------------------- lifespan 감싸기
    _redis_orig_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def _redis_lifespan(a):
        global _store, _redis_ready
        _redis_ready = asyncio.Event()
        _store = _RedisWindowStore(REDIS_URL, keep=ALL_REQUESTS_KEEP)
        for attempt in range(10):
            try:
                await _store.start()
                print(f"[central] Redis 연결 성공 ({REDIS_URL}) [REDIS_V1]")
                break
            except Exception as e:
                print(f"[central] Redis 연결 실패 (재시도 {attempt + 1}/10): {e}")
                await asyncio.sleep(3)
        else:
            raise RuntimeError("Redis 에 연결하지 못했다")

        async with _redis_orig_lifespan(a) as state:
            # 원래 lifespan 이 db_pool 을 만들었으므로 이제 Postgres 를 읽을 수 있다.
            async with db_conn() as conn:
                rows = await conn.fetch(
                    "SELECT seat_id, winner_request_id, winner_pop_id, winner_timestamp_ms, confirmed_at "
                    "FROM confirmed_seats"
                )
            n = await _store.hydrate_confirmed([dict(r) for r in rows])
            print(f"[central] 확정 좌석 {n}건을 Postgres -> Redis 로 복원했습니다 [REDIS_V1]")
            _redis_ready.set()
            sweeper = asyncio.create_task(_redis_sweeper())
            writer = asyncio.create_task(_redis_db_writer()) if _REDIS_DB_BUFFER else None
            if _REDIS_DB_BUFFER:
                print("[central] DB 앞단 Redis Stream 버퍼 활성화됨 [REDIS_V1][DBBUF]", flush=True)
            try:
                yield state
            finally:
                sweeper.cancel()
                if writer is not None:
                    writer.cancel()
                for t in list(_redis_bg_tasks):
                    t.cancel()
        await _store.stop()

    app.router.lifespan_context = _redis_lifespan

    # ---------------------------------------------------------------- HTTP 조회 교체
    def _redis_swap_route(path: str, endpoint) -> None:
        """기존 경로를 지우고 새 핸들러를 등록한다. 정적 파일 mount("/")는 항상 마지막에 둔다."""
        app.router.routes = [r for r in app.router.routes if getattr(r, "path", None) != path]
        app.add_api_route(path, endpoint, methods=["GET"])
        new_route = app.router.routes.pop()
        idx = len(app.router.routes)
        for i, r in enumerate(app.router.routes):
            if r.__class__.__name__ == "Mount" and getattr(r, "path", None) == "":
                idx = i
                break
        app.router.routes.insert(idx, new_route)

    async def _redis_http_seat_status(seat_id: str):
        st = await _store.seat_state(seat_id)
        if st["status"] == "open":
            return {"status": "open", "seat_id": seat_id}
        if st["status"] == "pending":
            return {"status": "pending", "seat_id": seat_id, "requests_so_far": st["requests_so_far"]}
        w = st["winner"]
        return {
            "status": "confirmed",
            "seat_id": seat_id,
            "winner_request_id": w["request_id"],
            "winner_pop_id": w["pop_id"],
            "all_requests": st["all_requests"],
            "lottery": st.get("lottery"),  # [TIE_LOTTERY_V1]
        }

    async def _redis_http_seats_overview():
        return await _store.overview()

    async def _redis_http_seat_count():
        async with db_conn() as conn:
            confirmed = await conn.fetchval("SELECT COUNT(*) FROM confirmed_seats")
        pending = await _store.pending_count()
        return {
            "total": TOTAL_SEATS,
            "confirmed": confirmed,
            "pending": pending,
            "remaining": TOTAL_SEATS - confirmed,
        }

    async def _redis_http_debug_windows():
        out = []
        for seat_id, status in (await _store.overview()).items():
            st = await _store.seat_state(seat_id)
            out.append({"seat_id": seat_id, "status": status, **{k: v for k, v in st.items() if k != "status"}})
        return out

    _redis_swap_route("/seat-status/{seat_id}", _redis_http_seat_status)
    _redis_swap_route("/seats-overview", _redis_http_seats_overview)
    _redis_swap_route("/seat-count", _redis_http_seat_count)
    _redis_swap_route("/debug/windows", _redis_http_debug_windows)
    print("[central] STATE_BACKEND=redis 활성화됨 [REDIS_V1]", flush=True)
