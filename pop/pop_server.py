"""
가장 최소 구성의 PoP(엣지 서버) 시뮬레이터. (컨테이너 1: pop)

역할:
- 좌석 요청(seat_id)을 받으면, "이 요청이 PoP에 도착한 순간"의 타임스탬프를 서버가 직접 찍는다.
- 이 값이 실제 시스템에서 말하는 "엣지 타임스탬프"에 해당한다.
- 기록은 메모리 리스트에 저장한다.

*** (중요) central로의 판정 요청 전달 방식이 HTTP에서 Kafka로 교체됨 (2026-08-14) ***
      예전에는 여기서 httpx로 central의 /judge를 직접 호출했지만, 이제는
      "seat-requests" 토픽에 Kafka producer로 이벤트를 발행(produce)만 하고
      끝낸다. central이 그 토픽을 구독해서 실제 판정을 수행한다.
      seat_id를 Kafka 메시지 key로 써서, 같은 좌석에 대한 요청들이 같은
      파티션으로 가도록 한다 (파티션 내부에서는 발행 순서가 보장되기 때문).

*** (중요) seat-status 구독 + detection-log 발행 추가됨 (2026-08-14) ***
      - seat-status: central이 좌석 상태(pending/confirmed) 변화를 발행하는 토픽.
        모든 PoP(지금은 pop-pangyo, pop-daejeon)가 이걸 구독해서 seat_status_cache를
        채운다. /seat-status, /seats-overview는 이제 central에 HTTP로 안 물어보고
        이 로컬 캐시에서 바로 응답한다 — 왕복 지연 없이 즉시 응답 가능.
        *** 반드시 PoP마다 고유한 group_id를 써야 한다 *** (pop-status-cache-{POP_ID}).
        같은 group_id를 여러 PoP가 공유하면 파티션이 나뉘어 일부 PoP만 메시지를
        받게 되어, 그 PoP의 캐시가 불완전해진다.
      - detection-log: 매크로/봇 탐지용 원본 이벤트(요청 IP 포함)를 발행하는 토픽.
        서울 AI Cloud가 이걸 구독해서 continuous learning에 사용할 예정 (그쪽 소비자는
        이 저장소 밖에서 별도로 구성됨 — 여기서는 발행만 정확히 하면 된다).

*** (중요) 시간 기준이 "중앙서버"에서 "외부 NTP 서버"로 교체됨 (2026-08-14) ***
      예전 구조는 중앙서버(판교)를 마스터 시계로 두고, 각 PoP가 central의 /time을
      HTTP로 물어봐서(Cristian's Algorithm) 자기 오차를 재는 방식이었다. 그건
      틀린 구조였다 — 중앙서버 자신의 시계 오차와 판교까지의 네트워크 비대칭이
      그대로 모든 판정의 기준에 섞여 들어간다.

      지금 구조:
        1) 모든 PoP 호스트가 "동일한 외부 NTP 서버"에 chrony로 물려 있다.
           기준이 어느 PoP도 아닌 제3자이므로, 특정 지역이 유리해지지 않는다.
        2) 각 PoP는 그 기준 대비 자기 시계 오차를 chrony에게서 받아 5초마다 갱신한다
           (CLOCK_SYNC_INTERVAL_SECONDS).
        3) 좌석 요청을 발행할 때, "그 순간의 오프셋 값"을 타임스탬프와 함께 실어 보낸다.
           PoP는 자기 시각을 절대 고쳐 쓰지 않는다 — 원본 + 오프셋을 따로 보낸다.
        4) 중앙 판정서버가 (원본 타임스탬프 + 오프셋)을 더해서 보정된 시각으로 판정한다.
           보정 권한이 판정 주체 한 곳에 모여 있어야, 어떤 값으로 판정했는지 추적 가능하다.

*** (중요) 오프셋을 직접 재지 않고 chrony에게서 받아오는 이유 (2026-08-14) ***
      한때 이 파일 안에서 SNTP(UDP 123)로 NTP 서버에 직접 질의해 오프셋을 쟀지만,
      측정해보고 기각했다:
          단발 SNTP 질의 : 지터 표준편차 105us, 최대-최소 338us
          chrony 추정치  : RMS 19.7us
      게다가 단발 질의의 평균이 chrony 추정치와 166us 어긋나 있었다 — 단발 샘플은
      그 순간의 네트워크 큐잉/비대칭을 그대로 먹는데, chrony는 여러 샘플로 그걸 걸러낸다.
      PoP 두 곳이 각자 단발 질의를 하면 상대 오차가 최대 0.5ms까지 벌어져서,
      1ms 미만의 승부는 우리 측정 노이즈가 결정해버린다. 공정성을 위해 넣은 보정이
      오히려 무작위성을 주입하는 셈이었다.
      결정적으로, 컨테이너 시계는 이미 호스트 chrony가 보정한 시계다. 직접 재는 것은
      chrony가 하는 일을 더 나쁜 방식으로 병렬로 다시 하는 것에 불과했다.

      컨테이너의 localhost는 호스트가 아니라서 chronyd에 직접 못 닿으므로, 호스트가
      `chronyc -c tracking` 결과를 파일로 내려주고(pop-clock-offset.service) 컨테이너는
      그 파일을 읽기전용으로 마운트해 읽는다. chrony.conf 변경도, 이미지에 패키지
      추가도, 네트워크 노출도 필요 없다. (scripts/chrony-offset-writer.sh 참고)

      *** 기준이 흔들려도 다른 기준으로 갈아타지 않는다 (중요) ***
      판정에서 중요한 건 "절대적으로 정확한 시각"이 아니라 "모든 PoP가 같은 기준을
      쓰는 것"이다. 그래서 오프셋을 못 읽으면(수집기 정지, chrony 동기화 상실 등)
      다른 기준으로 갈아타거나 0으로 되돌리지 않고, 마지막 값을 그대로 유지한다.
      기준이 PoP마다 갈라지면 그게 곧 지역 편향이고, 0으로 되돌리면 "보정이 필요 없다"는
      거짓 신호가 된다. 로컬 시계도 chrony로 규율되어 있어서 짧은 장애 동안 실제로
      벌어지는 양은 작다 (측정된 skew 0.06ppm 기준: 5분 약 18us, 1시간 약 0.2ms).

"""

import asyncio
import json
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from prometheus_client import Counter, Gauge, Histogram
from prometheus_fastapi_instrumentator import Instrumentator

# 이 PoP 자신의 이름과, 판정을 맡길 중앙 서버 주소.
# docker-compose에서 각 PoP마다 POP_ID를 다르게 넣어줘서 pop-pangyo/pop-daejeon을 구분한다.
# CENTRAL_URL은 이제 /seat-count(잔여석 조회) 하나에만 HTTP로 쓰인다.
# 클럭 기준은 더 이상 central이 아니라 호스트 chrony가 물려 있는 외부 NTP 서버다.
POP_ID = os.environ.get("POP_ID", "pop-unknown")
CENTRAL_URL = os.environ.get("CENTRAL_URL", "http://central:8080")

# 호스트의 chrony 오프셋이 적힌 파일. 호스트에서 pop-clock-offset.service가
# 5초마다 `chronyc -c tracking` 출력을 여기에 원자적으로 갱신하고, 컨테이너는
# 이걸 읽기전용으로 마운트해서 읽는다 (scripts/chrony-offset-writer.sh 참고).
CLOCK_OFFSET_FILE = os.environ.get("CLOCK_OFFSET_FILE", "/clock/tracking.csv")

# Kafka 브로커 주소와 토픽들.
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC_SEAT_REQUESTS = "seat-requests"
KAFKA_TOPIC_SEAT_STATUS = "seat-status"
KAFKA_TOPIC_DETECTION_LOG = "detection-log"
KAFKA_TOPIC_BOT_RULES = "bot-rules"  # 서울이 승인된 규칙 묶음을 보낸다 [BOTRULES_V1]

kafka_producer: AIOKafkaProducer | None = None
# detection-log 전용. 판정용 전송기와 묶음을 섞지 않으려고 따로 둔다.  [DETLOG_V2]
detlog_producer: AIOKafkaProducer | None = None

# central이 발행하는 seat-status 이벤트를 받아 채우는 로컬 캐시.
# { seat_id: {"status": "pending"|"confirmed", ...} }
seat_status_cache: dict[str, dict] = {}

# *** central로 나가는 HTTP 연결을 재사용하기 위한 공용 클라이언트 (2026-08-14) ***
# 예전에는 요청마다 httpx.AsyncClient를 새로 만들어서 매번 새 TCP 연결이 생겼다.
# 대전처럼 중계기를 거쳐 나가는 환경에서는 그게 곧 중계기 연결 폭증으로 이어진다.
# 하나를 계속 재사용하면 연결 풀이 유지되어 실제 TCP 연결 수가 몇 개로 고정된다.
http_client: httpx.AsyncClient | None = None

# *** /seat-count 응답 캐시 ***
# 메인 화면이 5초마다 이 값을 폴링하는데, 사용자가 많아지면 그 수만큼
# central까지 왕복이 발생한다. 잔여석은 1초 늦어도 아무 문제가 없는 값이므로
# 짧게 캐시해서, 사용자 수와 무관하게 초당 최대 1회만 central에 물어보게 한다.
SEAT_COUNT_TTL_SECONDS = 1.0
_seat_count_cache: dict = {"data": None, "at": 0.0}
_seat_count_lock = asyncio.Lock()

# 공통 기준 NTP 서버 대비 이 PoP 시계가 얼마나 빠르거나 느린지 (ms 단위, +면 내 시계가 느림).
# sync_clock_offset()이 5초마다 갱신한다.
#
# *** PoP는 이 값으로 자기 시각을 고치지 않는다 ***
# 원본 타임스탬프와 이 오프셋을 "따로" 실어 보내고, 둘을 더해서 판정에 쓸지는
# 판정 주체인 중앙서버가 결정한다. PoP가 미리 고쳐버리면 중앙은 받은 값이 원본인지
# 보정된 값인지 구분할 수 없게 되고, 판정 근거를 사후에 추적할 수 없다.
clock_offset_ms: float = 0.0

# 5초마다 갱신. 좌석 판정의 승부 margin이 ms 단위이므로, 오프셋이 그보다
# 훨씬 촘촘하게 갱신되어야 판정 시점의 실제 오차를 반영할 수 있다.
# 호스트 수집기(chrony-offset-writer.sh)도 같은 주기로 파일을 갱신한다.
CLOCK_SYNC_INTERVAL_SECONDS = 5

# `chronyc -c tracking` CSV에서 시스템 시계 오차(초)가 들어있는 필드 위치 (0-based).
# 사람이 읽는 출력의 "System time" 줄과 같은 값이다. 실측으로 확인했다.
CHRONY_FIELD_SYSTEM_TIME = 4


def read_chrony_offset() -> float:
    """
    호스트가 내려준 chrony tracking 파일을 읽어서 이 호스트 시계의 오차(ms)를 낸다.

    *** 부호 규약 (실측으로 확인) ***
    사람이 읽는 출력이 "System time : 0.000032461 seconds fast of NTP time"일 때
    CSV 필드는 -0.000032456 이었다. 즉 음수면 내 시계가 빠르다는 뜻이고,
    어느 쪽이든 "기준 시각 = 로컬 시각 + offset"으로 더하면 된다.
    이 부호를 뒤집으면 오차를 없애는 대신 두 배로 키우게 되므로 절대 바꾸면 안 된다.
    """
    with open(CLOCK_OFFSET_FILE) as f:
        fields = f.read().strip().split(",")
    return float(fields[CHRONY_FIELD_SYSTEM_TIME]) * 1000


async def sync_clock_offset():
    """
    호스트 chrony가 측정한 이 PoP의 시계 오차를 읽어서 갱신한다.

    *** 실패하면 마지막 오프셋을 그대로 유지한다 ***
    예외를 잡는 이유는 두 가지다. 잡지 않으면 예외가 periodic_clock_sync 루프를
    죽여서 오프셋이 영구히 얼어붙고 재시도조차 하지 않는다. 그리고 잡은 뒤에
    clock_offset_ms를 건드리지 않으면 마지막 값이 그대로 유지되는데, 0으로 되돌리면
    "보정이 필요 없다"는 거짓 신호가 되므로 유지하는 편이 맞다.
    """
    global clock_offset_ms
    try:
        # 파일 읽기는 블로킹이므로 스레드로 넘긴다. tmpfs라 보통 즉시 끝나지만,
        # 이벤트 루프가 잠깐이라도 멈추면 그만큼 좌석 요청 처리가 밀린다.
        clock_offset_ms = await asyncio.to_thread(read_chrony_offset)
        clock_offset_gauge.labels(pop_id=POP_ID).set(clock_offset_ms)
        print(f"[{POP_ID}] 클럭 오프셋 갱신: {clock_offset_ms:+.4f}ms")
    except (OSError, ValueError, IndexError) as e:
        print(
            f"[{POP_ID}] 클럭 오프셋 읽기 실패 ({CLOCK_OFFSET_FILE}): {e} "
            f"— 마지막 값 유지 (offset={clock_offset_ms:+.4f}ms). "
            f"호스트에서 pop-clock-offset.service가 돌고 있는지 확인하세요."
        )


async def periodic_clock_sync():
    while True:
        await asyncio.sleep(CLOCK_SYNC_INTERVAL_SECONDS)
        await sync_clock_offset()


async def kafka_consume_status_loop():
    """
    *** (중요) central이 발행한 seat-status 이벤트를 구독해서 로컬 캐시를 채우는 루프 ***
    group_id를 PoP마다 고유하게(pop-status-cache-{POP_ID}) 줘야, 모든 PoP가 각자
    전체 메시지를 다 받는다. group_id를 여러 PoP가 공유하면 Kafka가 파티션을
    나눠 분배해버려서, 한 PoP는 일부 좌석 이벤트를 아예 못 받는 사고가 난다.
    auto_offset_reset="earliest"라서, PoP가 재시작되어도 지금까지의 전체 좌석
    상태를 토픽에서 다시 읽어와 캐시를 복원한다.
    """
    # 워커(프로세스)마다 다른 group_id 를 써야 각자 전체 좌석 상태를 다 받는다.
    # 같은 group_id 면 Kafka 가 파티션을 나눠줘서 워커별로 화면이 달라진다.
    #  [WORKERS_V1]
    group_id = f"pop-status-cache-{POP_ID}-{os.getpid()}"
    while True:
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC_SEAT_STATUS,
            bootstrap_servers=KAFKA_BROKER,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            print(f"[{POP_ID}] seat-status consumer 시작됨 (group={group_id})")
            async for msg in consumer:
                data = msg.value
                if not (data.get("status") == "pending" and seat_status_cache.get(data["seat_id"], {}).get("status") == "confirmed"): seat_status_cache[data["seat_id"]] = data
                pop_seat_status_consumed_total.labels(
                    pop_id=POP_ID, status=data.get("status", "unknown")
                ).inc()
                pop_seat_status_cache_size.labels(pop_id=POP_ID).set(len(seat_status_cache))
        except asyncio.CancelledError:
            await consumer.stop()
            raise
        except Exception as e:
            print(f"[{POP_ID}] seat-status consumer 오류, 5초 후 재시도: {e}")
            await consumer.stop()
            await asyncio.sleep(5)


async def kafka_consume_rules_loop():
    """
    서울이 발행한 규칙 묶음을 받아 통째로 교체한다.  [BOTRULES_V1]
    group 없이(group_id=None) earliest 로 읽어서, 워커마다·재시작마다 늘 토픽을
    처음부터 읽고 마지막(=최신) 묶음이 남는다. group 을 쓰면 도커 안 pid 가 재시작
    후에도 같아 커밋된 위치부터 이어 읽고, 그 전 규칙을 놓친다(격리 시험에서 확인).
    형식이 틀린 묶음은 버린다.
    """
    global active_rules
    while True:
        consumer = AIOKafkaConsumer(
            KAFKA_TOPIC_BOT_RULES,
            bootstrap_servers=KAFKA_BROKER,
            group_id=None,
            auto_offset_reset="earliest",
        )
        try:
            await consumer.start()
            async for msg in consumer:
                try:
                    active_rules = _br.validate(json.loads(msg.value))
                    _fl.RULESET_VERSION = active_rules["version"]
                    print(f"[{POP_ID}] 봇 규칙 적용: {active_rules['version']} "
                          f"({len(active_rules['rules'])}개)")
                except Exception as e:
                    print(f"[{POP_ID}] 봇 규칙 묶음 무시 (offset {msg.offset}): {e}")
        except asyncio.CancelledError:
            await consumer.stop()
            raise
        except Exception as e:
            print(f"[{POP_ID}] bot-rules consumer 오류, 5초 후 재시도: {e}")
            await consumer.stop()
            await asyncio.sleep(5)


async def safe_publish_detection_log(payload: dict):
    """
    detection-log는 예약 흐름의 핵심 경로가 아니므로(봇 탐지용 부가 로그),
    실패해도 예약 자체를 막으면 안 된다 — 그래서 별도 태스크로 fire-and-forget.
    """
    try:
        await detlog_producer.send(  # [CAP_V1] 응답을 기다리지 않는다
            KAFKA_TOPIC_DETECTION_LOG, key=payload["seat_id"], value=payload
        )
        pop_kafka_published_total.labels(pop_id=POP_ID, topic=KAFKA_TOPIC_DETECTION_LOG).inc()
    except Exception as e:
        pop_kafka_publish_errors_total.labels(
            pop_id=POP_ID, topic=KAFKA_TOPIC_DETECTION_LOG
        ).inc()
        print(f"[{POP_ID}] detection-log 발행 실패: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global kafka_producer, detlog_producer, http_client

    # 연결을 재사용하는 공용 클라이언트. keepalive를 켜두면 중계기를 거치는
    # 대전 환경에서도 TCP 연결이 몇 개로 고정된다.
    http_client = httpx.AsyncClient(
        timeout=5.0,
        limits=httpx.Limits(max_keepalive_connections=10, max_connections=20),
    )

    await sync_clock_offset()  # 시작하자마자 한 번 측정
    clock_task = asyncio.create_task(periodic_clock_sync())  # 이후 30초마다 재측정

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    # kafka 컨테이너가 아직 준비 안 됐을 수 있어서 재시도하며 기다린다.
    for attempt in range(10):
        try:
            await kafka_producer.start()
            print(f"[{POP_ID}] Kafka producer 시작됨 (broker={KAFKA_BROKER})")
            break
        except Exception as e:
            print(f"[{POP_ID}] Kafka 연결 실패 (재시도 {attempt + 1}/10): {e}")
            await asyncio.sleep(3)

    # *** detection-log 는 최대 1초 모았다가 큰 묶음으로 보낸다 ***  [DETLOG_V2]
    # 서울 학습용이라 1초 늦어도 된다. 판정용 전송기와 같이 쓰면 로그(1.5KB/건)가
    # 16KB 묶음을 잘게 쪼개 PoP 처리량이 약 40% 떨어졌다(격리 측정 2026-10-14).
    # 대가: PoP 가 죽으면 아직 안 보낸 최대 1초치 로그를 잃는다.
    detlog_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
        linger_ms=1000,
        max_batch_size=524288,
    )
    try:
        await detlog_producer.start()
    except Exception as e:
        print(f"[{POP_ID}] detection-log producer 시작 실패: {e}")

    status_consumer_task = asyncio.create_task(kafka_consume_status_loop())
    rules_consumer_task = asyncio.create_task(kafka_consume_rules_loop())  # [BOTRULES_V1]
    # 전달 지연 상시 측정용 발신  [HEARTBEAT_V1]
    heartbeat_task = asyncio.create_task(heartbeat_send_loop())

    yield

    clock_task.cancel()
    status_consumer_task.cancel()
    rules_consumer_task.cancel()
    await kafka_producer.stop()
    await detlog_producer.stop()
    if http_client is not None:
        await http_client.aclose()


app = FastAPI(title=f"Mini PoP Server ({POP_ID})", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Prometheus용 /metrics 엔드포인트를 자동 생성한다.
# 이걸 호출하면 요청수/응답시간/상태코드 같은 기본 HTTP 지표가 Prometheus 텍스트 포맷으로 노출된다.
# central의 Prometheus가 이 엔드포인트를 주기적으로 긁어간다(scrape, pull 방식).
Instrumentator().instrument(app).expose(app)

# 클럭 오프셋처럼 이 프로젝트에서만 의미 있는 커스텀 지표는 직접 Gauge로 등록해야 한다.
# pop_id를 라벨로 붙여서, 나중에 Grafana에서 PoP별로 나눠서 비교할 수 있게 한다.
clock_offset_gauge = Gauge(
    "pop_clock_offset_ms",
    "공통 기준 NTP 서버 대비 이 PoP 시계의 오차 (ms, 양수면 이 PoP 시계가 느림)",
    ["pop_id"],
)

# ----------------------------------------------------------------------------
# 관측용 메트릭. 모든 지표에 pop_id 라벨을 붙여서, 나중에 대전 PoP가 붙었을 때
# 대시보드에서 지역별로 나눠 비교할 수 있게 한다 (지금은 판교 하나뿐이지만
# 처음부터 라벨을 붙여둬야 나중에 쿼리를 안 고쳐도 된다).
# ----------------------------------------------------------------------------

# 이 PoP가 사용자로부터 직접 접수한 좌석 요청 수. central의 소비 카운터와
# rate()로 비교하면 "엣지에서 받은 양 vs 중앙에서 처리한 양"의 차이가 보인다.
pop_seat_requests_total = Counter(
    "pop_seat_requests_total", "이 PoP가 접수한 좌석 요청 누적 수", ["pop_id"]
)

pop_kafka_published_total = Counter(
    "pop_kafka_published_total", "이 PoP가 Kafka에 발행한 메시지 누적 수", ["pop_id", "topic"]
)

pop_kafka_publish_errors_total = Counter(
    "pop_kafka_publish_errors_total", "Kafka 발행이 실패한 누적 횟수", ["pop_id", "topic"]
)

# seat-requests 발행에 걸린 시간. 이 값이 곧 사용자가 체감하는 응답 지연의
# 대부분을 차지한다 (요청을 받아 Kafka에 넣는 순간까지가 PoP의 책임 구간).
pop_kafka_publish_duration = Histogram(
    "pop_kafka_publish_duration_seconds",
    "seat-requests 토픽 발행에 걸린 시간(초)",
    ["pop_id"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# central이 보낸 seat-status 이벤트를 이 PoP가 실제로 받아 처리한 수.
# central의 발행 카운터와 이 값이 벌어지면, 전파가 이 PoP까지 안 닿고 있다는 뜻.
pop_seat_status_consumed_total = Counter(
    "pop_seat_status_consumed_total", "seat-status 토픽에서 소비한 이벤트 누적 수", ["pop_id", "status"]
)

# 로컬 캐시에 들어있는 좌석 수 — central과 이 PoP의 상태가 얼마나 동기화됐는지의 척도.
pop_seat_status_cache_size = Gauge(
    "pop_seat_status_cache_size", "로컬 좌석 상태 캐시에 들어있는 좌석 수", ["pop_id"]
)

# 잔여석 조회가 로컬 캐시로 처리된 횟수 / 실제로 central까지 나간 횟수.
# 이 둘의 비율이 곧 "중계기 부하를 얼마나 줄였는가"를 보여준다.
seat_count_cache_hits = Counter(
    "pop_seat_count_cache_hits_total", "잔여석 조회를 로컬 캐시로 처리한 횟수", ["pop_id"]
)
seat_count_upstream_calls = Counter(
    "pop_seat_count_upstream_calls_total", "잔여석 조회로 central까지 실제로 나간 횟수", ["pop_id"]
)

records: list[dict] = []
# "좌석 예매하기" 버튼을 눌러 예매 흐름에 진입한 시각을 좌석 클릭 기록과 분리해서 저장
entry_records: list[dict] = []


class SeatRequest(BaseModel):
    seat_id: str
    client_sent_at: str | None = None  # 클라이언트가 주장하는 시각 (참고용, 판정에 안 씀)


@app.post("/seat-request")
async def receive_seat_request(req: SeatRequest, request: Request):
    # *** (중요) PoP는 시각을 절대 손대지 않는다 ***
    # 로컬 시계를 있는 그대로 찍고, 공통 기준 NTP 서버 대비 오차(clock_offset_ms)는
    # 따로 실어 보낸다. 둘을 더해서 판정에 쓸지는 판정 주체인 중앙서버가 결정한다.
    # PoP가 미리 고쳐버리면 중앙은 받은 값이 원본인지 보정된 값인지 알 수 없게 되고,
    # 판정 근거를 사후에 추적할 수 없다.
    # *** 접수 시각은 미들웨어가 ASGI 최상단에서 이미 확정했다 ***
    # 여기서 다시 찍으면 그 사이 미들웨어 처리 시간(봇 필터 30us + 토큰 검증
    # 69us)이 접수 시각에 섞인다. 그러면 토큰을 가진 사용자만 늦게 접수되어
    # 로그인한 사람이 손해를 본다. 공정 판정이라는 주제 자체가 무너지는 지점이다.
    # 미들웨어가 없거나 꺼져 있으면 예전처럼 여기서 찍는다.  [POPAUTH_PAYLOAD_V1]
    _edge_ns = getattr(request.state, "edge_arrival_ns", None) or time.time_ns()
    arrival_time = datetime.fromtimestamp(_edge_ns / 1_000_000_000, timezone.utc)

    # 검증 결과. 미들웨어가 넣어둔 값이며, 없으면 익명으로 본다.
    _identity = getattr(request.state, "identity", None) or _pa.ANONYMOUS

    # 발행 직전에 "지금 이 순간"의 오프셋을 읽는다. 5초마다 갱신되는 값이라
    # 요청 처리 중에 바뀔 수 있으므로, 한 번 읽어서 record와 Kafka 메시지에
    # 같은 값을 쓴다 (둘이 달라지면 사후 추적이 어긋난다).
    offset_ms = round(clock_offset_ms, 3)

    request_id = str(uuid.uuid4())
    # 나노초 정수에서 바로 뽑는다(부동소수 왕복 제거). [POPAUTH_PAYLOAD_V1]
    epoch_ms = _edge_ns // 1_000_000
    # *** 판정에는 마이크로초 값을 쓴다 ***  [ARRIVAL_US_V1]
    # 밀리초로 자르면 같은 1ms 안에 들어온 두 PoP 의 요청이 같은 값이 되고,
    # 그 동점을 µs 단위 시계 오프셋이 가르게 된다. 실제 도착 순서와 무관하게
    # 오프셋이 작은 PoP 가 늘 이기는 지역 편향이다. 오프셋 정밀도(1us)에 맞춘다.
    epoch_us = _edge_ns // 1_000

    # X-Forwarded-For 의 첫 값은 클라이언트가 마음대로 써 넣을 수 있다.
    # 1차 필터와 같은 함수로 nginx 가 직접 본 마지막 값을 쓴다.  [DETLOG_V2]
    client_ip = _fl.client_ip_of(request)
    user_agent = request.headers.get("user-agent", "unknown")

    record = {
        "request_id": request_id,
        "seat_id": req.seat_id,
        "client_sent_at": req.client_sent_at,
        "pop_id": POP_ID,
        "pop_arrival_timestamp": arrival_time.isoformat(),
        "pop_arrival_epoch_ms": epoch_ms,
        "pop_arrival_epoch_us": epoch_us,
        # PoP는 시각을 보정하지 않으므로 항상 0. (보정 여부는 중앙이 결정한다.)
        "clock_offset_ms_applied": 0.0,
        # 공통 기준 NTP 서버 대비 이 PoP의 시계 오차. 중앙이 (원본 + offset)으로 보정한다.
        "clock_offset_ms_measured": offset_ms,
        # 누가 요청했는지. 검증 실패해도 identity_verified=False 로 남긴다.
        # "검증 안 된 요청"과 "검증해봤더니 가짜인 요청"은 다른 신호다. [POPAUTH_PAYLOAD_V1]
        **_identity.as_payload(),
    }
    records.append(record)

    # 여기서 판정을 직접 하지 않고, "원본 타임스탬프"를 Kafka에 발행만 한다.
    # PoP는 시각을 기록해서 넘겨주는 역할까지만 하고, 여러 PoP 간의 비교/판정은
    # central이 이 토픽을 구독해서 담당한다 (동기 HTTP 응답을 기다리지 않는다).
    pop_seat_requests_total.labels(pop_id=POP_ID).inc()
    try:
        with pop_kafka_publish_duration.labels(pop_id=POP_ID).time():
            _pubfut = await kafka_producer.send(
                KAFKA_TOPIC_SEAT_REQUESTS,
                key=req.seat_id,
                value={
                    "seat_id": req.seat_id,
                    "request_id": request_id,
                    "pop_id": POP_ID,
                    # 보정하지 않은 원본 도착 시각. 판정은 us 값을 쓰고, ms 는
                    # us 를 모르는 구버전 central 과 기존 화면/로그를 위해 남긴다.
                    "pop_arrival_epoch_ms": epoch_ms,
                    "pop_arrival_epoch_us": epoch_us,
                    # *** 중앙이 판정할 때 위 원본에 더할 보정값 ***
                    # 공통 기준 NTP 서버 대비 이 PoP의 시계 오차(5초마다 갱신).
                    "clock_offset_ms_measured": offset_ms,
                    # 중앙이 동일 계정/동일 기기의 중복 요청을 판정 단계에서
                    # 걸러낼 수 있게 신원을 함께 보낸다.  [POPAUTH_PAYLOAD_V1]
                    **_identity.as_payload(),
                },
            )
        _pubfut.add_done_callback(_seat_publish_done)  # [NOWAIT_V1]
    except Exception:
        pop_kafka_publish_errors_total.labels(
            pop_id=POP_ID, topic=KAFKA_TOPIC_SEAT_REQUESTS
        ).inc()
        record["central_forward_error"] = True

    # *** (중요) 매크로/봇 탐지용 metric 데이터를 detection-log 토픽으로 발행 ***
    # 서울 AI Cloud가 이 토픽을 구독해서 continuous learning에 사용할 예정.
    # 예약 흐름을 막으면 안 되므로 결과를 기다리지 않고 fire-and-forget으로 보낸다.
    asyncio.create_task(
        safe_publish_detection_log(
            {
                "event_type": "seat_request",
                "seat_id": req.seat_id,
                "request_id": request_id,
                "pop_id": POP_ID,
                "pop_arrival_epoch_ms": epoch_ms,
                "client_ip": client_ip,
                "user_agent": user_agent,
                "logged_at": arrival_time.isoformat(),
                # [DETLOG_V2] 서울 AI 학습 재료. 오픈 후 반응 시간은 여기서 계산하지 않고
                # 서울에서 (보정 도착 시각 - 실험별 오픈 시각)으로 구한다.
                "pop_arrival_epoch_us": epoch_us,
                "clock_offset_ms_measured": offset_ms,
                "client_sent_at": req.client_sent_at,
                **_first_layer_payload(request),
                # *** 매크로 탐지의 핵심 특징 ***
                # 같은 account_id 가 여러 좌석을 동시에 노리는지,
                # 같은 device_fp 에 계정이 몇 개 붙어 있는지,
                # 계정을 만든 지 몇 시간 만에 예매를 시도하는지 —
                # 서울 AI Cloud 는 이 세 가지를 이 토픽에서만 볼 수 있다.
                **_identity.as_payload(),  # [POPAUTH_PAYLOAD_V1]
            }
        )
    )

    return record


@app.get("/seat-status/{seat_id}")
async def get_seat_status(seat_id: str):
    """
    *** (중요) 더 이상 central에 HTTP로 물어보지 않는다 ***
    central이 발행한 seat-status 이벤트를 Kafka로 미리 구독해서 채워둔
    로컬 캐시(seat_status_cache)에서 바로 응답한다 — central 왕복 없이 즉시 응답.
    """
    cached = seat_status_cache.get(seat_id)
    if cached is None:
        return {"status": "open", "seat_id": seat_id}

    if cached["status"] == "pending":
        return {
            "status": "pending",
            "seat_id": seat_id,
            "requests_so_far": cached.get("requests_so_far", 1),
        }

    return {
        "status": "confirmed",
        "seat_id": seat_id,
        "winner_request_id": cached["winner_request_id"],
        "winner_pop_id": cached["winner_pop_id"],
        "all_requests": cached.get("all_requests", []),
    }


@app.get("/seats-overview")
async def get_seats_overview():
    """
    좌석 화면을 그릴 때, 지금 대기 중이거나 확정된 좌석들을 한 번에 받아오는 용도.
    central에 물어보지 않고 seat-status 구독으로 채운 로컬 캐시에서 바로 만든다.
    """
    return {seat_id: data["status"] for seat_id, data in seat_status_cache.items()}


@app.get("/seat-count")
async def get_seat_count():
    """
    메인 페이지(index.html)의 '잔여석' 표시용. source of truth는 항상 central의 DB지만,
    매 요청마다 물어보지 않고 1초짜리 캐시를 둔다.

    *** 이 캐시가 없으면 대전에서 심각한 병목이 된다 ***
    화면이 5초마다 이 값을 폴링하므로, 접속자가 1만 명이면 초당 2천 번의
    central 왕복이 발생하고 그게 전부 중계기를 거친다. 캐시를 두면 접속자 수와
    무관하게 초당 최대 1회로 고정된다 (잔여석은 1초 늦어도 무해한 값이다).

    Lock으로 감싸는 이유: 캐시가 만료되는 순간 대기 중이던 요청들이 한꺼번에
    central로 몰려나가는 것(thundering herd)을 막기 위함이다. 하나만 나가고
    나머지는 그 결과를 같이 쓴다.
    """
    now = time.monotonic()
    cached = _seat_count_cache["data"]
    if cached is not None and (now - _seat_count_cache["at"]) < SEAT_COUNT_TTL_SECONDS:
        seat_count_cache_hits.labels(pop_id=POP_ID).inc()
        return cached

    async with _seat_count_lock:
        # 락을 기다리는 동안 다른 요청이 이미 갱신했을 수 있으므로 다시 확인한다.
        now = time.monotonic()
        cached = _seat_count_cache["data"]
        if cached is not None and (now - _seat_count_cache["at"]) < SEAT_COUNT_TTL_SECONDS:
            seat_count_cache_hits.labels(pop_id=POP_ID).inc()
            return cached

        try:
            res = await http_client.get(f"{CENTRAL_URL}/seat-count")
            data = res.json()
            _seat_count_cache["data"] = data
            _seat_count_cache["at"] = time.monotonic()
            seat_count_upstream_calls.labels(pop_id=POP_ID).inc()
            return data
        except httpx.HTTPError:
            # central이 잠깐 안 닿아도 마지막으로 알던 값을 돌려준다.
            # 화면에 빈칸이 뜨는 것보다 1~2초 지난 값을 보여주는 게 낫다.
            if cached is not None:
                return cached
            return {"total": None, "confirmed": None, "pending": None, "remaining": None}


@app.get("/requests")
def get_all_requests():
    return records


@app.get("/requests/{seat_id}")
def get_requests_for_seat(seat_id: str):
    return [r for r in records if r["seat_id"] == seat_id]


@app.delete("/requests")
def clear_requests():
    records.clear()
    return {"status": "cleared"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/clock-offset")
def get_clock_offset():
    """
    지금 이 PoP가 들고 있는 시계 오차를 확인하는 디버그용 엔드포인트.
    양수면 이 PoP의 로컬 시계가 기준보다 느리다는 뜻 (중앙이 판정 시 이 값을 더한다).
    """
    return {
        "pop_id": POP_ID,
        "clock_offset_ms": round(clock_offset_ms, 3),
        "sync_interval_seconds": CLOCK_SYNC_INTERVAL_SECONDS,
    }


@app.post("/booking-entry")
def receive_booking_entry():
    """
    메인 페이지에서 '좌석 예매하기'를 눌렀을 때 호출되는 엔드포인트.
    아직 특정 좌석을 고른 게 아니라 '예매 흐름에 진입했다'는 이벤트이므로,
    좌석 클릭 기록(records)과는 별도로 저장한다.
    """
    arrival_time = datetime.now(timezone.utc)
    record = {
        "entry_id": str(uuid.uuid4()),
        "pop_arrival_timestamp": arrival_time.isoformat(),
        "pop_arrival_epoch_ms": int(arrival_time.timestamp() * 1000),
    }
    entry_records.append(record)
    return record


@app.get("/entries")
def get_all_entries():
    return entry_records


# 정적 파일(정적 UI) 서빙 — 반드시 위의 API 라우트들을 다 정의한 뒤 마지막에 마운트해야
# "/" 하위 정적 파일 처리가 다른 API 라우트를 가리지 않는다.
app.mount("/", StaticFiles(directory="static", html=True), name="static")


# ===========================================================================
# 1차 봇 필터 미들웨어 (2026-08-15 추가)   [BOTFILTER_MARKER_V1]
# ---------------------------------------------------------------------------
# *** 파일 맨 끝에 덧붙인 이유 ***
# 기존 코드를 한 줄도 건드리지 않기 위해서다. Starlette 는 @app.middleware 를
# 모듈 어디에서 등록하든(라우트 정의보다 뒤여도) 정상 동작한다. 좌석 요청
# 핸들러를 수정하지 않으므로, 되돌릴 때는 이 블록만 지우면 된다.
#
# *** 판정 시각에 미치는 영향 ***
# 미들웨어는 핸들러보다 먼저 돌기 때문에, 필터 실행 시간이 좌석 도착
# 타임스탬프에 그대로 얹힌다. 실측 5~10us 수준이고 두 PoP에서 동일한
# 코드가 돌므로 비교에서 상쇄된다. 실제 값은 언제든
# pop_bot_filter_duration_seconds 로 확인할 수 있다.
#
# *** 기본이 monitor 모드인 이유 ***
# 차단을 처음부터 켜면 우리 부하 테스트(curl)가 전부 막혀 성능 측정이
# 불가능해진다. monitor 는 판정만 하고 통과시키므로, 부하 테스트를 그대로
# 돌리면서도 "봇으로 판정된 건수"를 대시보드에서 볼 수 있다.
# 시연에서 차단을 보여줄 때만 BOT_FILTER_MODE=enforce 로 올린다.
# ===========================================================================
import first_layer as _fl
import bot_rules as _br
from fastapi.responses import JSONResponse as _BFJSONResponse

active_rules = _br.EMPTY  # kafka_consume_rules_loop 가 교체한다 [BOTRULES_V1]


@app.middleware("http")
async def _bot_filter_middleware(request, call_next):
    if _fl.MODE == "off" or request.url.path in _fl.EXEMPT_PATHS:
        return await call_next(request)

    ip = _fl.client_ip_of(request)
    headers = {k.lower(): v for k, v in request.headers.items()}
    verdict = _fl.first_layer_filter(ip, headers, pop_id=POP_ID)

    # 서울 규칙은 좌석 요청에만 돌린다. 학습 재료(detection-log)가 좌석 요청이라
    # 다른 경로에 돌리면 검증한 적 없는 곳에서 차단하게 된다.  [BOTRULES_V1]
    if verdict.passed and active_rules["rules"] and request.url.path == "/seat-request":
        identity = getattr(request.state, "identity", None) or _pa.ANONYMOUS
        rule_id = _br.match(active_rules, {
            "client_ip": ip, "headers": _fl.header_snapshot(request), **identity.as_payload()})
        if rule_id:
            verdict = _fl.FilterResult(passed=False, reason=f"rule:{rule_id}")
            _fl.bot_flagged.labels(pop_id=POP_ID, reason=verdict.reason).inc()

    if (not verdict.passed) and _fl.MODE == "enforce":
        _fl.bot_blocked.labels(pop_id=POP_ID, reason=verdict.reason).inc()
        # 429 는 "속도를 줄여라", 403 은 "너는 안 된다".
        # 폴링 봇에게는 429 를 주는 편이 의미가 맞고, 정상 사용자가
        # 오탐된 경우에도 잠시 뒤 재시도하면 풀린다는 신호가 된다.
        code = 429 if verdict.reason == "rate_limit" else 403
        # 막힌 좌석 요청은 핸들러까지 안 가서 detection-log 에 안 남는다.
        # "1차가 무엇을 막았나"를 서울이 알 수 있게 여기서 한 줄 남긴다.
        # 좌석 요청만 남긴다. 폴링까지 남기면 봇 한 대가 Kafka 를 채운다.  [DETLOG_V2]
        if request.url.path == "/seat-request":
            request.state.bot_verdict = verdict
            await _log_first_layer_block(request, ip)
        return _BFJSONResponse(status_code=code, content={
            "error": "blocked_by_first_layer",
            "reason": verdict.reason,
            "layer": 1,
        })

    # 통과시킨 요청에도 판정 결과를 붙여둔다.
    # 나중에 detection-log 로 서울 AI Cloud 에 보낼 때 학습 라벨로 쓴다.
    request.state.bot_verdict = verdict
    return await call_next(request)


def _first_layer_payload(request) -> dict:
    """detection-log 에 붙일 1차 판정 결과와 헤더.  [DETLOG_V2]
    first_layer_passed 가 None 이면 필터가 꺼져 있어 판정하지 않은 것이다."""
    verdict = getattr(request.state, "bot_verdict", None)
    return {
        "ruleset_version": _fl.RULESET_VERSION,
        "first_layer_mode": _fl.MODE,
        "first_layer_passed": verdict.passed if verdict else None,
        "first_layer_reason": (verdict.reason or None) if verdict else None,
        "headers": _fl.header_snapshot(request),
    }


async def _log_first_layer_block(request, ip: str):
    """enforce 모드에서 막은 좌석 요청을 detection-log 에 남긴다.  [DETLOG_V2]"""
    edge_ns = getattr(request.state, "edge_arrival_ns", None) or time.time_ns()
    identity = getattr(request.state, "identity", None) or _pa.ANONYMOUS
    asyncio.create_task(safe_publish_detection_log({
        "event_type": "first_layer_block",
        "seat_id": None,  # 막힌 요청은 본문을 읽지 않는다
        "request_id": str(uuid.uuid4()),
        "pop_id": POP_ID,
        "pop_arrival_epoch_ms": edge_ns // 1_000_000,
        "pop_arrival_epoch_us": edge_ns // 1_000,
        "clock_offset_ms_measured": round(clock_offset_ms, 3),
        "client_ip": ip,
        "user_agent": request.headers.get("user-agent", "unknown"),
        "logged_at": datetime.fromtimestamp(edge_ns / 1e9, timezone.utc).isoformat(),
        **identity.as_payload(),
        **_first_layer_payload(request),
    }))


@app.get("/bot-filter")
async def _bot_filter_status():
    """1차 필터 상태 점검용: 현재 모드, 추적 중인 IP 수, 차단 목록 샘플, 서울 규칙."""
    return {**_fl.stats(), "ruleset_version": _fl.RULESET_VERSION, "rules": active_rules["rules"]}


# ---------------------------------------------------------------------------
# 라우트 순서 교정  [BOTFILTER_ROUTE_FIX_V1]
# app.mount("/", StaticFiles(...)) 가 "/" 이하 경로 전부를 잡기 때문에,
# 그보다 뒤에 등록된 라우트는 절대 도달하지 못하고 정적 파일 404 가 대신 난다.
# 우리 /bot-filter 는 파일 맨 끝에 덧붙여져 mount 보다 뒤에 있으므로,
# 라우트 목록의 맨 앞으로 옮겨 mount 보다 먼저 매칭되게 한다.
# (미들웨어는 라우팅과 무관하게 동작하므로 필터 자체는 영향받지 않는다.)
for _bf_i, _bf_r in enumerate(app.routes):
    if getattr(_bf_r, "path", None) == "/bot-filter":
        app.routes.insert(0, app.routes.pop(_bf_i))
        break


# ===========================================================================
# 신원 확인 미들웨어  [POPAUTH_MARKER_V1]
# ---------------------------------------------------------------------------
# 이 블록은 파일 끝에 "덧붙이기만" 한다. 위쪽 기존 코드는 수정하지 않는다.
# ===========================================================================
import asyncio as _pa_asyncio
import time as _pa_time
import pop_auth as _pa
from fastapi.responses import JSONResponse as _PAJSONResponse

# 백그라운드 태스크 참조 보관소.
# asyncio.create_task() 의 반환값을 아무도 안 들고 있으면 가비지 컬렉터가
# 실행 도중에 태스크를 수거해버릴 수 있다(문서에 명시된 함정).
# 여기에 담아두고 끝나면 스스로 빠지게 한다.
_pa_tasks: set = set()


def _pa_spawn(coro):
    t = _pa_asyncio.create_task(coro)
    _pa_tasks.add(t)
    t.add_done_callback(_pa_tasks.discard)
    return t


@app.middleware("http")
async def _auth_middleware(request, call_next):
    # -----------------------------------------------------------------
    # (1) 무엇보다 먼저 도착 시각을 찍는다.
    #
    # *** 이 순서가 이 프로젝트의 핵심이다 ***
    # 토큰 검증에 약 69us 가 걸린다. 검증을 먼저 하고 시각을 재면
    # "로그인한 사용자만" 69us 늦게 접수된다. 로그인한 사람이 손해를 보는
    # 구조가 되면 공정 티켓팅이라는 주제 자체가 무너진다.
    # 그래서 ASGI 스택에서 가장 이른 지점인 여기서 시각을 확정하고,
    # 검증은 그 뒤에 한다. 검증 비용은 접수 시각에 영향을 주지 않는다.
    # -----------------------------------------------------------------
    request.state.edge_arrival_ns = _pa_time.time_ns()

    path = request.url.path
    if _pa.MODE == "off" or path in _pa.EXEMPT_PATHS:
        request.state.identity = _pa.ANONYMOUS
        return await call_next(request)

    # (2) 공개키가 없으면 백그라운드로 받아온다.
    #     여기서 await 로 기다리면 첫 요청이 중앙 왕복만큼 지연된다.
    #     지금 요청은 그냥 익명으로 처리하고, 다음 요청부터 검증이 붙는다.
    if not _pa.has_public_key():
        _pa_asyncio.create_task(_pa.ensure_public_key(POP_ID))

    token = _pa.extract_token({k.lower(): v for k, v in request.headers.items()})
    identity = _pa.verify(token, POP_ID)
    request.state.identity = identity

    # (3) required 모드에서만 차단한다.
    #     기본값은 optional 이라 토큰 없는 curl(부하 테스트)도 그대로 통과한다.
    if _pa.MODE == "required" and not identity.verified:
        return _PAJSONResponse(
            status_code=401,
            content={
                "detail": "로그인이 필요합니다",
                "reason": identity.reason,
                "pop_id": POP_ID,
            },
        )

    if identity.verified:
        _pa.auth_identified.labels(pop_id=POP_ID).inc()

    return await call_next(request)


@app.get("/auth-status")
async def _auth_status():
    """팀원이 브라우저에서 바로 확인할 수 있는 진단용 엔드포인트."""
    s = _pa.status()
    s["pop_id"] = POP_ID
    return s


# ---------------------------------------------------------------------------
# 라우트 순서 교정
# ---------------------------------------------------------------------------
# app.mount("/", StaticFiles(...)) 가 파일 위쪽에서 이미 등록되어 있으면,
# 그 뒤에 붙은 라우트는 전부 정적 파일 처리기에 먹혀 404 가 된다.
# (bot-filter 때 똑같이 당했다.) 그래서 방금 만든 라우트를 맨 앞으로 옮긴다.
for _i, _r in enumerate(list(app.routes)):
    if getattr(_r, "path", None) == "/auth-status" and _i != 0:
        app.routes.insert(0, app.routes.pop(_i))
        break

print(f"[pop-auth] 신원 확인 미들웨어 등록 완료 (mode={_pa.MODE})")


# ===========================================================================
# 워커 여러 개일 때 지표 합산  [WORKERS_V1]
# ---------------------------------------------------------------------------
# prometheus_client 는 프로세스마다 카운터를 따로 들고 있다.
# 워커가 4개면 Prometheus 가 /metrics 를 긁을 때마다 그 중 한 워커의 숫자만
# 받게 되어, 요청 처리율 그래프가 1/4 로 줄었다 늘었다 요동친다.
#
# PROMETHEUS_MULTIPROC_DIR 이 설정되면 각 워커가 그 디렉터리에 지표를 기록하고,
# 아래 엔드포인트가 전부 읽어 합산해서 내보낸다.
# 환경변수가 없으면 기존 동작 그대로다.
# ===========================================================================
import os as _w_os
from fastapi import Response as _WResponse

_W_DIR = _w_os.environ.get("PROMETHEUS_MULTIPROC_DIR", "")

if _W_DIR:
    _w_os.makedirs(_W_DIR, exist_ok=True)
    from prometheus_client import CollectorRegistry as _WReg, generate_latest as _wgen
    from prometheus_client import multiprocess as _wmp
    from prometheus_client import CONTENT_TYPE_LATEST as _WCT

    import re as _wre

    def _strip_pid(text):
        """
        Gauge 에 붙은 pid 라벨을 걷어낸다.  [STRIPPID_V1]

        워커가 4개면 prometheus_client 는 Gauge 를 프로세스별로 따로 내보낸다.
        그래서 pop_clock_offset_ms 하나가 pid=10,11,12,13 네 줄이 되고,
        단일 시계열을 기대하는 대시보드 패널이 값을 못 찾는다.
        (Counter 는 자동으로 합산되므로 영향이 없다.)

        워커들은 같은 값을 보고하므로 첫 줄만 남기고 나머지는 버린다.
        """
        out, seen = [], set()
        for line in text.splitlines():
            if not line or line.startswith("#"):
                out.append(line); continue
            if 'pid="' in line:
                l = _wre.sub(r'pid="[^"]*",?\s*', '', line)
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
    async def _metrics_multiproc():
        reg = _WReg()
        _wmp.MultiProcessCollector(reg)
        return _WResponse(content=_strip_pid(_wgen(reg).decode()), media_type=_WCT)

    for _i, _r in enumerate(list(app.routes)):
        if getattr(_r, "endpoint", None) is _metrics_multiproc:
            if _i != 0:
                app.routes.insert(0, app.routes.pop(_i))
            break

    print(f"[{POP_ID}] 지표 합산 모드 (PROMETHEUS_MULTIPROC_DIR={_W_DIR})")


# ===========================================================================
# Kafka 발행 결과 집계  [NOWAIT_V1]
# ---------------------------------------------------------------------------
# send() 는 버퍼에 넣고 바로 돌아오므로 실제 성공/실패는 나중에 알게 된다.
# 이 콜백이 그 결과를 지표에 반영한다. 카운터 이름과 라벨이 이전과 같으므로
# 대시보드 패널은 손댈 필요가 없다.
#
# 여기서 예외를 밖으로 던지면 안 된다. 콜백은 이벤트 루프가 호출하므로
# 예외가 새면 원인을 찾기 어려운 로그만 쌓인다.
# ===========================================================================
def _seat_publish_done(_fut):
    try:
        _fut.result()
    except Exception:
        try:
            pop_kafka_publish_errors_total.labels(
                pop_id=POP_ID, topic=KAFKA_TOPIC_SEAT_REQUESTS
            ).inc()
        except Exception:
            pass
        return
    try:
        pop_kafka_published_total.labels(
            pop_id=POP_ID, topic=KAFKA_TOPIC_SEAT_REQUESTS
        ).inc()
    except Exception:
        pass


# ===========================================================================
# 전달 지연 측정 발신  [HEARTBEAT_V1]
# ---------------------------------------------------------------------------
# 좌석 요청과 무관하게 주기적으로 자기 시각을 발행한다.
# 중앙이 이를 받은 시각과 비교해 PoP -> 중앙 전달 지연을 계산한다.
#
# 좌석 요청으로만 재면 두 가지 문제가 있다.
#   1) 오픈 직전 유휴 상태에는 요청이 없어 측정할 값이 없다
#   2) 부하 중 값은 큐잉 지연이 섞여 순수한 망 전달 시간이 아니다
#
# 두 서버 시계가 이미 정렬되어 있으므로 이 뺄셈이 의미를 갖는다.
# ===========================================================================
HEARTBEAT_TOPIC = os.environ.get("KAFKA_TOPIC_HEARTBEAT", "pop-heartbeat")
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL_SEC", "5"))


async def heartbeat_send_loop():
    await asyncio.sleep(3)
    while True:
        try:
            await kafka_producer.send(
                HEARTBEAT_TOPIC,
                key=POP_ID,
                value={
                    "pop_id": POP_ID,
                    "sent_epoch_ms": time.time_ns() // 1_000_000,
                    "clock_offset_ms": round(clock_offset_ms, 4),
                },
            )
        except asyncio.CancelledError:
            return
        except Exception as _hb_e:
            print(f"[{POP_ID}] heartbeat 발행 실패: {type(_hb_e).__name__}: {_hb_e}", flush=True)
        try:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
        except asyncio.CancelledError:
            return
