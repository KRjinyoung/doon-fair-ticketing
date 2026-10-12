"""
================================================================
 1차 봇 필터 (first_layer.py)  ―  PoP 엣지 노드에서 실행
================================================================

[원본]
  팀원이 작성한 first_layer.py 를 우리 운영 환경에 맞춰 고친 것.
  판단 로직(rate limit, 헤더 정합성)의 뼈대는 그대로 두고,
  실제로 돌렸을 때 문제가 되는 부분만 손봤다.

[원본에서 바꾼 것 4가지]

 ① 데이터센터 IP 대역 검사를 제거했다.
    원본은 IP 앞 2자리 문자열("13.", "34.", "35.", "52.")로 클라우드를
    판별했는데, 이건 CIDR이 아니라서 같은 앞자리의 무관한 대역까지
    통째로 걸린다. 게다가 모바일 캐리어 NAT나 기업 프록시를 쓰는
    실사용자가 오탐된다. 1차의 대원칙("애매하면 통과")에 정면으로
    어긋나므로 뺐다.

 ② MODE 를 도입했다 (off / monitor / enforce).
    차단 로직을 처음부터 켜면 우리 부하 테스트(curl)가 전부 막혀서
    성능 측정 자체가 불가능해진다. 실무에서 WAF를 도입할 때와 같이
    "탐지만 하고 통과시키는" monitor 를 기본값으로 두고, 시연 때만
    enforce 로 올린다. 차단 로직에 버그가 있어도 서비스가 안 죽는다.

 ③ 메모리 누수를 막았다.
    원본은 _req_log 에 IP별 deque 를 만들기만 하고 지우지 않으며,
    BLOCKED_IPS 도 영구 차단이다. 부하 테스트 한 번이면 수천 개의
    빈 deque 가 남고, 한 번 과속한 IP는 영원히 차단된다.
    유휴 IP 정리(기본 10분)와 차단 만료(기본 1시간)를 넣었다.

 ④ 관측 지표를 넣었다.
    monitor 모드는 "탐지했지만 통과시킨다"는 뜻이라, 지표가 없으면
    아무 일도 안 일어난 것과 구분이 안 된다. 무엇을 몇 건 걸렀는지
    Prometheus 로 내보낸다.

[시각 정밀도에 대한 메모 ― 중요]
  이 필터는 좌석 요청의 타임스탬프가 찍히기 직전에 실행된다.
  따라서 필터의 실행 시간이 판정 시각에 그대로 얹힌다.
  측정해보면 5~10us 수준이고, 두 PoP에서 같은 코드가 도는 이상
  이 비용은 양쪽에 동일하게 붙어 비교에서 상쇄된다.
  (우리 시계 오차가 6~30us, 승부 마진이 ms 단위이므로 무시할 수 있다.)
  다만 요청량이 많은 쪽이 deque 정리를 더 많이 하므로 완전한 대칭은
  아니다. pop_bot_filter_duration_seconds 로 항상 관측 가능하게 해뒀다.

[상태 주의점]
  BLOCKED_IPS 와 _req_log 는 프로세스 메모리에 있다.
  uvicorn 을 워커 여러 개로 띄우면 각 워커가 자기 카운터를 가져서
  rate limit 이 워커 수만큼 느슨해진다. 반드시 단일 프로세스로 실행할 것.
================================================================
"""

import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field

from prometheus_client import Counter, Gauge, Histogram


# ================================================================
#  동작 모드
# ================================================================
#   off      필터를 아예 돌리지 않는다 (오버헤드 0)
#   monitor  판정은 하되 차단하지 않는다. 지표에만 기록.  ← 기본값
#   enforce  실제로 차단한다 (403 / 429)
MODE = os.environ.get("BOT_FILTER_MODE", "monitor").strip().lower()
if MODE not in ("off", "monitor", "enforce"):
    MODE = "monitor"

# rate limit 튜닝값. 정상 사용자는 초당 1~2회 수준이고,
# 취소표 폴링 봇은 초당 수십~수백 회라서 이 사이에 선을 긋는다.
WINDOW_SEC = float(os.environ.get("BOT_RATE_WINDOW_SEC", "10"))
MAX_REQUESTS = int(os.environ.get("BOT_RATE_MAX", "30"))

# 한 번 걸린 IP를 얼마나 오래 차단할지. 영구 차단은 오탐이 났을 때
# 복구 수단이 없으므로 반드시 만료를 둔다.
BLOCK_TTL_SEC = float(os.environ.get("BOT_BLOCK_TTL_SEC", "3600"))

# 이 시간 동안 요청이 없는 IP의 기록은 버린다 (메모리 회수).
IDLE_TTL_SEC = float(os.environ.get("BOT_IDLE_TTL_SEC", "600"))

# 합성 트래픽(부하 테스트) 우회 키. 이 헤더 값이 맞으면 필터를 건너뛴다.
# 빈 문자열이면 기능 자체가 꺼진다(기본값).
#   요청 헤더:  X-Synthetic-Key: <값>
# enforce 모드에서 "정상 사용자 트래픽"과 "봇 트래픽"을 같은 스크립트로
# 동시에 만들어 대시보드에서 갈리는 걸 보여줄 때 쓴다.
SYNTHETIC_KEY = os.environ.get("LOADTEST_KEY", "").strip()

# 이 경로들은 필터를 타지 않는다.
#   /metrics     Prometheus 가 1초마다 긁는다. Accept-Language 가 없어서
#                필터를 태우면 우리 모니터링이 봇으로 잡힌다.
#   /health      헬스체크
#   /clock-offset  운영 점검용
EXEMPT_PATHS = frozenset({"/metrics", "/health", "/clock-offset"})

# User-Agent 에 이 단어가 있으면 스크립트로 본다.
# (원본 유지. curl/python 이 포함되어 있으므로 monitor 모드에서는
#  우리 부하 테스트가 "flagged" 로 집계되지만 통과는 된다.)
BOT_UA_TOKENS = ("python", "curl", "wget", "go-http",
                 "java", "scrapy", "headless", "bot")


# ================================================================
#  결과 타입
# ================================================================
@dataclass
class FilterResult:
    passed: bool                                   # True=통과, False=봇으로 판정
    reason: str = ""                               # 판정 사유
    signals: dict = field(default_factory=dict)    # 2차/3차로 넘길 관찰값


# ================================================================
#  상태 저장소
# ================================================================
_req_log: dict = defaultdict(deque)   # IP -> 최근 요청 시각들 (슬라이딩 윈도우)
_blocked: dict = {}                   # IP -> 차단 만료 시각(epoch)
_last_sweep = 0.0                     # 마지막 청소 시각


# ================================================================
#  지표
# ================================================================
bot_evaluated = Counter(
    "pop_bot_evaluated_total",
    "1차 필터를 통과시킨 검사 건수 (면제 경로 제외)",
    ["pop_id"],
)
bot_flagged = Counter(
    "pop_bot_flagged_total",
    "봇으로 판정된 건수. monitor 모드에서도 올라간다(통과는 시킴)",
    ["pop_id", "reason"],
)
bot_blocked = Counter(
    "pop_bot_blocked_total",
    "실제로 차단한 건수. enforce 모드에서만 올라간다",
    ["pop_id", "reason"],
)
bot_synthetic = Counter(
    "pop_bot_synthetic_total",
    "우회 키로 필터를 건너뛴 건수 (합성 트래픽)",
    ["pop_id"],
)
bot_tracked_ips = Gauge(
    "pop_bot_tracked_ips",
    "rate limit 창에 기록이 남아 있는 IP 수 (메모리 사용량 대리 지표)",
    ["pop_id"],
)
bot_blocked_ips = Gauge(
    "pop_bot_blocked_ips",
    "현재 차단 목록에 올라 있는 IP 수",
    ["pop_id"],
)
bot_filter_duration = Histogram(
    "pop_bot_filter_duration_seconds",
    "1차 필터 자체의 실행 시간. 이 값이 좌석 판정 시각에 그대로 얹히므로 관측한다",
    ["pop_id"],
    buckets=(1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3),
)


# ================================================================
#  클라이언트 IP 추출  ― 원본에 없던, 그러나 반드시 필요한 부분
# ================================================================
def client_ip_of(request) -> str:
    """
    실제 클라이언트 IP를 뽑는다.

    *** 이걸 안 하면 필터가 통째로 무의미해진다 ***
    우리 PoP는 nginx 뒤에 있어서 request.client.host 는 항상 도커
    브리지 게이트웨이 IP(172.x.x.1) 하나로만 보인다. 그 상태로
    rate limit 을 걸면 전체 사용자가 한 덩어리로 세어져서, 정상
    사용자 30명만 넘어도 서비스 전체가 차단된다.

    *** 왜 마지막 값을 쓰는가 ***
    nginx 는 $proxy_add_x_forwarded_for 로 헤더를 만든다. 이건
    "클라이언트가 보낸 XFF, nginx 가 본 실제 peer IP" 형태다.
    앞쪽 값은 클라이언트가 마음대로 위조할 수 있고, 맨 뒤 값만
    우리 nginx 가 직접 관찰해서 붙인 신뢰할 수 있는 값이다.
    """
    xff = request.headers.get("x-forwarded-for", "")
    if xff:
        parts = [p.strip() for p in xff.split(",") if p.strip()]
        if parts:
            return parts[-1]
    client = getattr(request, "client", None)
    return client.host if client and client.host else "unknown"


# ================================================================
#  청소 ― 메모리 회수 (원본에 없던 부분)
# ================================================================
def _sweep(now: float, pop_id: str):
    """
    유휴 IP 기록과 만료된 차단을 걷어낸다.

    async 태스크가 아니라 호출 중에 가끔 도는 방식(lazy sweep)을 쓴 이유:
    별도 백그라운드 루프를 만들면 lifespan 을 건드려야 해서 통합 지점이
    늘어난다. 여기서는 60초에 한 번, 요청 처리 흐름 안에서 처리한다.
    삭제 대상이 없으면 dict 순회 비용만 들고 끝난다.
    """
    global _last_sweep
    _last_sweep = now

    dead = [ip for ip, dq in _req_log.items()
            if not dq or (now - dq[-1]) > IDLE_TTL_SEC]
    for ip in dead:
        del _req_log[ip]

    expired = [ip for ip, until in _blocked.items() if until <= now]
    for ip in expired:
        del _blocked[ip]

    bot_tracked_ips.labels(pop_id=pop_id).set(len(_req_log))
    bot_blocked_ips.labels(pop_id=pop_id).set(len(_blocked))


# ================================================================
#  검사 ①: 차단 목록 (만료 있음)
# ================================================================
def check_blocklist(ip: str, now: float) -> bool:
    """True=정상, False=차단 목록에 있음"""
    until = _blocked.get(ip)
    if until is None:
        return True
    if until <= now:
        del _blocked[ip]     # 만료됐으면 풀어준다
        return True
    return False


# ================================================================
#  검사 ②: 요청 빈도  ― 취소표 폴링 봇 대응
# ================================================================
def check_rate_limit(ip: str, now: float) -> bool:
    """
    최근 WINDOW_SEC 초 안의 요청 수가 MAX_REQUESTS 를 넘으면 봇으로 본다.

    deque(양방향 큐)를 쓰는 이유는 앞에서 빼고 뒤에서 넣는 연산이
    O(1) 이기 때문이다. 리스트로 하면 앞에서 뺄 때마다 전체가 밀린다.
    """
    dq = _req_log[ip]
    cutoff = now - WINDOW_SEC
    while dq and dq[0] < cutoff:
        dq.popleft()
    dq.append(now)
    if len(dq) > MAX_REQUESTS:
        _blocked[ip] = now + BLOCK_TTL_SEC
        return False
    return True


# ================================================================
#  검사 ③: 헤더 정합성  ― 단순 스크립트 봇 대응
# ================================================================
def check_headers(headers: dict):
    """
    반환: (통과여부, 신호 dict). headers 는 키가 소문자인 dict 를 가정한다.

    [한계 ― 정직하게]
    User-Agent 는 위조가 쉽다. 정교한 봇은 크롬 UA를 그대로 흉내 낸다.
    진짜 강력한 신호는 TLS 지문(JA3/JA4)인데 핸드셰이크를 직접 파싱해야
    해서 무겁다. 여기서는 게이트웨이가 넣어준 값이 있으면 기록만 한다.
    """
    ua = headers.get("user-agent", "").lower()
    signals = {"ua": ua[:80]}

    if not ua:
        return False, signals                      # 정상 브라우저는 항상 보낸다
    if any(tok in ua for tok in BOT_UA_TOKENS):
        return False, signals
    if "accept-language" not in headers:
        return False, signals                      # 브라우저는 언어 설정을 함께 보낸다

    signals["tls_fp"] = headers.get("x-tls-fingerprint", "unknown")
    return True, signals


# ================================================================
#  메인 진입점
# ================================================================
def first_layer_filter(ip: str, headers: dict, pop_id: str = "unknown") -> FilterResult:
    """
    검사를 싼 것부터 순서대로 돌린다: 차단목록 -> 빈도 -> 헤더.
    하나라도 걸리면 즉시 반환하므로 뒤 검사는 생략된다.

    반환값의 passed 는 "판정 결과"이지 "차단 여부"가 아니다.
    실제 차단 여부는 호출하는 쪽이 MODE 를 보고 결정한다.
    """
    t0 = time.perf_counter()
    now = time.time()

    if SYNTHETIC_KEY and headers.get("x-synthetic-key", "") == SYNTHETIC_KEY:
        bot_synthetic.labels(pop_id=pop_id).inc()
        bot_filter_duration.labels(pop_id=pop_id).observe(time.perf_counter() - t0)
        return FilterResult(passed=True, signals={"synthetic": True})

    if now - _last_sweep > 60.0:
        _sweep(now, pop_id)

    bot_evaluated.labels(pop_id=pop_id).inc()

    result = None
    if not check_blocklist(ip, now):
        result = FilterResult(passed=False, reason="blocked_ip")
    elif not check_rate_limit(ip, now):
        result = FilterResult(passed=False, reason="rate_limit")
    else:
        ok, signals = check_headers(headers)
        if not ok:
            result = FilterResult(passed=False, reason="abnormal_headers", signals=signals)
        else:
            signals["first_layer"] = "passed"
            signals["pop_ts"] = now
            result = FilterResult(passed=True, signals=signals)

    if not result.passed:
        bot_flagged.labels(pop_id=pop_id, reason=result.reason).inc()

    bot_filter_duration.labels(pop_id=pop_id).observe(time.perf_counter() - t0)
    return result


def stats() -> dict:
    """운영 점검용 요약."""
    now = time.time()
    return {
        "mode": MODE,
        "window_sec": WINDOW_SEC,
        "max_requests": MAX_REQUESTS,
        "tracked_ips": len(_req_log),
        "blocked_ips": len(_blocked),
        "blocked_sample": [
            {"ip": ip, "expires_in_sec": round(until - now, 1)}
            for ip, until in list(_blocked.items())[:10]
        ],
        "synthetic_key_enabled": bool(SYNTHETIC_KEY),
        "exempt_paths": sorted(EXEMPT_PATHS),
    }
