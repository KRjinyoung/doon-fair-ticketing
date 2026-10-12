# DOON 티켓팅 시스템 — 현재 구성

공정 티켓팅 데모. 여러 지역 PoP(엣지)가 좌석 요청의 **도착 시각**만 찍어 중앙으로 넘기고,
중앙이 **클럭 오프셋으로 보정한 시각**으로 승자를 판정한다.

---

## 1. 구성 (판교 호스트 · `docker-compose.pangyo.yml`)

| 컨테이너 | 이미지/빌드 | 외부 포트 | 역할 |
|---|---|---|---|
| `nginx` | nginx:alpine | **13198** | 사용자 진입점. 정적 파일 직접 서빙 + API 프록시 |
| `pop-pangyo` | `./pop` (uvicorn, 워커 4) | (내부 8000) | 엣지. 도착 시각 각인, Kafka 발행, 좌석 상태 캐시 |
| `central` | `./central` (uvicorn) | 10001 | 판정 서버 + 인증 서버 |
| `kafka` | apache/kafka:3.7.0 (KRaft) | 13092(EXTERNAL) | 토픽 3종 |
| `postgres` | postgres:16-alpine | (내부) | 확정 좌석 · 계정 · 세션 · 서명키 |
| `prometheus` | v2.51.2 | 13090 | 1초 주기 scrape |
| `grafana` | 10.4.2 | 13300 | 대시보드(익명 접속, 1초 갱신) |
| `pushgateway` | v1.8.0 | 13091 | 대전 PoP 지표 수신 창구 |
| `kafka-exporter` / `postgres-exporter` | — | (내부) | Kafka lag / DB 부하 지표 |

대전 PoP는 별도 호스트에서 `docker-compose.daejeon.yml`(이 저장소 밖)로 뜨며,
호스트의 socat 중계기를 거쳐 판교의 Kafka(13092)와 pushgateway(13091)에 **대전이 먼저 연결하는 방향**으로만 붙는다.
그래서 Kafka EXTERNAL 광고 주소가 IP가 아니라 `kafka-external` 이름이고, 대전 compose가 `extra_hosts`로 그 이름을 자기 호스트에 매핑한다.

**Kafka 토픽**
- `seat-requests` : PoP → central (좌석 요청, key=seat_id)
- `seat-status` : central → 모든 PoP (pending/confirmed 전파)
- `detection-log` : PoP → 서울 AI Cloud (봇 학습용 원본 이벤트)

---

## 2. nginx 라우팅 (13198)

```
/auth/...                      → central:8080   (로그인/가입, no-store, timeout 30s)
/seat-request /seat-status /seats-overview /seat-count
/booking-entry /requests /entries /health
/clock-offset /bot-filter /auth-status /metrics
                               → upstream doon_pop (pop-pangyo:8000, keepalive 64)
/img/ *.css *.js /             → nginx가 디스크에서 직접 응답 (PoP까지 안 감)
```
프록시 경로에는 `X-Forwarded-For`(= 기존 XFF + 실제 peer IP)를 붙인다. **XFF의 마지막 값**만 신뢰해서
봇 필터의 rate limit 키와 `sessions.login_ip`로 쓴다. 판정 경로는 캐시 금지.

---

## 3. 사용자 흐름

### 3-1. 로그인 (`login.html`)
1. 브라우저가 화면/캔버스/언어 등 신호를 모아 SHA-256으로 **device_fp(16자)** 생성.
2. `POST /auth/login {email, password, device_fp}` → nginx → central.
3. central: `auth_conn()`(인증 전용 풀)로 users 조회 → **bcrypt 검증은 `asyncio.to_thread`**(이벤트 루프 정지 방지)
   → `sessions`에 session_id/device_fp/login_ip 기록.
4. **RS256 JWT** 발급. 클레임 = `sub`(계정), `sid`(세션), `dfp`(기기지문), `age`(계정 나이 일수), `exp`(기본 120분).
   키쌍은 `auth_keys` 테이블에 저장 → central 재시작해도 기존 토큰이 살아 있다.
5. 브라우저는 토큰을 localStorage에 보관하고 `returnTo`로 복귀.

### 3-2. 좌석 예매 (`index.html` → `seat.html`)
`index.html`은 5초마다 `/seat-count`로 잔여석만 갱신하고, 예매 버튼은 토큰이 없으면 로그인으로 보낸다.
`seat.html`은 608석(A~J, 8행)을 그리고 1.5초마다 `/seats-overview`, 3초마다 `/seat-count`, 5초마다 `/clock-offset`을 폴링한다.

**예약하기 클릭 → `POST /seat-request {seat_id}` (있으면 `Authorization: Bearer <token>`)**

PoP 안에서의 처리 순서 (미들웨어는 바깥쪽부터):

```
① 인증 미들웨어   ├─ 맨 먼저 request.state.edge_arrival_ns = time.time_ns()   ← 판정에 쓰이는 도착 시각
                  ├─ 공개키 없으면 백그라운드로 central /auth/public-key 조회 (요청은 안 기다림)
                  └─ 로컬 RS256 검증(약 62us) → Identity(account_id/sid/dfp/age/verified)
                     MODE=optional 이므로 토큰이 없어도 익명으로 통과
② 봇 필터        └─ 차단목록 → rate limit(10초 30건) → 헤더 정합성. MODE=monitor 라 판정만 하고 통과
③ 핸들러         ├─ 미들웨어가 찍은 시각을 그대로 쓴다 (검증 비용이 접수 시각에 섞이지 않게)
                  ├─ 지금 시점의 clock_offset_ms를 함께 읽어
                  ├─ seat-requests 발행 (key=seat_id, 원본시각 + 오프셋 + 신원). send()는 결과를 안 기다리고
                  │  콜백으로 성공/실패 카운터만 올린다
                  ├─ detection-log 발행 (fire-and-forget: client_ip, user_agent, 신원)
                  └─ 응답: request_id, pop_id, 도착 시각
```

시각 각인이 **모든 검증보다 앞**에 있는 것이 핵심이다. 검증을 먼저 하면 로그인한 사용자만 늦게 접수된다.

### 3-3. 중앙 판정
1. `kafka_consume_loop`(group `central-judge-group`)가 `seat-requests`를 소비.
2. `apply_seat_request()`
   - 이미 확정된 좌석이면 `already_confirmed`로 즉시 거절.
   - **`corrected_epoch_ms = pop_arrival_epoch_ms + clock_offset_ms_measured`** ← 보정이 일어나는 유일한 지점.
   - 해당 좌석의 첫 요청이면 **수집창(`WINDOW_SECONDS = 5초`)** 을 열고 타이머를 건다. 아니면 버퍼에 추가.
   - `seat-status`에 `pending {requests_so_far}` 발행 (휘발성이라 DB와 무관하게 즉시).
3. 5초 후 `close_window_after_delay()`
   - `corrected_epoch_ms` 오름차순 정렬 → **1등이 승자**. 1·2등 격차를 `winner_margin_ms`로 기록.
   - **Postgres `confirmed_seats` INSERT(ON CONFLICT UPDATE, 최대 3회 재시도)** 가 먼저.
   - **커밋 성공한 뒤에만** `seat-status`에 `confirmed {winner_request_id, winner_pop_id, winner_timestamp_ms, all_requests(최대 20)}` 발행.
     실패하면 그대로 return — "DB에 없는 확정 이벤트"가 나가지 않게 하는 불변식.

### 3-4. 결과 전파와 결제
- 각 PoP는 `seat-status`를 **워커 프로세스마다 고유 group_id**(`pop-status-cache-{POP_ID}-{pid}`, `earliest`)로 구독해
  `seat_status_cache`를 채운다. 그래서 `/seat-status/{id}`와 `/seats-overview`는 central 왕복 없이 로컬 캐시로 답한다.
- 브라우저는 예약 직후 0.5초 간격으로 `/seat-status/{seat_id}`를 폴링:
  `pending`이면 경쟁자 수 표시, `confirmed`면 `winner_request_id == 내 request_id` 비교로 승패 판정.
- 승리 시 `payment.html?seat_id=&request_id=`로 이동. 결제는 화면상 시뮬레이션(등급/가격 표시 + 예매번호 생성)이며 서버 연동은 없다.
- `/seat-count`만 central까지 간다. PoP가 **1초 TTL 캐시 + Lock**으로 묶어 접속자 수와 무관하게 초당 최대 1회, central은 DB를 COUNT해서 답한다.

---

## 4. 시각 동기화 흐름 (공정성의 근거)

```
외부 NTP  ──chrony──▶ 각 PoP 호스트
                       └ pop-clock-offset.service (5초마다)
                           chronyc -c tracking → /run/pop-clock/tracking.csv (임시파일→mv, 원자적)
                       └ 컨테이너가 /clock:ro 로 마운트
PoP: 5초마다 CSV 5번째 필드(System time, 초) × 1000 → clock_offset_ms
     자기 시계는 절대 고치지 않고, 원본 시각과 오프셋을 따로 실어 보냄
중앙: corrected = 원본 + 오프셋 으로만 판정
```
- 기준은 중앙서버가 아니라 제3자(외부 NTP)다. 어느 지역도 유리해지지 않는다.
- 파일을 못 읽으면 0으로 되돌리지 않고 **마지막 값을 유지**한다(기준이 갈라지는 것이 곧 지역 편향).
- 컨테이너 안에서 직접 SNTP를 쏘지 않는 이유: 단발 질의 지터(σ 105us)가 chrony 추정치(RMS 19.7us)보다 커서 보정이 노이즈가 된다.
- `pop-clock-offset.service`는 `RuntimeDirectoryPreserve=yes`가 필수. 없으면 재시작 때 inode가 바뀌어 컨테이너가 새 파일을 영영 못 본다.

---

## 5. 동작 모드 스위치

| 항목 | 환경변수 | 현재 값 | 의미 |
|---|---|---|---|
| 봇 필터 | `BOT_FILTER_MODE` | `monitor` | off / monitor(판정만) / enforce(403·429 차단) |
| 신원 확인 | `AUTH_MODE` | `optional` | off / optional(토큰 없으면 익명) / required(401) |
| 수집창 | 코드 상수 | 5.0초 | 좌석별 요청 수집 시간 |
| 판정 DB 풀 / 인증 DB 풀 | `DB_POOL_MAX_SIZE` / `AUTH_POOL_MAX_SIZE` | 32 / 8 | 로그인 폭주가 판정 커넥션을 못 뺏게 물리적으로 분리 |
| PoP 워커 | `UVICORN_WORKERS` | 4 | 지표는 `PROMETHEUS_MULTIPROC_DIR`로 합산 |

`monitor`/`optional`인 이유: 부하 테스트(토큰·정상 UA 없는 요청)가 그대로 통과해야 성능 측정이 가능하다. 차단 시연 때만 올린다.

면제 경로(필터·인증 모두 통과): `/metrics`, `/health`, `/clock-offset` (+ 인증은 `/bot-filter`, `/auth-status`).

---

## 6. 관측

- Prometheus가 **1초마다** central, pop-pangyo, pushgateway(=대전), postgres-exporter, kafka-exporter, 자기 자신을 scrape.
  pushgateway 잡은 `honor_labels: true` 필수(아니면 대전의 `pop_id` 라벨이 덮인다).
- Grafana 대시보드 `koren-pangyo-dashboard.json` (1초 갱신, 익명 Viewer): ① 지금 이 순간(확정/잔여/경쟁/오류/대전 수신) ② 요청 처리율 ③ 좌석당 경쟁자·1·2등 시간차 ④ **PoP 간 최대 시계 차이(µs) = 공정성 지표** ⑤ Postgres 부하 ⑥ 좌석별 승자 PoP ⑦ 봇 탐지.
- 핵심 지표: `pop_seat_requests_total`, `pop_kafka_publish_duration_seconds`, `central_kafka_consumed_total`,
  `central_competitors_per_seat`, `central_winner_margin_ms`, `central_confirmed_seats_db`(재시작에도 정확한 값),
  `central_db_acquire_wait_seconds`, `pop_clock_offset_ms`, `pop_bot_*`, `pop_auth_verify_duration_seconds`.

---

## 7. 재시작 시 동작

- central 기동: 두 커넥션 풀 생성 → `confirmed_seats` 테이블 생성 → **DB에서 확정 좌석을 `seat_windows`로 복원**
  (이게 없으면 확정 좌석에 새 창이 열려 먼저 예매한 사람이 덮어써진다) → 인증 테이블/키쌍 준비 → Kafka producer·consumer 기동.
- PoP 기동: HTTP 공용 클라이언트 → 오프셋 1회 측정 후 5초 주기 루프 → Kafka producer(10회 재시도) → seat-status 구독(`earliest`라 전체 좌석 상태 복원).
- nginx는 upstream 이름을 기동 시 해석하므로 **PoP를 먼저 올려야 한다**.

---

## 8. 운영 스크립트

| 파일 | 용도 |
|---|---|
| `scripts/seed_accounts.sh` | 시연 계정 시드. 정상 20명(user01~20, 계정 나이 30~400일로 조정) + 매크로 30개(bot01~30, 전부 같은 device_fp `a1b2c3d4e5f60789`, 오늘 생성). 봇 탐지 대비군. |
| `doonload.py` | 부하 생성기(표준 라이브러리만). 연결 N개 재사용, 고정 시드로 좌석 배열을 섞어 두 PoP가 같은 좌석을 동시에 노리게 하고, `--at HH:MM:SS` 절대 시각으로 동시 발사. 기본 50개 계정 토큰을 미리 받아 무작위로 실어 보낸다. `python3 doonload.py --total 10000 --rate 400 --conn 100 --at 22:35:00` |
| `scripts/reset.sh` | 시연 초기화. 좌석 상태는 네 곳(중앙 DB / 중앙 메모리 / PoP 캐시 / Kafka 잔류 메시지)에 있어 순서대로 비운다: 대전 PoP 정지 확인 → 판교 PoP 정지 → `TRUNCATE confirmed_seats` → 컨슈머 오프셋 `--to-latest` → central 재시작 → PoP 기동. 계정은 남긴다. |

---

## 9. 알려진 제약

- 결제/발권 연동 없음(화면 시뮬레이션). 좌석 총 수 608은 `seat.html` ZONES와 `central_server.py TOTAL_SEATS`, `doonload.py`에 각각 하드코딩되어 사람이 맞춰야 한다.
- DB 비밀번호·Grafana 익명 접속은 데모용 설정.
- 봇 필터의 rate limit 상태는 워커별 메모리라 워커 4개면 실효 한도가 4배로 느슨하다.
- `detection-log` 소비자는 서울 AI Cloud pod 에서 돈다 (`seoul/collector.py`). 판교에서는 발행만 한다.
