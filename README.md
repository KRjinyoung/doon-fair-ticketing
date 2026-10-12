# KOREN 전국 PoP 기반 엣지 시각 동기화를 통한 지역 간 네트워크 지연 격차 보정과 AI 매크로 탐지로 구현하는 실시간 공정 티켓팅 플랫폼

**Team Doon** · K-디지털 챌린지: 넷 챌린지 캠프 시즌13

> 서울 사용자와 지방 사용자가 같은 순간 누른 예매 버튼은, 같은 순간 누른 것으로 판정되어야 한다.

## 해결하려는 문제

- **지역 간 지연 격차** — 티켓팅 서버가 한 곳에 있으면 서버에 가까운 지역의 요청이 먼저 도착한다.
  "먼저 도착한 요청이 승자"인 지금의 방식에서는 사는 곳이 곧 경쟁력이 된다.
- **매크로** — 오픈 순간에 몰리는 자동화 요청이 정상 사용자의 기회를 가져간다.

## 핵심 아이디어

### 1. 엣지 시각 기록 + 시계 보정 판정

```
사용자 ──▶ 가까운 지역 PoP ── 도착 시각 기록 ──▶ Kafka ──▶ 중앙 판정 서버
               │                                              │
          chrony(외부 NTP)로                         보정 시각 = 도착 시각 + 시계 오차
          시계 오차를 상시 측정                       5초 수집창 안에서 가장 빠른 요청이 승자
```

- 요청이 중앙에 **도착한** 시각이 아니라, 사용자 가까이의 **PoP에 도착한** 시각으로 판정한다.
- 기준 시계는 중앙 서버가 아니라 제3자(외부 NTP)다. 어느 지역도 기준이 되지 않으므로 어느 지역도 유리하지 않다.
- PoP는 자기 시계를 고치지 않고 원본 시각과 오차를 따로 보내며, 보정은 중앙의 한 지점에서만 일어난다.

### 2. 매크로 탐지

- **PoP 1차 필터** (`pop/first_layer.py`) — 요청 빈도와 헤더 정합성으로 명백한 봇을 엣지에서 거른다.
- **AI 탐지용 데이터 파이프라인** — PoP가 요청마다 접속 정보와 신원(계정 나이, 기기 지문 등)을
  `detection-log` 토픽으로 발행하고, 서울 AI Cloud가 이를 수집해 학습 데이터로 보관한다 (`seoul/`).

## 전국 구성 (KOREN)

| 지역 | 역할 |
|---|---|
| 판교 | 중앙 판정 서버, 인증, Kafka, DB, 모니터링 + 판교 PoP |
| 대전 | 대전 PoP |
| 서울 (AI Cloud) | 매크로 탐지 로그 수집·학습 |

상세 구성과 동작 원리는 [`SYSTEM.md`](SYSTEM.md)를 참고.

## 기술 스택

FastAPI · Kafka · PostgreSQL · Redis · nginx · chrony · Prometheus · Grafana · Docker Compose

## 폴더

| 경로 | 내용 |
|---|---|
| `central/` | 중앙 판정 서버 + 인증 서버, 수집창 모니터 화면 |
| `pop/` | 지역 PoP 서버 (도착 시각 기록, 토큰 검증, 1차 봇 필터), 예매 사이트 화면 |
| `seoul/` | 서울 AI Cloud pod 에서 돌리는 탐지 로그 수집기 |
| `nginx/` | 사용자 진입점(13198) 라우팅 |
| `monitoring/` | Prometheus / Grafana 설정과 대시보드 |
| `scripts/` | 운영·시연·부하 테스트 스크립트 |
| `doonload.py` | 부하 생성기 (`scripts/imhero_load.sh` 가 사용) |
| `docker-compose.pangyo.yml` | 판교 호스트 전체 구성 |

## 판교 실행

```bash
cp .env.example .env        # 비밀번호 채우기
docker compose -f docker-compose.pangyo.yml up -d --build
```

| 포트 | 서비스 |
|---|---|
| 13198 | 예매 사이트 (nginx) |
| 10001 | central |
| 13092 | Kafka (외부) |
| 13090 | Prometheus |
| 13300 | Grafana |

PoP 호스트마다 시계 보정 수집기가 필요하다 (없으면 보정값 0으로 동작):

```bash
sudo install -m 755 scripts/chrony-offset-writer.sh /usr/local/bin/
sudo install -m 644 scripts/pop-clock-offset.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now pop-clock-offset
```

## 자주 쓰는 스크립트

```bash
bash scripts/seed_accounts.sh      # 시연 계정 (정상 20 + 매크로 30)
bash scripts/reset.sh              # 좌석 전체 초기화
bash scripts/imhero_load.sh equiv  # 부하 테스트
bash scripts/cheetah_tunnel.sh     # 서울 pod 로 Kafka 터널
```

## 저장소에 없는 것

- 대전 PoP 의 `docker-compose.daejeon.yml` (대전 호스트에만 있음)
- `.env` (비밀 값), SSH 키
