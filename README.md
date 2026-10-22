<div align="center">

# DOON

**티켓팅에 공정을 담다. with KOREN**

KOREN 전국 PoP 기반 엣지 시각 동기화를 통한 지역 간 네트워크 지연 격차 보정과<br>
AI 매크로 탐지로 구현하는 실시간 공정 티켓팅 플랫폼

K-디지털 챌린지: 넷 챌린지 캠프 시즌13 · Team Doon

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7-231F20?logo=apachekafka&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-F7931E?logo=scikitlearn&logoColor=white)
![Docker](https://img.shields.io/badge/Docker%20Compose-2496ED?logo=docker&logoColor=white)

</div>

## Overview

대규모 선착순에서 승부는 누가 먼저 눌렀느냐가 아니라 **어디서, 무엇으로** 눌렀느냐로 갈린다.

- **거리의 차이**: 서버에서 먼 지역의 요청은 늘 늦게 도착한다. 판교·대전 서버에서 실제 예매 사이트 5곳에 접속해 보면 대전이 평균 2.07ms 늦었고, 같은 순간에 같은 좌석을 누르는 실험에서 서버 도착순으로 정하면 대전은 **99번 중 한 번도 이기지 못했다.**
- **도구의 차이**: 매크로가 사람보다 먼저 좌석을 가져간다.

DOON은 KOREN 전국 PoP를 이용해 두 차이를 없앤다.

1. **엣지 스탬핑** — 사용자와 가장 가까운 PoP가 요청 도착 시각을 µs 단위로 먼저 찍고, 중앙은 그 시각에 PoP 시계 보정값을 더한 시각으로만 승자를 정한다. 지역에서 중앙까지 오는 구간은 승부에 들어가지 않는다.
2. **시계 보장 범위 안 동점 추첨** — 시계로 가를 수 없는 차이(chrony 보장 오차, 판교 약 1.3ms · 대전 약 2.5ms)는 순서를 우기지 않고 `sha1(좌석 | 요청 ID들)`로 추첨한다. 씨앗과 후보는 공개되어 누구나 다시 계산할 수 있다.
3. **매크로 3단계 방어** — 1차 PoP(요청 한 건) → 2차 중앙(전국 집계) → 3차 서울 AI(여러 회차의 계정 행동). 3차가 배운 명단·규칙·한도는 `bot-rules`로 1·2차에 자동 배포된다. 탐지는 **경쟁할 자격만** 정하고 순서는 바꾸지 않으며, AI 판단 근거에 지역·PoP·IP는 쓰지 않는다.

## Architecture

![DOON 네트워크 구성도](https://github.com/KRjinyoung/doon-fair-ticketing/blob/feat/bot-rules/docs/report/network/doon_network.png?raw=true)

| 구성 요소 | 위치 | 역할 |
|---|---|---|
| [doon.land](https://doon.land) | GitHub Pages ([doon-router](https://github.com/KRjinyoung/doon-router)) | 브라우저 위치로 가장 가까운 PoP 연결 (위치는 서버로 보내지 않음) |
| PoP | 판교 · 대전 | µs 도착 시각 각인 → 시계 보정값·보장 오차 첨부, JWT 로컬 검증, **매크로 1차** |
| chrony | 각 PoP 호스트 | 같은 외부 NTP 기준으로 시계 오차 측정 (5초 주기) |
| Kafka | 판교 | `seat-requests` · `seat-status` · `detection-log` · `bot-rules` |
| Central | 판교 | **매크로 2차** → 보정 시각순 판정 + 동점 추첨 → 결제 대기(좌석 선점 방지), 로그인 |
| Redis | 판교 | 좌석별 수집창, 2차 전국 집계, 결제 대기 |
| PostgreSQL | 판교 | 확정 좌석, 계정, 세션, 서명키 |
| Seoul AI Cloud | 서울 | `detection-log` 수집, **매크로 3차** 학습 → `bot-rules` 배포 |
| Prometheus · Grafana | 판교 | 처리량, 1·2등 시간차, PoP 시계 보장 오차, 추첨·차단 현황 |

### 판정 흐름

```
① 사용자 → 가까운 PoP          도착 시각 각인 (모든 검사보다 먼저)
② PoP → seat-requests          원본 시각 + 시계 보정값 + 보장 오차 + 신원
③ Central                      2차 검사 → 보정 시각 = 원본 + 보정값
                               좌석별 수집창이 닫히면 가장 빠른 요청과 보장 오차가 겹치는 요청끼리 추첨
④ seat-status → 모든 PoP        결과·추첨 씨앗 공개 → 승자는 결제 대기 (기한 초과 시 좌석 반환)
⑤ PoP → detection-log → 서울    3차 학습
⑥ 서울 → bot-rules → PoP·Central 명단·규칙·한도 자동 배포
```

### 매크로 3단계 방어

| 단계 | 위치 | 보는 범위 | 주요 검사 | 걸리면 |
|---|---|---|---|---|
| 1차 | PoP | 요청 한 건 + 이 PoP 최근 요청 | 위조 토큰, IP·계정·기기 요청 수, 이름표(User-Agent 등), AI 명단·규칙 | 403 / 429 |
| 2차 | Central | 전국 PoP 합산 + 중앙 DB | 신원 대조, 기기당 신규 계정 수, 미결제 선점, 전국 요청 수, AI 명단 | 판정 후보에서 제외 |
| 3차 | 서울 AI | 전국 · 여러 회차 | 회차 간 도착 흩어짐 등 계정 행동 → 봇 확률 + 근거 | 직접 막지 않음, 1·2차로 배포 |

3차는 정답표 없이 1·2차가 확실한 사유로 막은 계정을 '봇', 오래된 무사고 계정을 '사람'으로 삼아 학습한다(약한 지도 학습). 새 규칙은 그림자 모드로 먼저 나가 사람을 한 건도 맞히지 않을 때만 차단으로 승격된다. 자세한 내용은 [docs/research/13-macro-defense.md](https://github.com/KRjinyoung/doon-fair-ticketing/blob/feat/bot-rules/docs/research/13-macro-defense.md).

## Results

| 실험 | 결과 | 문서 |
|---|---|---|
| 지역 격차 실측 | 예매 사이트 5곳: 대전이 판교보다 평균 **+2.07ms** · RIPE Atlas 가정 회선: 전남이 수도권보다 **+1.3~3.5ms** | [06](docs/research/06-regional-gap.md) |
| 같은 순간 클릭 대결 (실서비스, 99좌석) | 서버 도착순: 대전 **0/99** → DOON: 대전 **54/99** (99좌석 모두 보장 범위 안 추첨) | [11](https://github.com/KRjinyoung/doon-fair-ticketing/blob/feat/bot-rules/docs/research/11-duel.md) |
| 매크로 3단계 (격리 실험판, 18회차) | 4회차부터 봇 6종 모두 차단, 처음 보는 봇 계정은 AI가 만든 규칙에 **첫 요청부터** 차단, 사람 요청 차단 **0건**, 3차 재학습·배포 약 0.2초 | [13](https://github.com/KRjinyoung/doon-fair-ticketing/blob/feat/bot-rules/docs/research/13-macro-defense.md) |

한계도 같은 문서에 적었다. 시간·헤더·기기를 모두 위장하면서 사람의 약 90%보다 빠른 봇은 시간 신호만으로 사람(손 빠른 사람)과 가를 수 없어 추가 인증으로 넘겨야 하고, 대리 예매는 범위 밖이다. 사람 쪽 데이터는 시뮬레이터다.

## Getting Started

### Requirements

- Docker, Docker Compose v2
- chrony (각 PoP 호스트)

### Run (Pangyo · Central + PoP)

```bash
git clone git@github.com:KRjinyoung/doon-fair-ticketing.git
cd doon-fair-ticketing

cp .env.example .env    # POSTGRES_PASSWORD, GRAFANA_ADMIN_PASSWORD 채우기
docker compose -f docker-compose.pangyo.yml up -d --build
```

`.env`로 바꾸는 동작 모드 (compose 기본값은 판정만 하고 통과시키는 `monitor`)

| 변수 | 기본값 | 실가동 | 의미 |
|---|---|---|---|
| `STATE_BACKEND` | `memory` | `redis` | 수집창 상태 저장소 |
| `REDIS_DB_BUFFER` | `0` | `1` | 확정 기록을 Redis Stream으로 모아 DB에 배치 저장 |
| `BOT_FILTER_MODE` | `monitor` | `enforce` | 매크로 1차: `off` / `monitor` / `enforce` |
| `L2_MODE` | `monitor` | `enforce` | 매크로 2차: `off` / `monitor` / `enforce` |
| `HOLD_SEC` | `120` | `20` | 결제 기한(초), 넘기면 좌석 반환 |

### Run (Daejeon · PoP)

대전은 hirun 프록시를 거쳐야 판교에 닿는다. hirun 셸에서 socat 중계기를 먼저 띄운다.

```bash
~/hirun -c bash
bash scripts/daejeon_relay.sh start
exit

docker compose -f docker-compose.daejeon.yml up -d --build
docker restart nginx    # PoP를 다시 만들면 nginx도 재시작 (upstream을 기동 때 해석)
```

대전 PoP 지표를 판교 Pushgateway로 보내는 루프는 systemd 서비스로 설치한다.

```bash
sudo install -m 755 scripts/push_metrics_loop.sh /usr/local/bin/push-metrics-daejeon.sh
sudo install -m 644 scripts/push-metrics-daejeon.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now push-metrics-daejeon
```

### Clock offset collector

PoP가 올라가는 모든 호스트에 설치한다. 설치하지 않으면 해당 PoP는 보정 없이(보장 오차 0으로) 동작한다.

```bash
sudo install -m 755 scripts/chrony-offset-writer.sh /usr/local/bin/
sudo install -m 644 scripts/pop-clock-offset.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pop-clock-offset
```

### Seoul AI (매크로 3차)

판교 Kafka를 서울 AI Cloud pod로 여는 역방향 SSH 터널을 띄운 뒤, pod에서 수집기와 3차 AI를 실행한다.

```bash
bash scripts/cheetah_tunnel.sh                       # 판교에서
python3.11 seoul/collector.py                        # 서울: detection-log 보관
TUNNEL_HOST=127.0.0.1 KAFKA_BOOTSTRAP=kafka-external:13092 \
  python3.11 seoul/ai3.py --rounds <오픈 일정 jsonl>    # 서울: 학습 → bot-rules 배포
```

### Ports

| Port | Service |
|---|---|
| 13198 | Web (nginx) |
| 10001 | Central API |
| 13092 | Kafka (external) |
| 13091 | Pushgateway (대전 PoP 지표) |
| 13090 | Prometheus |
| 13300 | Grafana |

## Scripts

```bash
bash scripts/seed_accounts.sh       # 시연 계정 생성 (user01~20, bot01~30)
bash scripts/reset_live.sh          # 좌석·결제 대기 초기화 (서비스 정지 없이)
bash scripts/reset.sh               # 전체 초기화 (PoP 재시작 포함)
bash scripts/imhero_load.sh equiv   # 부하 테스트
python3 scripts/duel.py --side pangyo --start HH:MM:SS --n 100   # 같은 순간 클릭 대결

# 격리 실험판 (실서비스와 분리된 별도 compose 프로젝트)
SIM_FILTER_MODE=enforce bash sim/up.sh
python3 bots/rounds.py --help       # 봇 6종 + 사람 시뮬레이터 회차 진행
bash sim/down.sh
```

## Project Structure

```
.
├── central/                    # 판정 서버, 2차 매크로 검사, 결제 대기, 인증, 중앙 모니터 화면
├── pop/                        # PoP 서버, 1차 매크로 검사, 예매 웹 (좌석·결제 화면)
├── seoul/                      # detection-log 수집기, 3차 AI (ai3.py)
├── bots/                       # 실험용 매크로 6종, 회차 진행기
├── sim/                        # 격리 실험판, 사람 시뮬레이터, 평가 스크립트
├── nginx/                      # 진입점 라우팅 (판교 · 대전)
├── monitoring/                 # Prometheus, Grafana
├── scripts/                    # 운영, 시연, 대결 실험, 부하 테스트
├── docs/
│   ├── report/network/         # 네트워크 구성도
│   └── research/               # 조사·측정·실험 기록
├── doonload.py                 # 부하 생성기
├── docker-compose.pangyo.yml
├── docker-compose.daejeon.yml
└── SYSTEM.md                   # 시스템 상세 문서
```

## Docs

| 문서 | 내용 |
|---|---|
| [SYSTEM.md](SYSTEM.md) | 컨테이너 구성, 사용자 흐름, 판정·시각 동기화 상세 |
| [docs/research/](docs/research/README.md) | 조사·분석 자료 목록 |
| [06-regional-gap](docs/research/06-regional-gap.md) | 지역 격차 실측 |
| [11-duel](https://github.com/KRjinyoung/doon-fair-ticketing/blob/feat/bot-rules/docs/research/11-duel.md) | 같은 순간 클릭 대결 |
| [13-macro-defense](https://github.com/KRjinyoung/doon-fair-ticketing/blob/feat/bot-rules/docs/research/13-macro-defense.md) | 매크로 3단계 방어 총정리 |

## Team

**Team Doon** · K-디지털 챌린지: 넷 챌린지 캠프 시즌13

| 이름 | 역할 |
|---|---|
| 박진영 | 인프라 · 시각 동기화 · 매크로 방어 |
| 정유진 | 백엔드 · 프론트엔드 |
