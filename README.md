# DOON — 공정 티켓팅 (KOREN)

여러 지역 PoP(엣지 서버)가 좌석 요청의 **도착 시각**을 찍어 Kafka로 중앙에 넘기고,
중앙 서버가 **시계 오차를 보정한 시각**으로 누가 먼저였는지 판정하는 티켓팅 데모.

시스템 구성과 동작 원리는 [`SYSTEM.md`](SYSTEM.md)에 자세히 있다.

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
