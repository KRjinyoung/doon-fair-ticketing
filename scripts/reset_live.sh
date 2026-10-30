#!/usr/bin/env bash
# 좌석 초기화 (대전 PoP 를 멈추지 않고)  — 시연 재촬영용
#
# reset.sh 와 다른 점
#   reset.sh 는 대전 PoP 정지를 요구하고 PoP 를 재시작한다. 여기서는 아무것도 멈추지 않는다.
#   PoP 의 seat-status 컨슈머는 group 없이 늘 토픽을 처음부터 읽는다.  [STATUS_NOGROUP_V1]
#
#   그래서 두 스크립트 모두 확정된 좌석마다 status=available 메시지를 '더 뒤 오프셋에'
#   덮어쓴다. 살아있는 PoP 는 실시간으로 캐시가 갱신되고, 나중에 재시작해
#   토픽을 처음부터 다시 읽어도 이 메시지가 마지막이라 최종 상태가 유지된다.
#   덕분에 대전 PoP 를 건드릴 필요가 없다.
set -u
cd "$(dirname "$0")/.." || exit 1   # 저장소 루트 기준으로 실행 (compose 파일, doonload.py 경로)
CF=docker-compose.pangyo.yml
DC="docker compose -f $CF"

SEATS=$($DC exec -T postgres psql -U ticketing -d ticketing -tAc \
        "SELECT seat_id FROM confirmed_seats;" | tr -d '\r' | grep -v '^$')
N=$(printf '%s\n' "$SEATS" | grep -c . || true)
echo "[1/4] 확정 좌석 ${N}석"

echo "[2/4] central 정지 후 중앙 DB 와 Redis 비우기"
# STATE_BACKEND=redis 이면 확정 기록(sw:{seat}:c), 열린 창, DB 반영 대기 Stream 이
# Redis 에 남아 있어 DB 만 비우면 좌석이 계속 확정으로 보인다.
# central 을 먼저 멈춰야 DB 반영 소비자가 지운 좌석을 다시 쓰지 않는다.
$DC stop central >/dev/null 2>&1
$DC exec -T postgres psql -U ticketing -d ticketing -c "TRUNCATE confirmed_seats;" >/dev/null 2>&1
$DC exec -T redis sh -c "redis-cli --scan --pattern 'sw:*' | xargs -r -n 500 redis-cli DEL" >/dev/null 2>&1
# 결제 대기·미결제 횟수·2차 집계도 비운다 (세션·기기 정보 l2:sess/l2:dev 는 central 시작 때 DB 에서 다시 채움)  [MACRO3_V1]
$DC exec -T redis sh -c "for p in 'hold:*' 'holds' 'l2:*'; do redis-cli --scan --pattern \"\$p\" | xargs -r -n 500 redis-cli DEL; done" >/dev/null 2>&1

echo "[3/4] central 시작 (메모리 seat_windows 비우기)"
$DC start central >/dev/null 2>&1
for i in $(seq 1 25); do curl -s -m 2 localhost:10001/admin/window >/dev/null 2>&1 && break; sleep 1; done

if [ "$N" -gt 0 ]; then
  echo "[4/4] seat-status 에 available 덮어쓰기 (${N}건)"
  printf '%s\n' "$SEATS" | while read -r s; do
    [ -n "$s" ] && printf '%s|{"seat_id":"%s","status":"available"}\n' "$s" "$s"
  done | $DC exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server localhost:9092 --topic seat-status \
        --property parse.key=true --property key.separator='|' 2>/dev/null
else
  echo "[4/4] 덮어쓸 좌석 없음"
fi

sleep 3
echo
echo "=== 결과 $(date '+%H:%M:%S') ==="
printf "  좌석   : "; curl -s -m 4 localhost:13198/seat-count; echo
printf "  수집창 : "; curl -s -m 3 localhost:10001/admin/window; echo
$DC logs central --tail=40 2>/dev/null | grep -E "복원" | tail -1 | sed 's/^/  /'
echo
echo "  대전 화면도 좌석이 풀렸는지 눈으로 확인하세요 (여기서는 조회 불가)."
