#!/usr/bin/env bash
# 실험판 좌석만 0/608 로 되돌린다 (scripts/reset_live.sh 와 같은 순서, 대상만 실험판).
# detection-log 는 그대로 남는다 — 회차를 이어서 더 돌릴 때 쓴다.
set -u
cd "$(dirname "$0")/.."
DC="docker compose -f sim/docker-compose.yml"
SEATS=$($DC exec -T postgres psql -U ticketing -d ticketing -tAc "SELECT seat_id FROM confirmed_seats;" | tr -d '\r' | grep -v '^$')
$DC stop central >/dev/null 2>&1
$DC exec -T postgres psql -U ticketing -d ticketing -c "TRUNCATE confirmed_seats;" >/dev/null
$DC exec -T redis sh -c "redis-cli --scan --pattern 'sw:*' | xargs -r -n 500 redis-cli DEL" >/dev/null
$DC start central >/dev/null 2>&1
for i in $(seq 1 25); do curl -s -m 2 localhost:18001/admin/window >/dev/null 2>&1 && break; sleep 1; done
printf '%s\n' "$SEATS" | while read -r s; do
  [ -n "$s" ] && printf '%s|{"seat_id":"%s","status":"available"}\n' "$s" "$s"
done | $DC exec -T kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic seat-status --property parse.key=true --property key.separator='|' 2>/dev/null
sleep 3
echo "실험판 좌석: $(curl -s -m 4 172.17.0.1:18198/seat-count)"
