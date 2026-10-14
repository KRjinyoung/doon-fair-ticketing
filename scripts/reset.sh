#!/usr/bin/env bash
# 시연 녹화 전 좌석 초기화 (판교에서 실행)
#
# 좌석 상태는 네 군데에 있다. 하나만 지우면 다른 데서 되살아난다.
#   1) 중앙 Postgres  confirmed_seats
#   2) 중앙 메모리    seat_windows        -> central 재시작
#   3) PoP 메모리     seat-status 캐시    -> PoP 재시작
#   4) Kafka          seat-status 잔류    -> 확정됐던 좌석마다 available 덮어쓰기
#   5) Redis          수집창/확정 키      -> redis-cli flushall   [REDIS_STEP1B]
#
# 4번을 빼면 PoP 가 재시작하며 옛 메시지를 다시 읽어 좌석이 되살아난다.
# PoP 는 seat-status 를 group 없이 늘 처음부터 읽으므로 오프셋을 옮겨도 소용없다.
# 대신 더 뒤 오프셋에 available 을 써서 '마지막 상태'를 바꾼다.  [STATUS_NOGROUP_V1]
set -u
cd "$(dirname "$0")/.." || exit 1   # 저장소 루트 기준으로 실행 (compose 파일, doonload.py 경로)
CF=docker-compose.pangyo.yml
DC="docker compose -f $CF"

echo
echo "  시작 전에 대전에서 아래를 실행하세요:"
echo "    docker compose -f docker-compose.daejeon.yml stop pop-daejeon"
printf "\n  대전 PoP 를 정지했습니까? [y/N] "
read -r a
case "$a" in y|Y) ;; *) echo "  중단"; exit 1;; esac

echo
echo "[1/5] Kafka 스크립트 경로"
KB=""
for c in /opt/kafka/bin /opt/bitnami/kafka/bin /usr/bin; do
  if $DC exec -T kafka sh -c "test -x $c/kafka-console-producer.sh" 2>/dev/null; then KB=$c; break; fi
done
if [ -n "$KB" ]; then echo "  $KB"; else echo "  못 찾음 - 4번 건너뜀"; fi

echo
echo "[2/5] 판교 PoP 정지"
$DC stop pop-pangyo >/dev/null 2>&1
echo "  pop-pangyo 정지"

echo
echo "[3/5] 중앙 DB 좌석 비우기"
SEATS=$($DC exec -T postgres psql -U ticketing -d ticketing -tAc \
        "SELECT seat_id FROM confirmed_seats;" | tr -d '\r' | grep -v '^$')
$DC exec -T postgres psql -U ticketing -d ticketing -c "TRUNCATE confirmed_seats;" >/dev/null 2>&1
printf "  남은 행 : "
$DC exec -T postgres psql -U ticketing -d ticketing -tAc "SELECT count(*) FROM confirmed_seats;"
# [REDIS_STEP1B] Postgres 만 비우면 Redis 의 옛 확정이 새 요청을 계속 반려한다. 같이 비운다.
if $DC exec -T redis redis-cli flushall >/dev/null 2>&1; then echo "  Redis 비움"; else echo "  Redis 없음/실패 - 건너뜀"; fi

echo
echo "[4/5] seat-status 에 available 덮어쓰기"
if [ -n "$KB" ] && [ -n "$SEATS" ]; then
  printf '%s\n' "$SEATS" | while read -r s; do
    [ -n "$s" ] && printf '%s|{"seat_id":"%s","status":"available"}\n' "$s" "$s"
  done | $DC exec -T kafka "$KB/kafka-console-producer.sh" \
        --bootstrap-server localhost:9092 --topic seat-status \
        --property parse.key=true --property key.separator='|' 2>/dev/null
  echo "  $(printf '%s\n' "$SEATS" | grep -c .)건"
else
  echo "  덮어쓸 좌석 없음"
fi

echo
echo "[5/5] 중앙 재시작 + PoP 기동"
$DC restart central >/dev/null 2>&1
sleep 15
$DC logs central --tail=80 2>/dev/null | grep -E "복원|Postgres 연결|인증 준비" | sed 's/^/  /'
$DC start pop-pangyo >/dev/null 2>&1
sleep 10
# [REDIS_STEP1B] PoP 가 새 IP 를 받았을 수 있다. nginx upstream 은 시작 때 한 번만 이름을 조회하므로 다시 읽게 한다.
if $DC exec -T nginx nginx -s reload >/dev/null 2>&1; then echo "  nginx 업스트림 갱신(reload)"; else echo "  nginx reload 실패 - 수동: docker exec nginx nginx -s reload"; fi

echo
echo "=== 결과 ==="
printf "  확정 좌석 : "
$DC exec -T postgres psql -U ticketing -d ticketing -tAc "SELECT count(*) FROM confirmed_seats;"
printf "  가입 계정 : "
$DC exec -T postgres psql -U ticketing -d ticketing -tAc "SELECT count(*) FROM users;"
printf "  seats-overview : "
curl -s -m5 localhost:13198/seats-overview | head -c 120
echo

cat <<'MSG'

  --- 남은 순서 ---
   1) 대전 : docker compose -f docker-compose.daejeon.yml start pop-daejeon
   2) 대시보드 : 확정 좌석 0 / 잔여석 608 / 대전 지표 수신 초 단위
   3) 브라우저 : 시크릿 창으로 열기 (남은 로그인 토큰 제거)

  계정(user01~20, bot01~30)은 지우지 않았습니다.
MSG
