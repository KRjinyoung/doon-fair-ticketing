#!/bin/bash
# 부하 테스트 — 판교/대전 양쪽에서 동시에 실행
#
# *** 두 VM에 같은 파일을 넣고 각자 실행한다 ***
#   판교(POP001) 터미널:  bash loadtest.sh pangyo  flood --at 14:30:00
#   대전 일반 셸:         bash loadtest.sh daejeon flood --at 14:30:00
#
# 각 VM은 자기 로컬 PoP(localhost:13198)로 요청을 보낸다.
# 중계기를 타지 않으므로 순수한 서비스 성능이 측정되고, 실제 사용자가
# 자기 지역 PoP에 접속하는 상황과 같아진다.
#
# 모드:
#   flood    좌석을 절반씩 나눠 맡아 전체 608석을 채운다 (화면이 빨갛게 물듦)
#   compete  양쪽이 '같은' 좌석에 동시 요청 (경쟁 판정 시연)
#   warmup   천천히 채우기 (영상 도입부용)
#   reset    좌석 비우는 방법 안내 (판교에서만 의미 있음)
#
# 화면 색상: 흰색=빈 좌석 / 빨강=경쟁 중(5초) / 회색=확정 완료

ROLE=${1:-help}
MODE=${2:-help}
shift 2 2>/dev/null

AT=""
CONC=60
PACE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --at)   AT="$2"; shift 2 ;;
    --conc) CONC="$2"; shift 2 ;;
    --pace) PACE="$2"; shift 2 ;;
    *) shift ;;
  esac
done

URL=http://localhost:13198

# ---------------------------------------------------------------------------
# 좌석 ID 생성 (A-1-1 ~ J-8-10, 총 608석)
# seat.html 의 ZONES/seatId() 규칙과 정확히 일치해야 화면에 반영된다.
# ---------------------------------------------------------------------------
gen_seats() {
  for z in A:6 B:8 C:10 D:8 E:6 F:6 G:8 H:10 I:8 J:6; do
    L=${z%%:*}; N=${z##*:}
    for r in 1 2 3 4 5 6 7 8; do
      for c in $(seq 1 $N); do echo "$L-$r-$c"; done
    done
  done
}

# 좌석을 두 지역이 겹치지 않게 나눈다.
# 좌석 이름 해시로 가르므로 양쪽에서 계산해도 결과가 같고, 겹치거나 빠지는 좌석이 없다.
my_seats() {
  local want=0
  [ "$ROLE" = "daejeon" ] && want=1
  gen_seats | while read -r s; do
    if [ $(( $(echo -n "$s" | cksum | cut -d' ' -f1) % 2 )) -eq $want ]; then echo "$s"; fi
  done
}

shuffle() { shuf 2>/dev/null || awk 'BEGIN{srand()}{print rand()"\t"$0}' | sort -k1,1n | cut -f2-; }

cat > /tmp/lt_fire.sh << 'FIREEOF'
#!/bin/bash
curl -s -o /dev/null --max-time 5 -X POST "$2/seat-request" \
  -H 'Content-Type: application/json' -d "{\"seat_id\":\"$1\"}"
FIREEOF
chmod +x /tmp/lt_fire.sh

remaining() {
  curl -s --max-time 5 $URL/seat-count 2>/dev/null \
    | grep -o '"remaining"[[:space:]]*:[[:space:]]*[0-9]*' | grep -o '[0-9]*$'
}

# 지정 시각까지 대기. 양쪽 VM이 같은 시각에 시작해야
# 경쟁 판정이 제대로 일어나고 영상에서도 동시에 채워지는 게 보인다.
wait_until() {
  [ -z "$AT" ] && return
  local target=$(date -d "today $AT" +%s 2>/dev/null)
  [ -z "$target" ] && { echo "  시각 형식 오류 (HH:MM:SS 로 주세요)"; exit 1; }
  local now=$(date +%s)
  [ "$target" -le "$now" ] && target=$((target + 86400))
  local wait=$((target - now))
  echo "  $AT 까지 ${wait}초 대기 (양쪽 동시 시작)"
  while [ $(date +%s) -lt $target ]; do
    local left=$((target - $(date +%s)))
    [ $left -le 5 ] && echo "  $left..."
    sleep 1
  done
  echo "  시작"
}

case "$ROLE" in pangyo|daejeon) ;; *)
  if [ "$ROLE" != "help" ]; then echo "role 은 pangyo 또는 daejeon 이어야 합니다"; fi
  MODE=help ;;
esac

case "$MODE" in

flood)
  echo "=== [$ROLE] 폭주: 담당 좌석 전체 동시 요청 ==="
  my_seats | shuffle > /tmp/lt_seats.txt
  T=$(wc -l < /tmp/lt_seats.txt)
  echo "  담당 좌석: ${T}석 (반대편이 나머지를 맡습니다)"
  echo "  동시 실행: ${CONC}"
  echo "  시작 전 잔여석: $(remaining)"
  [ "$PACE" -gt 0 ] && echo "  ${PACE}초에 걸쳐 나눠 발사 (영상용)"
  wait_until
  S=$(date +%s%3N)
  if [ "$PACE" -gt 0 ]; then
    # 좌석을 여러 묶음으로 나눠 천천히 보낸다.
    # 한 번에 다 쏘면 1초 만에 끝나서 화면이 빨개지는 과정이 안 보인다.
    STEPS=$((PACE * 2))
    [ $STEPS -lt 1 ] && STEPS=1
    CHUNK=$(( (T + STEPS - 1) / STEPS ))
    split -l $CHUNK /tmp/lt_seats.txt /tmp/lt_chunk_
    for f in /tmp/lt_chunk_*; do
      xargs -P $CONC -I{} /tmp/lt_fire.sh {} "$URL" < "$f"
      echo "    $(wc -l < $f)석 발사 (잔여 $(remaining))"
      sleep 0.5
    done
    rm -f /tmp/lt_chunk_*
  else
    xargs -P $CONC -I{} /tmp/lt_fire.sh {} "$URL" < /tmp/lt_seats.txt
  fi
  E=$(date +%s%3N)
  D=$((E-S))
  echo "  발사 완료: ${T}건 / ${D}ms"
  [ $D -gt 0 ] && echo "  초당 처리: $((T*1000/D)) req/s"
  echo '  판정 대기 (8초)...'
  sleep 8
  echo "  최종 잔여석: $(remaining)"
  ;;

compete)
  N=${CONC}
  SEAT=${SEAT:-C-4-5}
  echo "=== [$ROLE] 경쟁: 같은 좌석에 ${N}건 동시 요청 ==="
  echo "  대상 좌석: $SEAT (양쪽이 동일한 좌석을 노립니다)"
  wait_until
  for i in $(seq 1 $N); do
    /tmp/lt_fire.sh "$SEAT" "$URL" &
  done
  wait
  echo "  ${N}건 발사 완료. 판정 대기 (8초)..."
  sleep 8
  echo ''
  echo "  [$ROLE] 이 본 결과:"
  curl -s --max-time 5 $URL/seat-status/$SEAT | head -c 500; echo ''
  echo ''
  echo '  양쪽 터미널의 winner_request_id 가 같으면 판정이 일관된 것입니다.'
  ;;

warmup)
  echo "=== [$ROLE] 워밍업: 천천히 채우기 ==="
  my_seats | shuffle | head -30 > /tmp/lt_seats.txt
  wait_until
  i=0
  while read -r s; do
    /tmp/lt_fire.sh "$s" "$URL" &
    i=$((i+1))
    [ $((i % 10)) -eq 0 ] && echo "  ${i}석 요청됨 (잔여 $(remaining))"
    sleep 0.6
  done < /tmp/lt_seats.txt
  wait
  echo "  워밍업 완료: ${i}석"
  ;;

reset)
  echo '=== 좌석 비우기 (판교에서만 실행) ==='
  echo ''
  echo '아래 두 줄을 판교(POP001) 터미널에서 실행하세요:'
  echo ''
  echo '  docker exec postgres psql -U ticketing -d ticketing -c "TRUNCATE confirmed_seats;"'
  echo '  docker compose -f ~/koren-ticketing/docker-compose.pangyo.yml restart central pop-pangyo'
  echo ''
  echo 'central 재시작이 필요한 이유:'
  echo '  확정 좌석이 DB뿐 아니라 메모리(seat_windows)에도 남아 있어서,'
  echo '  DB만 비우면 화면에는 여전히 회색으로 보입니다.'
  echo 'pop 재시작이 필요한 이유:'
  echo '  각 PoP의 로컬 캐시(seat_status_cache)도 같이 비워야 화면이 초기화됩니다.'
  echo '  (대전 PoP도 재시작해야 합니다: docker restart pop-daejeon)'
  ;;

*)
  echo '사용법: bash loadtest.sh <역할> <모드> [옵션]'
  echo ''
  echo '  역할: pangyo | daejeon   (실행하는 VM에 맞게)'
  echo '  모드: flood | compete | warmup | reset'
  echo ''
  echo '  옵션:'
  echo '    --at HH:MM:SS   지정 시각에 동시 시작 (양쪽 맞추기용)'
  echo '    --conc N        동시 실행 수 (기본 60)'
  echo '    --pace N        N초에 걸쳐 나눠 발사 (영상용, 기본 0=즉시)'
  echo ''
  echo '  예시 (영상 촬영):'
  echo '    판교:  bash loadtest.sh pangyo  flood --at 14:30:00 --pace 20'
  echo '    대전:  bash loadtest.sh daejeon flood --at 14:30:00 --pace 20'
  echo ''
  echo '    판교:  bash loadtest.sh pangyo  compete --at 14:35:00 --conc 20'
  echo '    대전:  bash loadtest.sh daejeon compete --at 14:35:00 --conc 20'
  ;;
esac
