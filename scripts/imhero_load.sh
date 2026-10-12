#!/usr/bin/env bash
# 임영웅급 비례 부하 테스트 러너  (좌석 608 고정, 좌석당 18배 = 상암 45,000석:81만 트래픽 비례)
#
#   실제:  45,000석 : 81만 트래픽  →  좌석당 18배
#   축소:  608석    × 18          ≈  11,000 동시 시도
#
# 사용법:
#   scripts/imhero_load.sh equiv     # 임영웅 등가(권장): 총 110,000 / 피크 5,500rps
#   scripts/imhero_load.sh worst     # 최악(대기열 없음): 총 220,000 / 피크 22,000rps
#   scripts/imhero_load.sh ramp      # 사다리: rps를 올려가며 lag이 무너지는 지점 탐색
#   scripts/imhero_load.sh <total> <rate> <conn> <front>   # 수동
#
# 주의: 양 PoP(판교+대전)를 동시에 쏘려면 --at 로 시각을 맞춰 두 호스트에서 각각
#       total/rate 를 '절반씩' 주고 실행하세요. 아래는 단일 호스트(판교) 기준입니다.

cd "$(dirname "$0")/.." || exit 1   # 저장소 루트 기준으로 실행 (compose 파일, doonload.py 경로)

HOST=${HOST:-localhost}
PORT=${PORT:-13198}
PROM=${PROM:-http://localhost:13090}

lag() {  # 현재 central 컨슈머 그룹 lag 합계
  curl -s --max-time 4 \
    "$PROM/api/v1/query?query=sum(kafka_consumergroup_lag%7Bconsumergroup%3D~%22central.*%22%7D)" \
    | python3 -c "import sys,json;d=json.load(sys.stdin)['data']['result'];print(int(float(d[0]['value'][1])) if d else 0)" 2>/dev/null || echo "?"
}

seatcount() { curl -s --max-time 4 "http://$HOST:$PORT/seat-count"; }

fire() {  # total rate conn front  라벨
  local total=$1 rate=$2 conn=$3 front=$4 label=$5
  echo "──────────────────────────────────────────────"
  echo "[$label] total=$total rate=$rate conn=$conn front=$front  (lag 시작=$(lag))"
  python3 doonload.py --host "$HOST" --port "$PORT" \
      --total "$total" --rate "$rate" --conn "$conn" --front "$front" --jitter 10
  echo "부하 종료 직후 lag=$(lag)"
  # lag이 0으로 빠질 때까지 관찰 (드레인 시간 = 진짜 처리능력의 척도)
  local t=0
  while [ "$(lag)" != "0" ] && [ $t -lt 120 ]; do sleep 3; t=$((t+3)); echo "  +${t}s  lag=$(lag)"; done
  echo "드레인 완료(또는 120s 초과).  좌석: $(seatcount)"
}

case "$1" in
  equiv) fire 110000 5500 400 24 "임영웅 등가" ;;
  worst) fire 220000 22000 800 24 "최악(대기열없음)" ;;
  ramp)
    fire  60000  2000 200 24 "ramp-1 (2k/s, 천장 이하)"
    fire 100000  3000 300 24 "ramp-2 (3k/s, 천장 근처)"
    fire 110000  5500 400 24 "ramp-3 (5.5k/s = 임영웅 등가)"
    fire 150000  8000 500 24 "ramp-4 (8k/s)"
    fire 200000 12000 700 24 "ramp-5 (12k/s)"
    ;;
  "") echo "인자를 주세요: equiv | worst | ramp | <total rate conn front>"; exit 1 ;;
  *) fire "$1" "${2:-5500}" "${3:-400}" "${4:-24}" "수동" ;;
esac
