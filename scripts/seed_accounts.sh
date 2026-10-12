#!/usr/bin/env bash
# ===========================================================================
# 시연용 계정 시드  (판교에서 실행)
# ---------------------------------------------------------------------------
# 두 부류를 만든다.
#
#   정상 사용자 20명  - 계정 나이 제각각(30~400일), 기기도 제각각
#   매크로 계정 30개  - 오늘 만들어졌고, 전부 같은 기기에서 로그인
#
# *** 왜 이렇게 나누나 ***
# 서울 AI Cloud 가 학습할 신호는 결국 두 가지 대비다.
#   1) 계정 나이   : 오픈 직전에 무더기로 만들어진 계정인가
#   2) 기기 집중도 : 한 기기에 계정이 몇 개나 붙어 있나
# 정상 데이터가 없으면 "이게 이상한 것"이라고 말할 기준선이 없다.
# 그래서 정상군을 반드시 같이 심는다.
#
# 계정 나이는 /auth/signup 이 created_at 을 현재 시각으로 넣기 때문에
# 가입 후 Postgres 에서 직접 과거로 돌린다(시연 데이터이므로 허용).
# ===========================================================================
set -u
cd "$(dirname "$0")/.." || exit 1   # 저장소 루트 기준으로 실행 (compose 파일, doonload.py 경로)

CENTRAL="${CENTRAL:-localhost:10001}"
COMPOSE="${COMPOSE:-docker-compose.pangyo.yml}"
PW="${PW:-doon1234}"
NORMAL_N="${NORMAL_N:-20}"
BOT_N="${BOT_N:-30}"

# 매크로가 공유하는 기기 식별값. 30개 계정이 전부 이 값으로 로그인한다.
BOT_FP="a1b2c3d4e5f60789"

psql() { docker compose -f "$COMPOSE" exec -T postgres psql -U ticketing -d ticketing -tAc "$1"; }

signup() {  # $1=email
  curl -s -o /dev/null -w '%{http_code}' -X POST "http://$CENTRAL/auth/signup" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$1\",\"password\":\"$PW\"}"
}

login() {   # $1=email  $2=device_fp
  curl -s -o /dev/null -w '%{http_code}' -X POST "http://$CENTRAL/auth/login" \
    -H 'Content-Type: application/json' \
    -d "{\"email\":\"$1\",\"password\":\"$PW\",\"device_fp\":\"$2\"}"
}

echo "중앙서버 : http://$CENTRAL"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://$CENTRAL/auth/stats")
if [ "$code" != "200" ]; then
  echo "  [중단] /auth/stats 가 $code 를 반환했습니다. 중앙서버 주소를 확인하세요."
  echo "         예) CENTRAL=localhost:10001 bash $0"
  exit 1
fi

# ---------------------------------------------------------------------------
echo
echo "[1/4] 정상 사용자 $NORMAL_N 명 생성"
ok=0; dup=0
for i in $(seq -w 1 "$NORMAL_N"); do
  c=$(signup "user$i@doon.kr")
  if [ "$c" = "200" ]; then ok=$((ok+1)); else dup=$((dup+1)); fi
done
echo "      생성 $ok / 기존 $dup"

echo "[2/4] 매크로 계정 $BOT_N 개 생성 (오늘 만든 계정)"
ok=0; dup=0
for i in $(seq -w 1 "$BOT_N"); do
  c=$(signup "bot$i@doon.kr")
  if [ "$c" = "200" ]; then ok=$((ok+1)); else dup=$((dup+1)); fi
done
echo "      생성 $ok / 기존 $dup"

# ---------------------------------------------------------------------------
echo "[3/4] 계정 나이 조정"
# 정상 사용자만 과거로 돌린다. 30~400일 사이로 흩어놓는다.
# 매크로 계정은 오늘 그대로 둔다 - 그게 신호다.
psql "UPDATE users
      SET created_at = now() - ((30 + (random() * 370)::int) || ' days')::interval
      WHERE email LIKE 'user%@doon.kr';" > /dev/null
echo "      정상 사용자: 30~400일 전으로 분산"
echo "      매크로 계정: 오늘(0일) 유지"

# ---------------------------------------------------------------------------
echo "[4/4] 로그인 이력 생성"
# 정상 사용자는 각자 다른 기기에서 로그인한다.
for i in $(seq -w 1 "$NORMAL_N"); do
  login "user$i@doon.kr" "$(printf 'u%015d' "$((10#$i))")" > /dev/null
done
echo "      정상 사용자 $NORMAL_N 명 : 각자 다른 기기"

# 매크로는 전부 같은 기기에서 로그인한다. 이게 핵심 패턴이다.
for i in $(seq -w 1 "$BOT_N"); do
  login "bot$i@doon.kr" "$BOT_FP" > /dev/null
done
echo "      매크로 $BOT_N 개          : 전부 같은 기기($BOT_FP)"

# ---------------------------------------------------------------------------
echo
echo "=== 결과 ==="
printf '전체 계정      : %s\n' "$(psql 'SELECT count(*) FROM users;')"
printf '전체 세션      : %s\n' "$(psql 'SELECT count(*) FROM sessions;')"
echo
echo "기기별 계정 수 (상위 5개) - 숫자가 크면 매크로 의심"
docker compose -f "$COMPOSE" exec -T postgres psql -U ticketing -d ticketing -c \
"SELECT device_fp AS 기기, count(DISTINCT account_id) AS 계정수
 FROM sessions WHERE device_fp IS NOT NULL
 GROUP BY device_fp ORDER BY 2 DESC LIMIT 5;"

echo "계정 나이 분포 - 0일짜리가 뭉쳐 있으면 매크로 의심"
docker compose -f "$COMPOSE" exec -T postgres psql -U ticketing -d ticketing -c \
"SELECT CASE
          WHEN age_days = 0 THEN '0일 (오늘 생성)'
          WHEN age_days < 30 THEN '1~29일'
          WHEN age_days < 180 THEN '30~179일'
          ELSE '180일 이상'
        END AS 계정나이,
        count(*) AS 계정수
 FROM (SELECT (EXTRACT(EPOCH FROM (now() - created_at)) / 86400)::int AS age_days
       FROM users) t
 GROUP BY 1 ORDER BY 2 DESC;"

echo
echo "로그인 테스트용 계정"
echo "  정상  user001@doon.kr / $PW"
echo "  매크로 bot001@doon.kr / $PW"
