#!/bin/bash
# ===========================================================================
# 대전 PoP 시계 보정 배포
# *** 반드시 대전 VM의 "일반 셸"에서 실행 (hirun 셸 아님) ***
#   hirun 셸 안에서는 172.17.0.1로 나가는 연결이 프록시를 타서 실패한다.
#   중계기(socat)를 띄운 hirun 창은 그대로 열어둔 채, 별도 일반 셸에서 실행.
#
# 하는 일:
#   1) 판교에서 배포 파일 4개를 받아온다 (nginx 경유, 체크섬 검증)
#   2) chrony.conf를 판교와 동일하게 맞춘다
#   3) 호스트 수집기(systemd)를 설치한다
#   4) pop_server.py를 교체한다
#   5) compose에 오프셋 파일 마운트를 추가한다
#   6) 재빌드하고 검증한다
#
# 실패하면 그 자리에서 멈추고, 무엇을 되돌려야 하는지 알려준다.
# ===========================================================================
set -u

SRC=http://172.17.0.1:14198/_deploy
REPO=$HOME/koren-ticketing
COMPOSE=$REPO/docker-compose.daejeon.yml
WORK=/tmp/djclock

die() { echo ''; echo "[중단] $1"; echo ''; exit 1; }
ok()  { echo "  [완료] $1"; }

echo '###########################################################'
echo '# 0. 사전 점검'
echo '###########################################################'
[ -d "$REPO" ] || die "저장소 없음: $REPO"
[ -f "$COMPOSE" ] || die "compose 파일 없음: $COMPOSE"
command -v docker >/dev/null || die 'docker 없음'
command -v python3 >/dev/null || die 'python3 없음'

# hirun 셸에서 실행했는지 감지 (172.17.0.1이 안 나가면 프록시 셸이다)
if ! curl -fsS -o /dev/null --max-time 10 "$SRC/pop_server.py" 2>/dev/null; then
  echo ''
  echo '판교에 도달할 수 없습니다. 아래를 순서대로 확인하세요:'
  echo ''
  echo '  1) 이 셸이 hirun 셸이 아닌지 (hirun 안에서는 172.17.0.1이 막힙니다)'
  echo '  2) 중계기가 살아있는지:  ss -tln | grep 14198'
  echo '  3) 판교에서 _deploy 공개를 했는지 (1단계 스크립트)'
  die '판교 도달 실패'
fi
ok '판교 도달 확인'

echo ''
echo '###########################################################'
echo '# 1. 배포 파일 수신'
echo '###########################################################'
rm -rf "$WORK"; mkdir -p "$WORK"; cd "$WORK" || die 'tmp 생성 실패'
for f in pop_server.py chrony-offset-writer.sh pop-clock-offset.service SHA256; do
  curl -fsS --max-time 30 -o "$f" "$SRC/$f" || die "다운로드 실패: $f"
  echo "  받음: $f ($(wc -c < "$f") bytes)"
done
sha256sum -c SHA256 >/dev/null 2>&1 || die '체크섬 불일치 — 전송이 깨졌습니다. 다시 실행하세요'
ok "체크섬 일치 (pop_server.py $(wc -l < pop_server.py)줄)"

echo ''
echo '###########################################################'
echo '# 2. chrony.conf 를 판교와 동일하게'
echo '###########################################################'
CONF=/etc/chrony/chrony.conf
[ -f "$CONF" ] || CONF=/etc/chrony.conf
[ -f "$CONF" ] || die 'chrony.conf 를 찾을 수 없음'
echo "  대상: $CONF"

echo '  --- 변경 전 ---'
grep -hE '^[[:space:]]*(server|pool)[[:space:]]' "$CONF" | sed 's/^/    /'

BK="$CONF.bak.$(date +%Y%m%d_%H%M%S)"
sudo cp "$CONF" "$BK" || die 'chrony.conf 백업 실패 (sudo 권한 확인)'
echo "  백업: $BK"

# 기존 server/pool 줄을 전부 주석 처리한다.
# 지우지 않고 주석으로 남기는 이유: 되돌릴 때 원본을 보고 판단할 수 있어야 한다.
sudo sed -i -E 's/^([[:space:]]*(server|pool)[[:space:]].*)$/#[doon] \1/' "$CONF"

# 판교와 글자 하나까지 동일해야 한다.
#   - 121이 유일한 선택 대상 (noselect 없음)
#   - 211은 감시만 하고 선택은 안 함 (noselect)
# 두 PoP가 다른 서버를 기준으로 삼으면 그 차이가 그대로 판정 편향이 되므로,
# 자동 페일오버(둘 다 선택 가능)보다 "반드시 같은 서버"를 우선한다.
printf '%s\n' \
  '' \
  '# === DOON: 판교 PoP와 동일한 시각 기준 (2026-08-15) ===' \
  '# 두 PoP가 서로 다른 NTP 서버를 보면 그 차이(실측 약 153us)가' \
  '# 그대로 좌석 판정의 지역 편향이 된다. 반드시 판교와 같아야 한다.' \
  '# 211에 noselect가 붙어 있으므로 자동 페일오버는 되지 않는다(의도적).' \
  '# 121이 죽으면 양쪽에서 수동으로 noselect를 옮겨야 한다.' \
  'server 121.134.215.104 iburst minpoll 4 maxpoll 6' \
  'server 211.108.117.211 iburst minpoll 4 maxpoll 6 noselect' \
  | sudo tee -a "$CONF" >/dev/null

# sources.d / conf.d 에 다른 서버가 숨어있으면 위 설정이 무의미해진다.
for d in /etc/chrony/sources.d /etc/chrony/conf.d /run/chrony-dhcp; do
  if [ -d "$d" ] && [ -n "$(ls -A "$d" 2>/dev/null)" ]; then
    echo "  [주의] $d 에 파일이 있습니다 — 다른 NTP 서버가 섞일 수 있습니다:"
    ls -A "$d" | sed 's/^/      /'
  fi
done

sudo systemctl restart chrony 2>/dev/null || sudo systemctl restart chronyd 2>/dev/null \
  || die 'chrony 재시작 실패'
echo '  동기화 대기 (15초)...'
sleep 15

REF=$(chronyc tracking 2>/dev/null | awk -F'[()]' '/Reference ID/{print $2}')
echo "  Reference ID: ${REF:-(없음)}"
if [ "$REF" = '121.134.215.104' ]; then
  ok 'chrony 기준 서버 일치'
else
  echo '  [주의] 아직 121.134.215.104 가 아닙니다.'
  echo '         30초쯤 뒤 chronyc tracking 으로 다시 확인하세요.'
  echo '         계속 다르면 UDP 123 이 막힌 것일 수 있습니다.'
fi
chronyc sources 2>/dev/null | sed 's/^/    /'

echo ''
echo '###########################################################'
echo '# 3. 호스트 수집기 설치'
echo '###########################################################'
cd "$WORK" || die 'cd 실패'
sudo install -m 755 chrony-offset-writer.sh /usr/local/bin/ || die '수집기 설치 실패'
sudo install -m 644 pop-clock-offset.service /etc/systemd/system/ || die '유닛 설치 실패'
sudo systemctl daemon-reload
sudo systemctl enable --now pop-clock-offset || die '유닛 기동 실패'
echo '  첫 기록 대기 (8초)...'
sleep 8
[ -s /run/pop-clock/tracking.csv ] || die '/run/pop-clock/tracking.csv 가 생성되지 않음 (systemctl status pop-clock-offset 확인)'
OFFS=$(cut -d, -f5 /run/pop-clock/tracking.csv)
ok "수집기 동작 중 (현재 오차 ${OFFS}초)"

echo ''
echo '###########################################################'
echo '# 4. pop_server.py 교체'
echo '###########################################################'
PBK="$REPO/pop/pop_server.py.bak.$(date +%Y%m%d_%H%M%S)"
cp "$REPO/pop/pop_server.py" "$PBK" || die 'pop_server 백업 실패'
echo "  백업: $PBK"
cp "$WORK/pop_server.py" "$REPO/pop/pop_server.py" || die '복사 실패'
python3 -c "import ast; ast.parse(open('$REPO/pop/pop_server.py').read())" \
  || die "문법 오류 — cp $PBK $REPO/pop/pop_server.py 로 되돌리세요"
ok "교체 완료 ($(wc -l < "$REPO/pop/pop_server.py")줄)"

echo ''
echo '###########################################################'
echo '# 5. compose 에 오프셋 파일 마운트 추가'
echo '###########################################################'
CBK="$COMPOSE.bak.$(date +%Y%m%d_%H%M%S)"
cp "$COMPOSE" "$CBK" || die 'compose 백업 실패'
echo "  백업: $CBK"

python3 - "$COMPOSE" << 'PYEOF'
import sys, re
path = sys.argv[1]
src = open(path).read()
lines = src.split('\n')

has_env  = any('CLOCK_OFFSET_FILE' in l for l in lines)
has_mnt  = any('/run/pop-clock' in l for l in lines)

if has_env and has_mnt:
    print('  이미 적용되어 있음 (건너뜀)')
    sys.exit(0)

# pop-daejeon 서비스의 environment 블록을 찾는다.
env_idx = None
for i, l in enumerate(lines):
    if re.match(r'^\s+- KAFKA_BROKER=', l):
        env_idx = i
        break
if env_idx is None:
    print('  [실패] KAFKA_BROKER 줄을 찾지 못했습니다'); sys.exit(1)

item_ind = re.match(r'^(\s*)', lines[env_idx]).group(1)   # 예: 6칸
svc_ind  = item_ind[:-2] if len(item_ind) >= 2 else ''     # 예: 4칸

out = []
i = 0
while i < len(lines):
    out.append(lines[i])
    if i == env_idx:
        if not has_env:
            out.append(item_ind + '# 호스트 수집기가 5초마다 갱신하는 chrony 오프셋 파일.')
            out.append(item_ind + '- CLOCK_OFFSET_FILE=/clock/tracking.csv')
        # environment 블록의 끝을 찾아 그 뒤에 volumes 를 넣는다.
        j = i + 1
        while j < len(lines) and (lines[j].startswith(item_ind) or lines[j].strip() == ''):
            # 다음 서비스/키가 시작되면 멈춘다
            if lines[j].strip() and not lines[j].startswith(item_ind):
                break
            out.append(lines[j])
            j += 1
        if not has_mnt:
            out.append(svc_ind + '# *** 이 마운트가 없으면 오프셋이 0.0 으로 고정되어')
            out.append(svc_ind + '# *** 대전만 보정을 못 받는 조용한 불공정이 발생한다.')
            out.append(svc_ind + 'volumes:')
            out.append(item_ind + '- /run/pop-clock:/clock:ro')
        i = j - 1
    i += 1

open(path, 'w').write('\n'.join(out))
print('  수정 완료')
PYEOF
RC=$?
[ $RC -eq 0 ] || { cp "$CBK" "$COMPOSE"; die 'compose 수정 실패 — 백업으로 복구했습니다'; }

docker compose -f "$COMPOSE" config >/dev/null 2>&1 \
  || { cp "$CBK" "$COMPOSE"; die 'compose 문법 오류 — 백업으로 복구했습니다'; }
ok 'compose 검증 통과'
echo '  --- 적용된 부분 ---'
grep -n -i -B1 -A1 'clock' "$COMPOSE" | sed 's/^/    /'

echo ''
echo '###########################################################'
echo '# 6. 재빌드'
echo '###########################################################'
docker compose -f "$COMPOSE" up -d --build || die '재빌드 실패'
echo '  기동 대기 (20초)...'
sleep 20

echo ''
echo '###########################################################'
echo '# 7. 검증'
echo '###########################################################'
docker ps --format '  {{.Names}}  {{.Status}}' | grep -E 'pop-daejeon|nginx'
echo ''
echo -n '  /clock-offset : '
curl -s --max-time 5 localhost:13198/clock-offset; echo
echo ''
echo '  --- 컨테이너 로그 ---'
docker logs pop-daejeon 2>&1 | grep -i '클럭 오프셋' | tail -3 | sed 's/^/    /'
echo ''
echo '  --- 마운트 확인 ---'
docker exec pop-daejeon head -c 120 /clock/tracking.csv 2>&1 | sed 's/^/    /'; echo

echo ''
echo '###########################################################'
echo '# 판정'
echo '###########################################################'
VAL=$(curl -s --max-time 5 localhost:13198/clock-offset | grep -o '"clock_offset_ms":[-0-9.]*' | cut -d: -f2)
echo "  clock_offset_ms = ${VAL:-(읽기 실패)}"
echo ''
ABS=$(echo "${VAL:-999}" | tr -d -)
if [ -n "$ABS" ] && [ "$(awk -v v="$ABS" 'BEGIN{print (v<1)?1:0}' 2>/dev/null)" = '1' ]; then
  echo '  성공 — 1ms 미만입니다. chrony 값이 정상적으로 들어왔습니다.'
  echo ''
  echo '  다음: 판교에서 3단계(정리 + 재빌드)를 실행하세요.'
else
  echo '  실패 — 12~14ms 대이면 옛날 코드가 그대로이거나 마운트가 안 된 것입니다.'
  echo '  확인:  docker exec pop-daejeon ls -l /clock/'
  echo '         docker logs pop-daejeon 2>&1 | tail -30'
  echo ''
  echo '  이 상태에서는 절대 판교를 재빌드하지 마세요 (대전이 불리해집니다).'
fi
echo ''
