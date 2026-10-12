#!/bin/bash
#
# PoP 컨테이너에게 chrony의 클럭 오프셋을 전달하는 호스트 측 수집기.
#
# 왜 이게 필요한가:
#   PoP 컨테이너는 자기 시계 오차를 판정서버에 실어 보내야 한다. 그 값은 호스트의
#   chrony가 이미 정확하게 알고 있다(여러 샘플을 필터링한 추정치).
#   그런데 컨테이너의 localhost는 호스트가 아니라서 chronyd에 직접 못 닿고,
#   chronyd의 명령 포트를 도커 브리지에 열려면 호스트 chrony.conf를 고치고
#   재시작해야 한다(PoP 호스트마다 전부).
#   그래서 방향을 뒤집는다: 호스트가 값을 파일로 내려주고, 컨테이너는 그 파일을
#   읽기전용으로 마운트해서 읽는다. chrony.conf 변경도, 이미지에 패키지 추가도,
#   네트워크 노출도 필요 없다.
#
# 왜 컨테이너 안에서 직접 NTP 서버에 물어보지 않는가:
#   해봤고, 측정으로 기각했다. 단발 SNTP 질의는 지터가 표준편차 105us
#   (최대-최소 338us)인데 chrony의 필터링된 추정치는 RMS 19.7us다. 게다가 단발
#   질의의 평균은 chrony 추정치와 166us 어긋나 있었다(네트워크 비대칭이 걸러지지 않아서).
#   PoP 두 곳이 각자 단발 질의를 하면 상대 오차가 최대 0.5ms까지 벌어져서,
#   1ms 미만의 승부는 우리 측정 노이즈가 결정해버린다. 공정성을 위해 넣은 보정이
#   오히려 무작위성을 주입하는 셈이 된다. 시간 측정은 시간 데몬에게 맡기는 게 맞다.
#
# 설치 (PoP를 띄우는 모든 호스트에서):
#   sudo install -m 755 scripts/chrony-offset-writer.sh /usr/local/bin/
#   sudo install -m 644 scripts/pop-clock-offset.service /etc/systemd/system/
#   sudo systemctl daemon-reload
#   sudo systemctl enable --now pop-clock-offset.service
#
set -u

OUT="${1:-/run/pop-clock/tracking.csv}"
# PoP가 5초 주기로 이 값을 읽어가므로 같은 주기로 갱신한다.
INTERVAL="${INTERVAL:-5}"
TMP="${OUT}.tmp"

while true; do
    # *** 원자적 갱신 (temp에 쓰고 mv) ***
    # 컨테이너가 아무 때나 읽어가므로, 파일에 직접 쓰면 절반만 쓰인 줄을 읽을 수 있다.
    # mv는 같은 파일시스템 안에서 원자적이라 읽는 쪽은 항상 완전한 줄만 본다.
    #
    # *** 실패하면 일부러 OUT을 건드리지 않는다 ***
    # chronyc가 실패했을 때 빈 파일로 덮어쓰거나 mtime만 갱신하면, 컨테이너는
    # "방금 측정된 값"으로 오해한다. 그냥 놔두면 mtime이 멈춰서 컨테이너가 계산하는
    # age가 계속 커지고, 그게 곧 중앙 판정서버까지 전달되는 장애 신호가 된다.
    if chronyc -c tracking > "$TMP" 2>/dev/null && [ -s "$TMP" ]; then
        mv -f "$TMP" "$OUT"
    fi
    sleep "$INTERVAL"
done
