#!/bin/bash
#
# 대전 호스트에서 판교로 가는 socat 중계기 (대전에서 실행).
#
# 왜 중계기가 필요한가:
#   대전은 판교 사설망(10.246.246.19)에 직접 갈 수 없고 hirun 프록시를 거쳐야만 닿는다.
#   컨테이너는 프록시를 물려받지 못하므로, hirun 셸에서 띄운 socat 이 대전 호스트에서
#   포트를 열어두고 판교로 이어준다. 컨테이너는 172.17.0.1:<포트> 로 붙는다
#   (docker-compose.daejeon.yml 참고).
#
# 사용법:
#   ~/hirun -c bash                          # 1) 먼저 hirun 셸을 연다
#   bash scripts/daejeon_relay.sh start      # 2) 그 안에서 시작 (이미 떠 있는 포트는 건너뜀)
#   exit                                     # 3) hirun 셸을 닫아도 중계기는 살아 있다
#
#   bash scripts/daejeon_relay.sh status     # 포트와 판교 응답 확인 (아무 셸에서나)
#   bash scripts/daejeon_relay.sh stop       # 전부 종료
#
# 재부팅되면 중계기는 사라진다. 그때 위 1)~3)을 다시 하면 된다.

PANGYO=10.246.246.19

# 대전포트:판교포트  용도
RELAYS="
13092:13092  Kafka
10001:10001  central
13091:13091  pushgateway (대전 지표)
14198:13198  판교 nginx
14300:13300  Grafana
"

listening() { ss -tln | grep -q ":$1 "; }

case "${1:-status}" in
  start)
    echo "$RELAYS" | while read -r spec name; do
      [ -z "$spec" ] && continue
      L=${spec%%:*}; R=${spec##*:}
      if listening "$L"; then
        echo "  [유지] $L -> $R  $name"
      else
        nohup socat TCP-LISTEN:$L,fork,reuseaddr,backlog=1024 TCP:$PANGYO:$R \
          >/tmp/socat_$L.log 2>&1 &
        echo "  [시작] $L -> $R  $name"
      fi
    done
    sleep 2
    "$0" status
    ;;
  status)
    echo "$RELAYS" | while read -r spec name; do
      [ -z "$spec" ] && continue
      L=${spec%%:*}
      listening "$L" && echo "  [열림] $L  $name" || echo "  [없음] $L  $name"
    done
    # 포트가 열려 있어도 hirun 밖에서 띄웠으면 판교까지 못 간다. 실제 응답으로 확인한다.
    echo -n "  central     : "; curl -s --max-time 5 localhost:10001/health || echo '응답 없음'; echo
    echo -n "  pushgateway : "; curl -s --max-time 5 localhost:13091/-/healthy || echo '응답 없음'; echo
    ;;
  stop)
    pkill -f "socat TCP-LISTEN:.*TCP:$PANGYO:" && echo '중계기 종료' || echo '떠 있는 중계기 없음'
    ;;
  *)
    echo "사용법: $0 [start|status|stop]"; exit 1
    ;;
esac
