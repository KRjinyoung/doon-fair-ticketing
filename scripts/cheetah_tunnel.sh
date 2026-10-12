#!/bin/bash
#
# 판교 Kafka를 서울 AI Cloud(Cheetah) pod에 열어주는 역방향 SSH 터널.
#
# 왜 역방향인가:
#   Cheetah에서는 판교 Kafka(10.246.246.19:13092)로 직접 나갈 수 없고,
#   판교에서는 Cheetah의 SSH에 들어갈 수 있다. 그래서 판교가 Cheetah에 접속하면서
#   "Cheetah의 localhost:13092 → 판교 localhost:13092(Kafka)"를 열어준다.
#   Kafka는 kafka-external:13092 로 광고하므로 Cheetah 쪽 클라이언트는 그 이름을
#   127.0.0.1로 바꿔서 쓴다 (seoul/consume_test.py 참고).
#
# 사용법 (판교에서):
#   bash scripts/cheetah_tunnel.sh          터널 시작 (이미 떠 있으면 아무것도 안 함)
#   bash scripts/cheetah_tunnel.sh status   떠 있는지 확인
#   bash scripts/cheetah_tunnel.sh stop     터널 종료
#
# 재부팅되거나 연결이 끊기면 터널은 사라진다. 그때 다시 실행하면 된다.
# 키 파일(~/.ssh/cheetah_tunnel)은 저장소에 넣지 않는다.

KEY=$HOME/.ssh/cheetah_tunnel
HOST=jovyan@116.89.177.90
PORT=32261
FWD=13092:localhost:13092
LOG=$HOME/cheetah_tunnel.log

# 이 터널의 ssh 프로세스만 정확히 집어내는 패턴
PATTERN="ssh .*-R $FWD $HOST"

case "${1:-start}" in
  status)
    if pgrep -af "$PATTERN"; then echo '터널 동작 중'; else echo '터널 없음'; exit 1; fi
    ;;
  stop)
    pkill -f "$PATTERN" && echo '터널 종료' || echo '떠 있는 터널 없음'
    ;;
  start)
    if pgrep -f "$PATTERN" >/dev/null; then
      echo '이미 동작 중:'; pgrep -af "$PATTERN"; exit 0
    fi
    [ -f "$KEY" ] || { echo "키 없음: $KEY"; exit 1; }
    nohup ssh -i "$KEY" -p "$PORT" -N \
      -o BatchMode=yes -o ExitOnForwardFailure=yes \
      -o ServerAliveInterval=15 -o ServerAliveCountMax=3 \
      -R "$FWD" "$HOST" >"$LOG" 2>&1 &
    sleep 3
    if pgrep -f "$PATTERN" >/dev/null; then
      echo '터널 시작됨'; pgrep -af "$PATTERN"
    else
      echo "터널 시작 실패 — $LOG 확인:"; cat "$LOG"; exit 1
    fi
    ;;
  *)
    echo "사용법: $0 [start|status|stop]"; exit 1
    ;;
esac
