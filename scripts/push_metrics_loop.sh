#!/bin/bash
# 대전 PoP의 /metrics 를 5초마다 판교 Pushgateway로 밀어 넣는다.
# localhost:13091 은 hirun 셸에서 띄운 중계기다. 중계기가 죽으면 push도 실패하지만
# 루프는 계속 돌게 두어, 중계기가 살아나면 자동으로 재개된다.
#
# 결과를 /tmp 파일로 남기지 않는다. 서비스는 root 로 도는데, 다른 계정이 만든
# /tmp 파일에는 root 도 쓸 수 없어서(fs.protected_regular) 리다이렉트가 실패하고,
# 그러면 bash 가 curl 자체를 실행하지 않는다. 실패는 journalctl 로 확인한다.
#
# $(...) 는 끝 줄바꿈을 지운다. Pushgateway 는 마지막 줄바꿈이 없으면 400
# (unexpected end of input stream)으로 거부하므로 \n 을 다시 붙여 보낸다.
while true; do
  BODY=$(curl -s --max-time 5 http://localhost:13198/metrics)
  if [ -n "$BODY" ]; then
    CODE=$(printf '%s\n' "$BODY" | curl -s --max-time 5 --data-binary @- -o /dev/null -w '%{http_code}' \
      http://localhost:13091/metrics/job/pop-daejeon/instance/pop-daejeon)
    [ "$CODE" = 200 ] || echo "push 실패 (HTTP $CODE)"
  fi
  sleep 5
done
