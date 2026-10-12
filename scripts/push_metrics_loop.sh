#!/bin/bash
# 대전 PoP의 /metrics 를 5초마다 판교 Pushgateway로 밀어 넣는다.
# localhost:13091 은 hirun 셸에서 띄운 중계기다. 중계기가 죽으면 push도 실패하지만
# 루프는 계속 돌게 두어, 중계기가 살아나면 자동으로 재개된다.
while true; do
  BODY=$(curl -s --max-time 5 http://localhost:13198/metrics)
  if [ -n "$BODY" ]; then
    printf '%s' "$BODY" | curl -s --max-time 5 --data-binary @- \
      http://localhost:13091/metrics/job/pop-daejeon/instance/pop-daejeon \
      -o /tmp/push_body.txt -w '%{http_code}' > /tmp/push_code.txt
  fi
  sleep 5
done
