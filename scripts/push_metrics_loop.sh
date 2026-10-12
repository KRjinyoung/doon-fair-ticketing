#!/bin/bash
# 대전 PoP의 /metrics 를 5초마다 판교 Pushgateway로 밀어 넣는다 (대전에서 실행).
#
#   nohup bash scripts/push_metrics_loop.sh >/dev/null 2>&1 &
#
# 주의: 이 스크립트는 hirun 셸에서 띄운 중계기(localhost:13091)를 통해 나간다
#       (scripts/daejeon_relay.sh). 중계기가 죽으면 push 도 같이 실패하므로,
#       실패해도 루프는 계속 돌게 두고 중계기가 살아나면 자동으로 재개되도록 한다.
#       PoP 가 꺼져 있으면 /metrics 가 비어 있어 push 를 건너뛴다.
while true; do
  # pop-daejeon 컨테이너의 지표를 대전 nginx를 통해 가져온다.
  BODY=$(curl -s --max-time 5 http://localhost:13198/metrics)
  if [ -n "$BODY" ]; then
    # Pushgateway는 job/instance 를 URL 경로로 받는다.
    # 여기서 넣은 job=pop-daejeon 라벨은 Prometheus 쪽 honor_labels: true 덕분에
    # 덮어써지지 않고 그대로 유지된다.
    echo "$BODY" | curl -s --max-time 5 --data-binary @- \
      http://localhost:13091/metrics/job/pop-daejeon/instance/116.89.187.140 \
      -o /tmp/push_result.txt -w '%{http_code}' > /tmp/push_code.txt
  fi
  sleep 5
done
