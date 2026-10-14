#!/usr/bin/env bash
# 격리 실험판 띄우기 + 계정 시드. 라이브(docker-compose.pangyo.yml)는 건드리지 않는다.
#   SIM_FILTER_MODE=enforce bash sim/up.sh   # 1차 필터를 실제 차단으로 (기본 monitor)
set -eu
cd "$(dirname "$0")/.."
mkdir -p sim/.gen
# 라이브 nginx 설정(git HEAD) 그대로 + 두 가지만 바꾼다.
#  1) upstream 을 각 PoP 로
#  2) 시뮬레이터가 보낸 X-Forwarded-For 를 '실제 접속 IP'로 믿는다.
#     가상 사람 수십 명이 한 컴퓨터에서 나가므로, 이게 없으면 전부 같은 IP 로 보여
#     v0 의 IP 빈도 제한에 사람이 걸린다. (실험판 전용. 라이브에 쓰면 IP 위조가 된다)
for p in pangyo daejeon; do
  git show HEAD:nginx/default.conf \
    | sed "s/pop-pangyo:8000/pop-$p:8000/g; s/^\(\s*\)listen 80;/&\n\1set_real_ip_from 0.0.0.0\/0;\n\1real_ip_header X-Forwarded-For;/" \
    > sim/.gen/nginx-$p.conf
done
docker compose -f sim/docker-compose.yml up -d --build
echo "central 준비 대기..."
for i in $(seq 1 60); do curl -sf -m 2 localhost:18001/auth/stats >/dev/null && break; sleep 2; done
COMPOSE=sim/docker-compose.yml CENTRAL=localhost:18001 NORMAL_N=${NORMAL_N:-60} bash scripts/seed_accounts.sh
echo "판교 PoP http://172.17.0.1:18198   대전 PoP http://172.17.0.1:18199"
