#!/usr/bin/env bash
# 격리 실험판을 흔적 없이 지운다 (컨테이너, 네트워크, 볼륨, 빌드한 이미지).
cd "$(dirname "$0")/.."
docker compose -f sim/docker-compose.yml down -v --rmi local
rm -rf sim/.gen
