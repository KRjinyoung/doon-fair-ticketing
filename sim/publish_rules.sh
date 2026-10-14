#!/usr/bin/env bash
# 실험판 PoP 들에 규칙 묶음을 배포한다 (seoul/publish_rules.py 의 실험판판: 서울 Kafka 대신 실험판 Kafka).
#   bash sim/publish_rules.sh rules.json 운영자이름
# 되돌리기: echo '{"version":"v1-rollback","rules":[]}' > r.json && bash sim/publish_rules.sh r.json 운영자
set -eu
cd "$(dirname "$0")/.."
F=$1; WHO=${2:?승인자 이름}; T=$(mktemp)
python3 - "$F" "$WHO" <<'PY' > "$T"
import json, sys
sys.path.insert(0, "pop"); import bot_rules
r = json.load(open(sys.argv[1], encoding="utf-8")); r["approved_by"] = sys.argv[2]
bot_rules.validate(r)
for x in r["rules"]:
    print(f"  [{x['id']}] {x.get('why', '')}  — 계정 {len(x['all'][0].get('v', []))}개" if x["all"][0]["op"] == "in" else f"  [{x['id']}] {x}", file=sys.stderr)
print("ruleset|" + json.dumps(r, ensure_ascii=False))
PY
docker compose -f sim/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic bot-rules --property parse.key=true --property key.separator='|' \
  < "$T" 2>/dev/null
rm -f "$T"
sleep 3
for p in 18198 18199; do curl -s 172.17.0.1:$p/bot-filter | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  PoP :$p 모드 {d[\"mode\"]} / 규칙 {d[\"ruleset_version\"]}')"; done
