#!/usr/bin/env bash
# 실험판 detection-log 를 서울 보관 형식(날짜/시.jsonl.gz, _kafka 필드)으로 꺼낸다.
# 그래서 seoul/round_report.py 를 그대로 쓸 수 있다.
#   bash sim/dump_log.sh [OUT=sim/out]
#   python3 seoul/round_report.py bots/rounds.jsonl --archive sim/out
set -eu
cd "$(dirname "$0")/.."
OUT=${1:-sim/out}
rm -rf "${OUT:?}" && mkdir -p "$OUT/all"
docker compose -f sim/docker-compose.yml exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic detection-log --from-beginning --timeout-ms 5000 \
  --property print.partition=true --property print.offset=true 2>/dev/null \
| python3 -c '
import sys, json, gzip
n = 0
with gzip.open(sys.argv[1], "wt") as f:
    for line in sys.stdin:
        p, o, v = line.rstrip("\n").split("\t", 2)
        r = json.loads(v)
        r["_kafka"] = {"partition": int(p.split(":")[1]), "offset": int(o.split(":")[1])}
        f.write(json.dumps(r, ensure_ascii=False) + "\n"); n += 1
print(n, "건 ->", sys.argv[1])
' "$OUT/all/00.jsonl.gz"
