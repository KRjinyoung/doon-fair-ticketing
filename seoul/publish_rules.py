"""
승인된 봇 규칙 묶음을 전국 PoP 에 배포한다.  [BOTRULES_V1]

  python3.11 publish_rules.py rules.json          # 내용을 보여주고 y 를 받아야 보낸다
  python3.11 publish_rules.py rules.json --yes    # 확인 없이 보낸다 (스크립트용)

되돌리기: {"version": "v1-rollback", "rules": []} 를 같은 방법으로 보낸다.
규칙 형식과 검사는 pop/bot_rules.py 와 같은 코드를 쓴다 (같은 폴더에 두거나 ../pop).

'bot-rules' 토픽은 compact 로 만든다. 기본 보존(7일)이면 일주일 뒤 규칙이
사라져 PoP 가 재시작할 때 v0 으로 돌아간다. compact 는 키별 마지막 1건을 영구 보관한다.
"""

import json
import os
import socket
import sys
from datetime import datetime, timedelta, timezone

sys.path[:0] = [os.path.dirname(os.path.abspath(__file__)),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pop")]
import bot_rules  # noqa: E402

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-external:13092")
TUNNEL_HOST = os.environ.get("TUNNEL_HOST", "127.0.0.1")
TOPIC = "bot-rules"

# collector.py 와 같은 이유: 판교 Kafka 가 광고하는 이름을 터널 입구로 돌린다.
_orig_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda host, *a, **k: _orig_getaddrinfo(
    TUNNEL_HOST if host == "kafka-external" else host, *a, **k)

from kafka import KafkaProducer  # noqa: E402
from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType  # noqa: E402
from kafka.errors import TopicAlreadyExistsError  # noqa: E402


def main():
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    ruleset = json.load(open(sys.argv[1], encoding="utf-8"))
    ruleset.setdefault("approved_at", datetime.now(timezone(timedelta(hours=9))).isoformat())
    bot_rules.validate(ruleset)

    print(f"버전 {ruleset['version']} / 규칙 {len(ruleset['rules'])}개")
    for r in ruleset["rules"]:
        conds = " 그리고 ".join(f"{c['f']} {c['op']} {c.get('v', '')}".strip() for c in r["all"])
        print(f"  [{r['id']}] {conds}   ({r.get('why', '')})")
    if "--yes" not in sys.argv and input("전국 PoP 에 배포할까요? (y/N) ").strip().lower() != "y":
        sys.exit("취소")

    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    compact = {"cleanup.policy": "compact"}
    try:
        admin.create_topics([NewTopic(TOPIC, 1, 1, topic_configs=compact)])
    except TopicAlreadyExistsError:
        admin.alter_configs([ConfigResource(ConfigResourceType.TOPIC, TOPIC, configs=compact)])
    admin.close()

    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    meta = producer.send(TOPIC, key=b"ruleset",
                         value=json.dumps(ruleset, ensure_ascii=False).encode()).get(timeout=10)
    producer.close()
    print(f"배포 완료 (offset {meta.offset}). PoP 상태 확인: GET /bot-filter")


if __name__ == "__main__":
    main()
