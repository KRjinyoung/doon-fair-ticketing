"""
consume_test.py -- 서울(치타) pod에서 detection-log 를 읽어보는 연결 확인용 소비자.

판교 Kafka 는 EXTERNAL 주소를 'kafka-external:13092' 라는 이름으로 광고한다.
서울 pod 는 /etc/hosts 를 쓸 수 없으므로, 이 프로세스 안에서만 그 이름을
역방향 SSH 터널 입구(127.0.0.1)로 돌린다.

    pip install kafka-python
    python3 consume_test.py            # 최근 메시지 몇 건 출력 후 종료
"""
import json
import os
import socket

TUNNEL_HOST = os.environ.get("TUNNEL_HOST", "127.0.0.1")
_orig_getaddrinfo = socket.getaddrinfo


def _getaddrinfo(host, *args, **kwargs):
    if host == "kafka-external":
        host = TUNNEL_HOST
    return _orig_getaddrinfo(host, *args, **kwargs)


socket.getaddrinfo = _getaddrinfo

from kafka import KafkaConsumer  # noqa: E402  (패치 이후에 import)

consumer = KafkaConsumer(
    os.environ.get("KAFKA_TOPIC_DETECTION", "detection-log"),
    bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP", "kafka-external:13092"),
    group_id=None,                 # 확인용: 오프셋을 커밋하지 않는다
    auto_offset_reset="earliest",
    consumer_timeout_ms=10000,     # 10초 동안 새 메시지가 없으면 종료
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
)

n = 0
for msg in consumer:
    n += 1
    if n <= 5:
        print(msg.partition, msg.offset, msg.value)
print(f"총 {n}건 읽음")
