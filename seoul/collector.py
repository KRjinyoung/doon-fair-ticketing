"""
collector.py -- 서울(치타) pod 에서 detection-log 를 계속 읽어 home 볼륨에 보관하는 수집기.

판교 Kafka 의 detection-log 보존 기간은 7일이다. 학습·검증에 쓸 원본 로그는
여기서 빠짐없이 옮겨 두어야 한다. 가공은 하지 않고 원본 그대로 저장한다.

[저장 위치]
  $ARCHIVE_DIR (기본 ~/data/detection-log)/YYYY-MM-DD/HH.jsonl.gz
  날짜·시각은 Kafka 메시지 타임스탬프 기준(KST). 한 줄 = 메시지 하나.
  각 줄에는 원본 필드 + "_kafka": {"partition", "offset", "ts_ms"} 가 붙는다.
  JSON 이 아닌 메시지는 같은 폴더의 HH.bad.jsonl.gz 에 원문 그대로 남긴다.

[유실·중복]
  파일에 쓰고 fsync 한 뒤에만 오프셋을 커밋한다 (at-least-once).
  수집기가 중간에 죽으면 마지막 커밋 이후 메시지가 다시 저장될 수 있으므로,
  읽을 때 (_kafka.partition, _kafka.offset) 로 중복을 제거한다.

[연결]
  판교 Kafka 는 자기 주소를 'kafka-external:13092' 로 광고한다. 서울 pod 는
  /etc/hosts 를 쓸 수 없으므로, 이 프로세스 안에서만 그 이름을 역방향 SSH
  터널 입구(127.0.0.1)로 돌린다.

[실행]
    pip install kafka-python
    nohup python3 collector.py > ~/collector.log 2>&1 &
  같은 보관 폴더로 두 개가 동시에 돌지 않도록 잠금 파일을 잡는다.
  pod 가 재시작되면 프로세스는 사라지므로 다시 실행해야 한다
  (오프셋은 Kafka 에 커밋되어 있어 이어서 읽는다).

[읽기 예시]
    import gzip, json, glob
    rows = [json.loads(l) for f in sorted(glob.glob("~/data/detection-log/*/*[0-9].jsonl.gz"))
            for l in gzip.open(f, "rt")]
"""
import fcntl
import gzip
import json
import os
import signal
import socket
import sys
import time
from datetime import datetime, timedelta, timezone

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka-external:13092")
TOPIC = os.environ.get("KAFKA_TOPIC_DETECTION", "detection-log")
GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "seoul-ai-detection")
TUNNEL_HOST = os.environ.get("TUNNEL_HOST", "127.0.0.1")
ARCHIVE_DIR = os.path.expanduser(os.environ.get("ARCHIVE_DIR", "~/data/detection-log"))
FLUSH_EVERY_N = int(os.environ.get("FLUSH_EVERY_N", "2000"))
FLUSH_EVERY_SEC = float(os.environ.get("FLUSH_EVERY_SEC", "5"))
KST = timezone(timedelta(hours=9))

_orig_getaddrinfo = socket.getaddrinfo


def _getaddrinfo(host, *args, **kwargs):
    if host == "kafka-external":
        host = TUNNEL_HOST
    return _orig_getaddrinfo(host, *args, **kwargs)


socket.getaddrinfo = _getaddrinfo

from kafka import KafkaConsumer  # noqa: E402  (패치 이후에 import)
from kafka.errors import KafkaError  # noqa: E402

_stop = False


def _on_signal(signum, frame):
    global _stop
    _stop = True


def _append_gz(path: str, lines: list) -> None:
    """gzip 멤버 하나를 이어 붙이고 디스크까지 내린다. 여러 멤버로 된 파일도 gzip 으로 그대로 읽힌다."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "ab") as f:
        with gzip.GzipFile(fileobj=f, mode="wb") as gz:
            gz.write("".join(lines).encode("utf-8"))
        f.flush()
        os.fsync(f.fileno())


def _flush(buffers: dict, consumer, stats: dict) -> None:
    if not buffers:
        return
    for path, lines in buffers.items():
        _append_gz(path, lines)
    buffers.clear()
    consumer.commit()  # 파일에 안전하게 쓴 뒤에만 커밋
    stats["flushed_at"] = datetime.now(KST).isoformat(timespec="seconds")
    _write_status(stats)


def _write_status(stats: dict) -> None:
    tmp = os.path.join(ARCHIVE_DIR, "_status.json.tmp")
    with open(tmp, "w") as f:
        json.dump(stats, f, ensure_ascii=False, indent=1)
    os.replace(tmp, os.path.join(ARCHIVE_DIR, "_status.json"))


def _consume(stats: dict) -> None:
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",  # 처음 붙을 때 남아 있는 것부터 전부
    )
    print(f"[collector] 연결됨: {BOOTSTRAP} topic={TOPIC} group={GROUP_ID}", flush=True)
    buffers: dict = {}
    pending = 0
    last_flush = time.monotonic()
    try:
        while not _stop:
            batch = consumer.poll(timeout_ms=1000, max_records=1000)
            for records in batch.values():
                for m in records:
                    ts = datetime.fromtimestamp(m.timestamp / 1000, KST)
                    base = os.path.join(ARCHIVE_DIR, ts.strftime("%Y-%m-%d"), ts.strftime("%H"))
                    meta = {"partition": m.partition, "offset": m.offset, "ts_ms": m.timestamp}
                    try:
                        row = json.loads(m.value)
                        row["_kafka"] = meta
                        path = base + ".jsonl.gz"
                    except (ValueError, TypeError):
                        row = {"raw": m.value.decode("utf-8", "replace"), "_kafka": meta}
                        path = base + ".bad.jsonl.gz"
                        stats["bad"] += 1
                    buffers.setdefault(path, []).append(json.dumps(row, ensure_ascii=False) + "\n")
                    stats["saved"] += 1
                    stats["last"][str(m.partition)] = m.offset
                    pending += 1
            if pending and (pending >= FLUSH_EVERY_N or time.monotonic() - last_flush >= FLUSH_EVERY_SEC):
                _flush(buffers, consumer, stats)
                print(f"[collector] {pending}건 저장 (누적 {stats['saved']})", flush=True)
                pending = 0
                last_flush = time.monotonic()
        _flush(buffers, consumer, stats)
    finally:
        consumer.close(autocommit=False)


def main() -> None:
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    lock = open(os.path.join(ARCHIVE_DIR, ".collector.lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        sys.exit(f"[collector] 이미 실행 중이다 ({ARCHIVE_DIR})")

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)
    stats = {"started_at": datetime.now(KST).isoformat(timespec="seconds"),
             "saved": 0, "bad": 0, "last": {}, "flushed_at": None, "last_error": None}

    # 터널이 끊기거나 판교가 재시작돼도 프로세스는 살아서 다시 붙는다.
    while not _stop:
        try:
            _consume(stats)
        except (KafkaError, OSError) as e:
            stats["last_error"] = f"{datetime.now(KST).isoformat(timespec='seconds')} {e!r}"
            _write_status(stats)
            print(f"[collector] 연결 오류, 10초 뒤 재시도: {e!r}", flush=True)
            for _ in range(10):
                if _stop:
                    break
                time.sleep(1)
    print(f"[collector] 종료. 누적 {stats['saved']}건", flush=True)


if __name__ == "__main__":
    main()
