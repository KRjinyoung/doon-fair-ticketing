#!/usr/bin/env python3
"""
auto_rules.py -- 3차(서울) AI 자동 반영 루프: 회차가 끝날 때마다 다시 배우고, 스스로 검사하고, 바로 배포한다.

  python auto_rules.py --rounds rounds.jsonl --labels-upto 20 [--bootstrap kafka:9092]

[한 회차마다]
  1) detection-log 를 계속 받아 둔다 (Kafka 를 처음부터 읽고 계속 따라간다).
  2) rounds.jsonl 에 새 회차가 기록되면 (= 회차가 끝나면):
       계정마다 회차 간 도착 흩어짐 등 시간 분포 특징을 다시 계산하고 (train_rules.py 와 같은 함수)
       정답이 있는 과거 회차(1~labels-upto)로 모델을 다시 맞춰 깊이 3 규칙으로 압축한다.
       → 그 규칙에 걸리는 계정 = 처음 보는 계정이어도 '시간 분포가 기계 같은' 계정.
  3) 자동 검사 (사람 승인 대신): 정답이 사람인 계정이 하나라도 걸리면 배포하지 않고 이전 규칙을 유지.
  4) 통과하면 bot-rules 로 바로 발행 → 전국 PoP 가 1초 안에 교체 → 다음 회차부터 차단.
  사람의 역할은 승인이 아니라 비상 정지: 빈 규칙 묶음을 발행하면 즉시 v0 으로 돌아간다.

[정답을 어디까지 쓰나]  labels-upto 이후 회차의 봇 정답은 학습에 쓰지 않는다.
  그 뒤에 나타난 새 봇 계정은 오직 시간 분포만으로 잡혀야 한다.

출력: 회차마다 한 줄 로그 + --log (jsonl) 에 배포 기록 (그래프용).
"""
import argparse
import json
import os
import socket
import sys
import threading
import time
from datetime import datetime

import numpy as np

sys.path[:0] = [os.path.dirname(os.path.abspath(__file__)),
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pop")]
import bot_rules  # noqa: E402
import train_rules as T  # noqa: E402

# 서울 pod 에서 돌 때: 판교 Kafka 가 광고하는 이름을 역방향 터널 입구로 돌린다 (collector.py 와 같음)
if os.environ.get("TUNNEL_HOST"):
    _orig = socket.getaddrinfo
    socket.getaddrinfo = lambda h, *a, **k: _orig(os.environ["TUNNEL_HOST"] if h == "kafka-external" else h, *a, **k)

from kafka import KafkaConsumer, KafkaProducer  # noqa: E402

LOG = []          # detection-log 전부 (소비 스레드가 채운다)
LOCK = threading.Lock()


def follow(bootstrap):
    c = KafkaConsumer("detection-log", bootstrap_servers=bootstrap, group_id=None,
                      auto_offset_reset="earliest", value_deserializer=lambda v: json.loads(v))
    for m in c:
        with LOCK:
            LOG.append(m.value)


def learn(rounds, labels_upto, cols):
    with LOCK:
        log = list(LOG)
    reqs = T.requests_by_round(rounds, log, 30)
    known = {}  # 정답이 있는 과거 회차의 계정 -> 봇?
    for rd in rounds:
        if rd["round"] <= labels_upto:
            for _, _, who, r in T.requests_by_round([rd], log, 30):
                known[r["account_id"]] = who != "human"
    feats = T.account_features(reqs)
    judged = [k for k, f in feats.items() if f["n_rounds"] >= T.MIN_ROUNDS]
    train = [k for k in judged if k in known]
    X = lambda ks: np.array([[feats[k][c] for c in cols] for k in ks], dtype=float)
    y = np.array([int(known[k]) for k in train])
    _, small = T.fit(X(train), y)
    flagged = sorted(k for k, p in zip(judged, small.predict(X(judged))) if p == 1)
    humans_hit = [k for k in flagged if known.get(k) is False]
    return flagged, humans_hit, T.tree_text(small, cols), set(feats) - set(judged)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rounds", required=True)
    ap.add_argument("--labels-upto", type=int, required=True)
    ap.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    ap.add_argument("--features", default="행동만", choices=list(T.SETS))
    ap.add_argument("--log", default="auto_rules.jsonl")
    a = ap.parse_args()
    cols = T.SETS[a.features]

    threading.Thread(target=follow, args=(a.bootstrap,), daemon=True).start()
    producer = KafkaProducer(bootstrap_servers=a.bootstrap)
    seen, version, current = 0, 0, None
    print(f"[서울 AI] 자동 반영 시작 — 특징 {a.features}, 정답은 {a.labels_upto}회차까지만 사용", flush=True)
    while True:
        rounds = [json.loads(l) for l in open(a.rounds) if l.strip()]
        if len(rounds) == seen:
            time.sleep(1)
            continue
        seen = len(rounds)
        time.sleep(3)  # 마지막 detection-log 가 들어올 틈
        t0 = time.time()
        flagged, humans_hit, rule, pending = learn(rounds, a.labels_upto, cols)
        took = time.time() - t0
        last = rounds[-1]["round"]
        rec = {"after_round": last, "at": datetime.now().isoformat(timespec="seconds"), "rule": rule,
               "flagged": len(flagged), "humans_hit": len(humans_hit), "pending": len(pending)}
        if humans_hit:
            rec["deployed"] = False
            print(f"[서울 AI] {last}회차 후 재학습: 사람 {len(humans_hit)}명이 걸려 배포 보류 (이전 규칙 유지)", flush=True)
        elif flagged == current:
            rec["deployed"] = False
            print(f"[서울 AI] {last}회차 후 재학습 {took:.1f}초: 변화 없음 (걸린 계정 {len(flagged)}, 관찰 부족 {len(pending)})", flush=True)
        else:
            version += 1
            ruleset = bot_rules.validate({
                "version": f"v{version}-ai-auto-r{last}", "approved_by": "auto-check",
                "approved_at": rec["at"],
                "rules": [{"id": "ai-timing", "why": " / ".join(rule) + f" ({T.MIN_ROUNDS}회 이상)",
                           "all": [{"f": "account_id", "op": "in", "v": flagged}]}]})
            producer.send("bot-rules", key=b"ruleset", value=json.dumps(ruleset, ensure_ascii=False).encode()).get(10)
            new = len(flagged) - len(current or [])
            rec |= {"deployed": True, "version": ruleset["version"], "new": new, "seconds": round(time.time() - t0, 2)}
            print(f"[서울 AI] {last}회차 후 재학습 → 자동 검사 통과(사람 0명) → {ruleset['version']} 전국 배포 "
                  f"{rec['seconds']}초 | 규칙: {' / '.join(rule)} | 걸린 계정 {len(flagged)} (새로 {new:+d})", flush=True)
            current = flagged
        with open(a.log, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")


if __name__ == "__main__":
    main()
