#!/usr/bin/env python3
"""
수집 회차 진행기 — 사람과 봇이 같은 '오픈'에 붙게 한다  [ROUNDS_V1]  (판교에서 실행)

  python3 bots/rounds.py --every 2 --rounds 10

회차 시각 = every 분의 배수 정각 (seat.html?every=2 카운트다운과 같은 규칙).
매 회차 오픈 40초 전에 봇 3종을 띄우고, 오픈 정각에 쏘게 한다.
  simple    : 단순 API 봇 (v0 에 걸림)
  disguised : 헤더 위장 API 봇 (v0 통과, Referer 없음)
  stealth   : 진짜 크롬 + stealth (v0 통과, 헤더도 브라우저와 같음)
사람은 같은 시각에 각자 기기에서 seat.html?every=2 를 열고 사람 계정(user..)으로 누른다.

격리 실험판에서는 사람도 시뮬레이터로 넣는다 (sim/humans.py, 회차마다 같은 오픈 시각):
  python3 bots/rounds.py --base http://172.17.0.1:18198,http://172.17.0.1:18199 \
      --every 1 --rounds 15 --copies 3 --humans 30 --out sim/rounds.jsonl --yes
  --base 를 여러 개 주면 봇을 번갈아 나눠 보낸다. --copies K 는 봇 종류마다 K개씩.

결과는 --out (기본 bots/rounds.jsonl) 에 회차마다 한 줄:
  {"round", "open_at_ms", "open_at", "bots": [각 봇 출력 — account_id 포함], "humans": [...]}
서울 학습에서 정답표로 쓴다: bots 의 account_id = 봇, 그 밖의 로그인 계정 = 사람.
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
IMAGE = "doon-browser-bot"

# 봇 종류 → script_bot.py 인자. 종류마다 다른 단계에서 잡히도록 행동을 나눈다.  [MACRO3_V1]
#   simple   : 파이썬 이름표                                   → 1차 User-Agent
#   farm     : 위장 이름표, IP 바꿈, 기기 하나를 여러 계정이 같이 씀 → 2차 기기당 계정 수
#   hoarder  : 위장, 기기·IP 바꿈, 좌석 3개 선점 후 결제 안 함  → 2차 미결제 선점
#   disguised: 위장, 기기·IP 바꿈, 이기면 결제                  → 3차 AI
#   poller   : disguised + 좌석 4개를 균등 랜덤 간격으로 시도    → 3차 AI (요청 간격 분포)
#   stealth  : 진짜 크롬 + stealth 플러그인, IP 바꿈, 결제       → 3차 AI
TYPES = {
    "simple":    [],
    "farm":      ["--disguise", "--xff", "random", "--label", "farm"],
    "hoarder":   ["--disguise", "--device-fp", "random", "--xff", "random", "--seats", "3", "--no-pay", "--label", "hoarder"],
    "disguised": ["--disguise", "--device-fp", "random", "--xff", "random"],
    "poller":    ["--disguise", "--device-fp", "random", "--xff", "random", "--attempts", "4", "--sleep", "0.3", "1.2", "--label", "poller"],
    "stealth":   None,   # 브라우저 봇
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=int, default=2, help="회차 간격(분). seat.html?every= 와 같게")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--first", type=int, default=1, help="회차 번호 시작 (같은 --out 에 이어 붙일 때)")
    ap.add_argument("--base", default="http://localhost:13198", help="봇이 겨눌 PoP (쉼표로 여러 개)")
    ap.add_argument("--copies", type=int, default=1, help="봇 종류마다 몇 개씩")
    ap.add_argument("--bot-from", type=int, default=1, help="봇 계정 번호 시작 (botNN)")
    ap.add_argument("--bot-count", type=int, default=30, help="돌려 쓸 봇 계정 수")
    ap.add_argument("--humans", type=int, default=0, help="시뮬레이터 사람 수 (격리 실험판 전용)")
    ap.add_argument("--types", default="simple,disguised,stealth",
                    help="봇 종류 (쉼표): " + ",".join(TYPES))
    ap.add_argument("--out", default=os.path.join(HERE, "rounds.jsonl"))
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()

    print(f"대상 {a.base} / {a.every}분마다 {a.rounds}회 / 기록 {a.out}")
    if not a.yes and input("이 사이트에 봇을 보냅니다. 진행할까요? (y/N) ").strip().lower() != "y":
        sys.exit("취소")
    if subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode:
        subprocess.run(["docker", "build", "-q", "-t", IMAGE, HERE], check=True)

    period = a.every * 60
    for r in range(a.first, a.first + a.rounds):
        open_at = (int(time.time() + 45) // period + 1) * period  # 브라우저 봇 준비 시간 확보
        at = datetime.fromtimestamp(open_at).strftime("%H:%M:%S.000")
        time.sleep(max(0, open_at - 40 - time.time()))
        print(f"[{r}/{a.first + a.rounds - 1}] {at} 오픈 — 봇 출발")

        bases = a.base.split(",")
        types = a.types.split(",")
        n = len(types) * a.copies
        procs = []
        for k in range(n):
            email, base = f"bot{(n * (r - 1) + k) % a.bot_count + a.bot_from:02d}@doon.kr", bases[k % len(bases)]
            kind = types[k % len(types)]
            if TYPES[kind] is None:
                ip = f"10.{100 + k}.{r % 250}.{k + 1}"
                cmd = ["docker", "run", "--rm", "--network", "host", "-e", f"BASE={base}", "-e", f"AT={at}",
                       "-e", f"EMAIL={email}", "-e", "STEALTH=1", "-e", f"XFF={ip}", IMAGE]
            else:
                cmd = [sys.executable, os.path.join(HERE, "script_bot.py"), "--base", base, "--at", at,
                       "--email", email] + TYPES[kind]
            procs.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True))
        hp = a.humans and subprocess.Popen(
            [sys.executable, os.path.join(HERE, "..", "sim", "humans.py"), "--open-at", str(open_at),
             "--n", str(a.humans), "--bases", a.base], stdout=subprocess.PIPE, text=True)
        bots = []
        for p in procs:
            out = p.communicate(timeout=180)[0].strip().splitlines()
            bots.append(json.loads(out[-1]) if out and out[-1].startswith("{") else {"error": p.returncode})
        humans = json.loads(hp.communicate(timeout=120)[0].strip().splitlines()[-1])["humans"] if hp else []
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(json.dumps({"round": r, "open_at_ms": open_at * 1000,
                                "open_at": datetime.fromtimestamp(open_at).isoformat(),
                                "bots": bots, "humans": humans}, ensure_ascii=False) + "\n")
        for b in bots:
            print(f"    {b.get('bot', '?'):16} {b.get('account', '')}  {b.get('status', b.get('error'))}")
        if humans:
            print(f"    사람 {sum(1 for h in humans if h['sent'])}/{len(humans)}명 참여, 요청 {sum(len(h['sent']) for h in humans)}건")


if __name__ == "__main__":
    main()
