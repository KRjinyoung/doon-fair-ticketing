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

결과는 --out (기본 bots/rounds.jsonl) 에 회차마다 한 줄:
  {"round", "open_at_ms", "open_at", "bots": [각 봇 출력 — account_id 포함]}
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--every", type=int, default=2, help="회차 간격(분). seat.html?every= 와 같게")
    ap.add_argument("--rounds", type=int, default=10)
    ap.add_argument("--base", default="http://localhost:13198", help="봇이 겨눌 PoP (판교 nginx)")
    ap.add_argument("--out", default=os.path.join(HERE, "rounds.jsonl"))
    ap.add_argument("--yes", action="store_true")
    a = ap.parse_args()

    print(f"대상 {a.base} / {a.every}분마다 {a.rounds}회 / 기록 {a.out}")
    if not a.yes and input("이 사이트에 봇을 보냅니다. 진행할까요? (y/N) ").strip().lower() != "y":
        sys.exit("취소")
    if subprocess.run(["docker", "image", "inspect", IMAGE], capture_output=True).returncode:
        subprocess.run(["docker", "build", "-q", "-t", IMAGE, HERE], check=True)

    period = a.every * 60
    for r in range(1, a.rounds + 1):
        open_at = (int(time.time() + 45) // period + 1) * period  # 브라우저 봇 준비 시간 확보
        at = datetime.fromtimestamp(open_at).strftime("%H:%M:%S.000")
        time.sleep(max(0, open_at - 40 - time.time()))
        print(f"[{r}/{a.rounds}] {at} 오픈 — 봇 출발")

        acct = [f"bot{(3 * (r - 1) + k) % 30 + 1:02d}@doon.kr" for k in range(3)]
        script = [sys.executable, os.path.join(HERE, "script_bot.py"), "--base", a.base, "--at", at]
        procs = [
            subprocess.Popen(script + ["--email", acct[0]], stdout=subprocess.PIPE, text=True),
            subprocess.Popen(script + ["--email", acct[1], "--disguise"], stdout=subprocess.PIPE, text=True),
            subprocess.Popen(["docker", "run", "--rm", "--network", "host", "-e", f"BASE={a.base}",
                              "-e", f"AT={at}", "-e", f"EMAIL={acct[2]}", "-e", "STEALTH=1", IMAGE],
                             stdout=subprocess.PIPE, text=True),
        ]
        bots = []
        for p in procs:
            out = p.communicate(timeout=120)[0].strip().splitlines()
            bots.append(json.loads(out[-1]) if out and out[-1].startswith("{") else {"error": p.returncode})
        with open(a.out, "a", encoding="utf-8") as f:
            f.write(json.dumps({"round": r, "open_at_ms": open_at * 1000,
                                "open_at": datetime.fromtimestamp(open_at).isoformat(),
                                "bots": bots}, ensure_ascii=False) + "\n")
        for b in bots:
            print(f"    {b.get('bot', '?'):16} {b.get('account', '')}  {b.get('status', b.get('error'))}")


if __name__ == "__main__":
    main()
