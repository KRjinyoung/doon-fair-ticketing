"""
서울 AI 가 만든 봇 규칙을 PoP 에서 실행하는 모듈  [BOTRULES_V1]

흐름
  서울: detection-log 로 학습 -> 사람이 읽을 수 있는 규칙 -> 운영자 승인
        -> Kafka 'bot-rules' 토픽에 규칙 묶음 1건 발행 (seoul/publish_rules.py)
  PoP : 그 토픽을 구독하다가 새 묶음이 오면 통째로 교체하고,
        좌석 요청마다 규칙을 돌린다 (차단 여부는 BOT_FILTER_MODE 가 정한다).

규칙 묶음 형식 (JSON)
  {"version": "v1-...", "approved_by": "...", "approved_at": "...",
   "rules": [{"id": "r1", "why": "좌석 페이지를 거치지 않음",
              "all": [{"f": "headers.values.referer", "op": "missing"},
                      {"f": "identity_verified", "op": "eq", "v": false}]}]}
  규칙 하나 = 조건들이 '모두' 맞으면 봇. 묶음 안의 규칙은 하나라도 맞으면 봇.
  f 는 detection-log 한 줄 안의 경로다. 그래서 서울은 쌓인 로그에 같은 함수를
  돌려 "이 규칙이면 몇 건을 잡고 몇 건을 오탐하나"를 배포 전에 잴 수 있다.

안전장치
  - 형식이 틀린 묶음은 통째로 버리고 지금 규칙을 유지한다.
  - 로그에 없는 칸을 가리키면 그 조건은 '안 맞음'이다 (애매하면 통과).
  - 되돌리기 = rules 가 빈 묶음을 새 version 으로 발행.
"""

OPS = {
    "present":  lambda x, v: x is not None,
    "missing":  lambda x, v: x is None,
    "eq":       lambda x, v: x == v,
    "ne":       lambda x, v: x is not None and x != v,
    "in":       lambda x, v: x in v,
    "contains": lambda x, v: x is not None and v in x,   # 문자열 또는 목록(헤더 순서)
    "lt":       lambda x, v: x is not None and x < v,
    "le":       lambda x, v: x is not None and x <= v,
    "gt":       lambda x, v: x is not None and x > v,
    "ge":       lambda x, v: x is not None and x >= v,
}

EMPTY = {"version": "v0-hardcoded", "rules": []}


def _get(record: dict, path: str):
    x = record
    for k in path.split("."):
        if not isinstance(x, dict):
            return None
        x = x.get(k)
    return x


def validate(ruleset: dict) -> dict:
    """형식이 맞으면 그대로 돌려주고, 아니면 ValueError."""
    if not isinstance(ruleset, dict) or not isinstance(ruleset.get("version"), str):
        raise ValueError("version 이 없다")
    rules = ruleset.get("rules")
    if not isinstance(rules, list):
        raise ValueError("rules 가 목록이 아니다")
    for r in rules:
        if not isinstance(r.get("id"), str) or not r.get("all"):
            raise ValueError(f"규칙 형식 오류: {r}")
        for c in r["all"]:
            if c.get("op") not in OPS or not isinstance(c.get("f"), str):
                raise ValueError(f"조건 형식 오류: {c}")
    return ruleset


def match(ruleset: dict, record: dict):
    """맞은 첫 규칙의 id, 없으면 None. 조건 하나가 예외를 내면 그 규칙은 안 맞은 것으로 본다."""
    for r in ruleset["rules"]:
        try:
            if all(OPS[c["op"]](_get(record, c["f"]), c.get("v")) for c in r["all"]):
                return r["id"]
        except Exception:
            continue
    return None


if __name__ == "__main__":
    # 자체 점검:  python bot_rules.py
    rs = validate({"version": "t", "rules": [
        {"id": "no_referer_anon", "all": [{"f": "headers.values.referer", "op": "missing"},
                                          {"f": "identity_verified", "op": "eq", "v": False}]},
        {"id": "bad", "all": [{"f": "account_age_days", "op": "lt", "v": "x"}]}]})
    bot = {"headers": {"values": {}}, "identity_verified": False, "account_age_days": 3}
    human = {"headers": {"values": {"referer": "http://x/seat.html"}}, "identity_verified": False}
    assert match(rs, bot) == "no_referer_anon" and match(rs, human) is None
    try:
        validate({"version": "t", "rules": [{"id": "x", "all": [{"f": "a", "op": "rm -rf"}]}]})
        raise AssertionError("잘못된 op 를 통과시킴")
    except ValueError:
        pass
    print("ok")
