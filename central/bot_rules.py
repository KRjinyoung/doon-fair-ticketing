"""
※ pop/bot_rules.py 와 같은 파일이다 (central 빌드 폴더에 복사본). 고칠 때 둘 다 고친다.

서울 AI 가 만든 봇 규칙 묶음을 1차(PoP)·2차(중앙)에서 실행하는 모듈  [BOTRULES_V1] [MACRO3_V1]

흐름
  서울: detection-log 로 학습 -> 계정별 봇 확률 -> 명단 + 규칙 + 1·2차 한도 값
        -> Kafka 'bot-rules' 토픽에 규칙 묶음 1건 발행
  PoP·중앙: 그 토픽을 구독하다가 새 묶음이 오면 통째로 교체한다.

규칙 묶음 형식 (JSON)  — docs/research/12-macro-3layer.md §2
  {"version": "v1-...", "approved_by": "...", "approved_at": "...",
   "params":    {"l1": {...}, "l2": {...}},          # 1·2차 한도 값. 빠진 값은 기본값
   "blacklist": {"accounts": [...], "devices": [...]},  # 3차가 봇으로 본 계정·기기
   "rules": [{"id": "r1", "layer": 1, "mode": "enforce", "why": "좌석 페이지를 거치지 않음",
              "all": [{"f": "headers.values.referer", "op": "missing"},
                      {"f": "identity_verified", "op": "eq", "v": false}]}]}
  규칙 하나 = 조건들이 '모두' 맞으면 봇. layer 1 = PoP(요청 한 건의 값), layer 2 = 중앙(집계 값도 볼 수 있음).
  mode: enforce = 막는다, shadow = 막지 않고 '막았을 것'만 기록한다 (정교화의 그림자 배포).
  f 는 detection-log 한 줄(2차는 집계 값 agg.* 를 더한 것) 안의 경로다. 그래서 서울은 쌓인 로그에
  같은 함수를 돌려 "이 규칙이면 몇 건을 잡고 몇 건을 오탐하나"를 배포 전에 잴 수 있다.

안전장치
  - 형식이 틀린 묶음은 통째로 버리고 지금 규칙을 유지한다.
  - 로그에 없는 칸을 가리키면 그 조건은 '안 맞음'이다 (애매하면 통과).
  - 되돌리기 = 빈 묶음(rules·blacklist 없음)을 새 version 으로 발행 → 기본 한도 + v0 규칙만 남는다.
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

EMPTY = {"version": "v0-hardcoded", "rules": [], "params": {}, "blacklist": {"accounts": [], "devices": []}}


def _get(record: dict, path: str):
    x = record
    for k in path.split("."):
        if not isinstance(x, dict):
            return None
        x = x.get(k)
    return x


def validate(ruleset: dict) -> dict:
    """형식이 맞으면 빠진 칸을 채워 돌려주고, 아니면 ValueError."""
    if not isinstance(ruleset, dict) or not isinstance(ruleset.get("version"), str):
        raise ValueError("version 이 없다")
    rules = ruleset.get("rules", [])
    if not isinstance(rules, list):
        raise ValueError("rules 가 목록이 아니다")
    for r in rules:
        if not isinstance(r.get("id"), str) or not r.get("all"):
            raise ValueError(f"규칙 형식 오류: {r}")
        if r.setdefault("layer", 1) not in (1, 2):
            raise ValueError(f"layer 는 1 또는 2: {r}")
        if r.setdefault("mode", "enforce") not in ("enforce", "shadow"):
            raise ValueError(f"mode 는 enforce 또는 shadow: {r}")
        for c in r["all"]:
            if c.get("op") not in OPS or not isinstance(c.get("f"), str):
                raise ValueError(f"조건 형식 오류: {c}")
    params = ruleset.setdefault("params", {})
    if not isinstance(params, dict):
        raise ValueError("params 는 객체")
    bl = ruleset.setdefault("blacklist", {})
    if not isinstance(bl, dict):
        raise ValueError("blacklist 는 객체")
    for k in ("accounts", "devices"):
        if not isinstance(bl.setdefault(k, []), list):
            raise ValueError(f"blacklist.{k} 는 목록")
    ruleset["rules"] = rules
    return ruleset


_sets: dict = {}   # 명단은 요청마다 찾으므로 묶음마다 한 번 집합으로 바꿔 둔다 (묶음 자체는 JSON 그대로)


def _blsets(ruleset: dict):
    key = id(ruleset)
    got = _sets.get(key)
    if got is None or got[0] is not ruleset:
        bl = ruleset.get("blacklist") or {}
        got = (ruleset, frozenset(bl.get("accounts", ())), frozenset(bl.get("devices", ())))
        _sets.clear()
        _sets[key] = got
    return got[1], got[2]


def _hit(rule: dict, record: dict) -> bool:
    try:
        return all(OPS[c["op"]](_get(record, c["f"]), c.get("v")) for c in rule["all"])
    except Exception:
        return False   # 조건 하나가 예외를 내면 그 규칙은 안 맞은 것으로 본다


def evaluate(ruleset: dict, record: dict, layer: int):
    """
    한 요청을 이 단계(layer)의 명단과 규칙에 대 본다.
    반환: (막을 사유 또는 None, 그림자 규칙 중 맞은 id 목록)
      사유 = "blacklist:account" | "blacklist:device" | "rule:<id>"
    """
    acct, dev = record.get("account_id"), record.get("device_fp")
    bl_accounts, bl_devices = _blsets(ruleset)
    if acct and acct in bl_accounts:
        return "blacklist:account", []
    if dev and dev in bl_devices:
        return "blacklist:device", []
    block, shadow = None, []
    for r in ruleset["rules"]:
        if r.get("layer", 1) != layer or not _hit(r, record):
            continue
        if r.get("mode", "enforce") == "shadow":
            shadow.append(r["id"])
        elif block is None:
            block = f"rule:{r['id']}"
    return block, shadow


def match(ruleset: dict, record: dict):
    """맞은 첫 enforce 규칙의 id, 없으면 None. (예전 호출부·서울 재실행용, 단계 구분 없이)"""
    for r in ruleset["rules"]:
        if r.get("mode", "enforce") == "enforce" and _hit(r, record):
            return r["id"]
    return None


if __name__ == "__main__":
    # 자체 점검:  python bot_rules.py
    rs = validate({"version": "t", "rules": [
        {"id": "no_referer_anon", "all": [{"f": "headers.values.referer", "op": "missing"},
                                          {"f": "identity_verified", "op": "eq", "v": False}]},
        {"id": "young", "mode": "shadow", "all": [{"f": "account_age_days", "op": "lt", "v": 1}]},
        {"id": "farm", "layer": 2, "all": [{"f": "agg.device_accounts", "op": "gt", "v": 3}]},
        {"id": "bad", "all": [{"f": "account_age_days", "op": "lt", "v": "x"}]}],
        "blacklist": {"accounts": ["acc-bot"]}})
    bot = {"headers": {"values": {}}, "identity_verified": False, "account_age_days": 0}
    human = {"headers": {"values": {"referer": "http://x/seat.html"}}, "identity_verified": False}
    assert match(rs, bot) == "no_referer_anon" and match(rs, human) is None
    assert evaluate(rs, bot, 1) == ("rule:no_referer_anon", ["young"])
    assert evaluate(rs, {"account_id": "acc-bot"}, 2) == ("blacklist:account", [])
    assert evaluate(rs, {"agg": {"device_accounts": 5}}, 2) == ("rule:farm", [])
    assert evaluate(rs, {"agg": {"device_accounts": 5}}, 1) == (None, [])
    for bad in ({"version": "t", "rules": [{"id": "x", "all": [{"f": "a", "op": "rm -rf"}]}]},
                {"version": "t", "rules": [{"id": "x", "layer": 3, "all": [{"f": "a", "op": "eq"}]}]}):
        try:
            validate(bad)
            raise AssertionError("잘못된 묶음을 통과시킴")
        except ValueError:
            pass
    import json
    json.dumps(rs)   # 검사한 묶음을 그대로 다시 발행할 수 있어야 한다
    print("ok")
