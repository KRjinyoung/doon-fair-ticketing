"""DOON 네트워크 구성도 — 좌표를 직접 잡은 SVG (doon_network.svg / .html)
설명은 보고서 본문에 두고, 그림에는 이름과 핵심 낱말만 크게 적는다."""
import base64
import os

HERE = os.path.dirname(os.path.abspath(__file__))
W, H = 1760, 990
NAVY, ORANGE, GREEN, GREY, INK, SUB = "#1F3A93", "#D35400", "#1E8449", "#8A94A6", "#1B2230", "#3F4A5C"
FONT = "'Noto Sans CJK KR', sans-serif"
F_BOX, F_COMP, F_SUB, F_TAG, F_LABEL = 28, 26, 22, 20, 22


def icon(name):
    return "data:image/png;base64," + base64.b64encode(open(f"{HERE}/icons/{name}.png", "rb").read()).decode()


out = []
DA_BOX, DA_ARROW = 'stroke-dasharray="10 7"', 'stroke-dasharray="9 7"'
MS = 'marker-start="url(#s{})"'


def text_w(s, size):
    """글자 폭 어림값 (한글은 글자 크기만큼, 나머지는 절반 남짓)"""
    return sum(size if ord(c) > 0x1100 else size * 0.55 for c in s)


def txt(x, y, s, size=F_SUB, color=SUB, bold=False, anchor="start"):
    fw = 'font-weight="700" ' if bold else ""
    out.append(f'<text x="{x}" y="{y}" font-size="{size}" {fw}fill="{color}" text-anchor="{anchor}">{s}</text>')


def box(x, y, w, h, color, title, right=False):
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="16" fill="none" stroke="{color}" '
               f'stroke-width="2.6" {DA_BOX}/>')
    if right:
        txt(x + w - 20, y + 40, title, F_BOX, color, True, "end")
    else:
        txt(x + 20, y + 40, title, F_BOX, color, True)


def tag(x, top, label, color, w=None):
    """꼬리표 (매크로 1·2·3차 등). 반환: 꼬리표 폭"""
    w = w or text_w(label, F_TAG) + 24
    fill = "#FDEDE3" if color == ORANGE else "#E8EDF8"
    out.append(f'<rect x="{x}" y="{top}" width="{w}" height="36" rx="8" fill="{fill}" stroke="{color}" stroke-width="1.6"/>')
    txt(x + w / 2, top + 25, label, F_TAG, color, True, "middle")
    return w


def comp(x, y, icons, title, lines=(), size=48, fs=F_COMP):
    """아이콘 + 이름 (+ 짧은 설명). 설명 줄이 (꼬리표, 색) 이면 꼬리표"""
    for i, n in enumerate(icons):
        out.append(f'<image href="{icon(n)}" x="{x + i * (size + 6)}" y="{y}" width="{size}" height="{size}"/>')
    tx = x + len(icons) * (size + 6) + 10
    ty = y + fs if lines else y + size / 2 + fs * 0.36
    txt(tx, ty, title, fs, INK, True)
    cy = ty + 6                         # 바로 위 줄의 아래 끝
    for l in lines:
        if isinstance(l, tuple):
            tag(tx, cy + 8, *l)
            cy += 44
        else:
            txt(tx, cy + 28, l)
            cy += 34


def pill(x, y, w, color, s):
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="46" rx="10" fill="white" stroke="{color}" stroke-width="2.2"/>')
    out.append(f'<text x="{x + w / 2}" y="{y + 31}" font-size="22" text-anchor="middle" fill="{INK}" '
               f'font-family="monospace" font-weight="700">{s}</text>')


def arrow(pts, color, label=None, lx=None, ly=None, dash=False, width=3, both=False, anchor="middle"):
    d = "M " + " L ".join(f"{a},{b}" for a, b in pts)
    m = color.strip("#")
    out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" '
               f'{DA_ARROW if dash else ""} marker-end="url(#a{m})" '
               f'{MS.format(m) if both else ""}/>')
    if label:
        for k, s in enumerate([label] if isinstance(label, str) else label):
            out.append(f'<text x="{lx}" y="{ly + k * 27}" font-size="{F_LABEL}" font-weight="700" fill="{color}" '
                       f'text-anchor="{anchor}" paint-order="stroke" stroke="white" stroke-width="7">{s}</text>')


# ---------- 프로젝트 제목 (홈페이지 상단 문구 그대로)
out.append(f'<text x="24" y="62"><tspan font-size="52" font-weight="700" fill="{INK}">DOON</tspan>'
           f'<tspan dx="18" font-size="28" fill="{SUB}">티켓팅에 공정을 담다.</tspan>'
           f'<tspan dx="10" font-size="28" font-weight="700" fill="{NAVY}">with KOREN</tspan></text>')

# ---------- 사용자 · PoP 배정 · 시계
PX, PW, PH = 310, 390, 360              # PoP 상자
PY = {"pangyo": 104, "daejeon": 504}    # PoP 상자 위쪽
NY = 94                                 # PoP 위쪽 → nginx 아이콘 가운데

comp(24, 96, ["users"], "수도권 사용자", size=62)
arrow([(96, PY["pangyo"] + NY), (326, PY["pangyo"] + NY)], GREY, "① 좌석 요청", 211, PY["pangyo"] + NY - 12)

comp(24, 556, ["users"], "지방 사용자", ["(대전)"], size=62)
arrow([(246, PY["daejeon"] + NY), (326, PY["daejeon"] + NY)], GREY, "①", 286, PY["daejeon"] + NY - 12)

out.append(f'<rect x="16" y="300" width="270" height="104" rx="14" fill="#F7F9FC" stroke="{GREY}" stroke-width="2"/>')
out.append(f'<path d="M42,322 a13,13 0 0 1 13,13 c0,10 -13,23 -13,23 c0,0 -13,-13 -13,-23 a13,13 0 0 1 13,-13 z" '
           f'fill="{NAVY}"/><circle cx="42" cy="335" r="5" fill="white"/>')
txt(66, 342, "doon.land", F_COMP, INK, True)
txt(66, 380, "가까운 PoP 연결")
arrow([(55, 166), (55, 294)], GREY, "⓪ PoP 배정", 70, 238, dash=True, width=2.6, both=True, anchor="start")
arrow([(55, 410), (55, 550)], GREY, None, dash=True, width=2.6, both=True)

comp(24, 900, ["internet"], "외부 NTP 시계", size=48, fs=24)

# ---------- PoP 두 곳 (매크로 1차)
for key, city in (("pangyo", "판교"), ("daejeon", "대전")):
    y0 = PY[key]
    box(PX, y0, PW, PH, NAVY, f"KOREN {city} PoP")
    comp(334, y0 + NY - 24, ["nginx"], "nginx")
    comp(334, y0 + 140, ["fastapi"], f"pop-{key}", ["µs 도착 시각 스탬핑", ("매크로 1차", ORANGE)])
    out.append(f'<rect x="334" y="{y0 + 274}" width="340" height="62" rx="10" fill="#EAF5EE" stroke="{GREEN}" stroke-width="2"/>')
    txt(352, y0 + 314, "chrony", 24, INK, True)
    txt(448, y0 + 313, "시계 오차 측정")

# NTP → chrony
arrow([(262, 924), (300, 924), (300, PY["pangyo"] + 305), (330, PY["pangyo"] + 305)], GREEN, "시간 동기화", 120, 966,
      dash=True, width=2.6)
arrow([(300, PY["daejeon"] + 305), (330, PY["daejeon"] + 305)], GREEN, None, dash=True, width=2.6)

# ---------- PoP ↔ 중앙 연결 (Kafka)
BX = 760
out.append(f'<line x1="{BX}" y1="150" x2="{BX}" y2="620" stroke="{INK}" stroke-width="8" stroke-linecap="round"/>')
for y in (300, 540):
    out.append(f'<line x1="{PX + PW}" y1="{y}" x2="{BX}" y2="{y}" stroke="{INK}" stroke-width="5"/>')

# ---------- 판교 Core
box(900, 26, 845, 628, INK, "판교 Core (Central)")
KX, KW = 920, 224                       # Kafka 열
out.append(f'<rect x="{KX}" y="86" width="{KW}" height="550" rx="12" fill="#F4F6F9" stroke="#CBD2DC" stroke-width="1.5"/>')
out.append(f'<image href="{icon("kafka")}" x="{KX + 14}" y="98" width="34" height="34"/>')
txt(KX + 56, 125, "Kafka", 24, INK, True)
PLX, PLW = KX + 12, 200                 # 토픽 알약
pill(PLX, 150, PLW, NAVY, "seat-requests")
pill(PLX, 250, PLW, NAVY, "seat-status")
pill(PLX, 470, PLW, ORANGE, "bot-rules")
pill(PLX, 560, PLW, ORANGE, "detection-log")

LX = 846                                # 버스 ↔ Kafka 사이 글자 가운데
arrow([(BX, 173), (PLX - 4, 173)], NAVY, ["② 도착 시각", "+ 오차"], LX, 134)
arrow([(PLX, 273), (BX + 5, 273)], NAVY, "④ 결과", LX, 261)
arrow([(PLX, 493), (BX + 5, 493)], ORANGE, "⑥ AI 규칙", LX, 481)
arrow([(BX, 583), (PLX - 4, 583)], ORANGE, "⑤ 탐지 로그", LX, 571)

# central: 2차 검사 → 판정 → 좌석 선점 방지, 로그인
CX, CY, CW, CH = 1176, 100, 274, 336
TX, TW = CX + 18, CW - 36
out.append(f'<rect x="{CX}" y="{CY}" width="{CW}" height="{CH}" rx="12" fill="white" stroke="{NAVY}" stroke-width="2.2"/>')
out.append(f'<image href="{icon("fastapi")}" x="{CX + 16}" y="{CY + 12}" width="34" height="34"/>')
txt(CX + 60, CY + 39, "central", F_COMP, INK, True)
for i, (label, color) in enumerate((("매크로 2차", ORANGE), ("판정 · 보정 시각순", NAVY), ("좌석 선점 방지", NAVY))):
    top = 152 + i * 76
    tag(TX, top, label, color, TW)
    if i < 2:
        arrow([(TX + TW / 2, top + 38), (TX + TW / 2, top + 72)], NAVY, None, width=2.4)
out.append(f'<line x1="{CX + 12}" y1="{CY + 280}" x2="{CX + CW - 12}" y2="{CY + 280}" stroke="#E2E8F0" stroke-width="1.5"/>')
tag(TX, CY + 290, "로그인", NAVY, TW)

arrow([(PLX + PLW + 4, 166), (CX - 4, 166)], NAVY, "③", 1154, 154, width=2.6)
# bot-rules → 2차 (④ 화살표와 겹치는 곳은 흰 틈으로 띄운다)
VX = 1156
arrow([(PLX + PLW, 485), (VX, 485), (VX, 186), (CX - 4, 186)], ORANGE, None, width=2.6)
out.append(f'<rect x="{VX - 7}" y="266" width="14" height="14" fill="white"/>')
arrow([(CX, 273), (PLX + PLW + 6, 273)], NAVY, None, width=2.8)

comp(1490, 112, ["redis"], "Redis", ["수집창 · 결제 대기"])
comp(1490, 232, ["postgresql"], "PostgreSQL", ["확정 좌석"])
for y in (136, 256):
    out.append(f'<line x1="{CX + CW}" y1="{y}" x2="1484" y2="{y}" stroke="{GREY}" stroke-width="2.2"/>')
comp(1360, 556, ["prometheus", "grafana"], "Prometheus · Grafana", ["모니터링"], size=40, fs=24)

# ---------- 서울 AI (매크로 3차)
box(900, 690, 845, 210, ORANGE, "KOREN AI Cloud (서울)", right=True)
comp(950, 770, ["python"], "collector", ["탐지 로그 수집"], size=52)
comp(1280, 770, ["python"], "AI 학습", [("매크로 3차", ORANGE), "봇 판별 → 규칙 배포"], size=52)
arrow([(1150, 796), (1272, 796)], ORANGE, None, width=2.6)
arrow([(976, 606), (976, 764)], ORANGE, "⑤", 960, 740, anchor="end")
arrow([(1306, 764), (1306, 501), (PLX + PLW + 6, 501)], ORANGE, "⑥", 1322, 640, anchor="start")

markers = "".join(
    f'<marker id="a{c.strip("#")}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto">'
    f'<path d="M0,0 L10,5 L0,10 z" fill="{c}"/></marker>'
    f'<marker id="s{c.strip("#")}" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="6" markerHeight="6" orient="auto">'
    f'<path d="M10,0 L0,5 L10,10 z" fill="{c}"/></marker>' for c in (NAVY, ORANGE, GREEN, GREY, INK))
svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">'
       f'<defs>{markers}</defs><rect width="{W}" height="{H}" fill="white"/>' + "\n".join(out) + "</svg>")
open(f"{HERE}/doon_network.svg", "w").write(svg)
open(f"{HERE}/doon_network.html", "w").write(f'<html><body style="margin:0">{svg}</body></html>')
print("ok")
