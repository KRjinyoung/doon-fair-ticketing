"""DOON 네트워크 구성도 — 좌표를 직접 잡은 SVG (doon_network.svg / .html)"""
import base64
import os

HERE = os.path.dirname(os.path.abspath(__file__))
W, H = 1760, 900
NAVY, ORANGE, GREEN, GREY, INK, SUB = "#1F3A93", "#D35400", "#1E8449", "#8A94A6", "#1B2230", "#4A5568"
FONT = "'Noto Sans CJK KR', sans-serif"


def icon(name):
    return "data:image/png;base64," + base64.b64encode(open(f"{HERE}/icons/{name}.png", "rb").read()).decode()


out = []
DA75, DA86, DA64 = 'stroke-dasharray="7 5"', 'stroke-dasharray="8 6"', 'stroke-dasharray="6 4"'
MS = 'marker-start="url(#s{})"'


def text_w(s, size):
    """글자 폭 어림값 (한글은 글자 크기만큼, 나머지는 절반 남짓)"""
    return sum(size if ord(c) > 0x1100 else size * 0.55 for c in s)


def box(x, y, w, h, color, title, dash=True, fill="none", tcolor=None):
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{color}" '
               f'stroke-width="1.8" {DA75 if dash else ""}/>')
    out.append(f'<text x="{x + 16}" y="{y + 26}" font-size="16" font-weight="700" fill="{tcolor or color}">{title}</text>')


def tag(x, y, label, color=ORANGE, fill="#FDEDE3"):
    """작은 꼬리표 (매크로 1·2·3차 등). 반환: 꼬리표 폭"""
    w = text_w(label, 11.5) + 14
    out.append(f'<rect x="{x}" y="{y - 13}" width="{w}" height="18" rx="5" fill="{fill}" stroke="{color}" stroke-width="1"/>')
    out.append(f'<text x="{x + w / 2}" y="{y + 1}" font-size="11.5" font-weight="700" fill="{color}" '
               f'text-anchor="middle">{label}</text>')
    return w


def line(x, y, item):
    """설명 한 줄. 문자열이거나 (꼬리표, 색, 나머지 글) 묶음"""
    if isinstance(item, tuple):
        label, color, rest = item
        fill = "#FDEDE3" if color == ORANGE else "#E8EDF8"
        w = tag(x, y, label, color, fill)
        out.append(f'<text x="{x + w + 6}" y="{y}" font-size="12.5" fill="{SUB}">{rest}</text>')
    else:
        out.append(f'<text x="{x}" y="{y}" font-size="12.5" fill="{SUB}">{item}</text>')


def comp(x, y, icons, title, lines=(), size=38, fs=15):
    for i, n in enumerate(icons):
        out.append(f'<image href="{icon(n)}" x="{x + i * (size + 4)}" y="{y}" width="{size}" height="{size}"/>')
    tx = x + len(icons) * (size + 4) + 8
    out.append(f'<text x="{tx}" y="{y + 15}" font-size="{fs}" font-weight="700" fill="{INK}">{title}</text>')
    for j, l in enumerate(lines):
        line(tx, y + 34 + j * 18, l)


def pill(x, y, w, color, text, fill="white"):
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="34" rx="9" fill="{fill}" stroke="{color}" stroke-width="1.6"/>')
    out.append(f'<text x="{x + w / 2}" y="{y + 22}" font-size="14" text-anchor="middle" fill="{INK}" '
               f'font-family="monospace" font-weight="600">{text}</text>')


def arrow(pts, color, label=None, lx=None, ly=None, dash=False, width=2.2, both=False, anchor="middle"):
    d = "M " + " L ".join(f"{a},{b}" for a, b in pts)
    m = color.strip("#")
    out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" '
               f'{DA86 if dash else ""} marker-end="url(#a{m})" '
               f'{MS.format(m) if both else ""}/>')
    if label:
        for k, s in enumerate([label] if isinstance(label, str) else label):
            out.append(f'<text x="{lx}" y="{ly + k * 16}" font-size="13.5" font-weight="600" fill="{color}" '
                       f'text-anchor="{anchor}" paint-order="stroke" stroke="white" stroke-width="5">{s}</text>')


# ---------- 사용자 · PoP 배정 · 시계
comp(30, 150, ["users"], "수도권 사용자", size=44, fs=16)
comp(30, 470, ["users"], "지방 사용자 (대전)", size=44, fs=16)

out.append(f'<rect x="24" y="262" width="214" height="92" rx="12" fill="#F7F9FC" stroke="{GREY}" stroke-width="1.6"/>')
out.append(f'<path d="M48,276 a11,11 0 0 1 11,11 c0,8 -11,19 -11,19 c0,0 -11,-11 -11,-19 a11,11 0 0 1 11,-11 z" '
           f'fill="{NAVY}"/><circle cx="48" cy="287" r="4" fill="white"/>')
out.append(f'<text x="70" y="292" font-size="15" font-weight="700" fill="{INK}">doon.land</text>')
out.append(f'<text x="70" y="314" font-size="12.5" fill="{SUB}">가까운 PoP로 자동 연결</text>')
out.append(f'<text x="70" y="332" font-size="12.5" fill="{SUB}">(브라우저 위치 기준)</text>')
arrow([(52, 198), (52, 258)], GREY, "⓪ PoP 배정", 64, 233, dash=True, width=1.8, both=True, anchor="start")
arrow([(52, 358), (52, 466)], GREY, None, dash=True, width=1.8, both=True)

comp(30, 770, ["internet"], "외부 NTP 기준 시계", size=36, fs=14)
out.append(f'<text x="76" y="812" font-size="13" font-weight="600" fill="{GREEN}">시간 동기화</text>')

# ---------- PoP 두 곳 (매크로 1차)
for key, city, y0 in (("pangyo", "판교", 110), ("daejeon", "대전", 430)):
    box(300, y0, 400, 280, NAVY, f"KOREN {city} PoP (Edge)")
    comp(326, y0 + 46, ["nginx"], "nginx", ["웹 · 요청 수신"], size=34, fs=14)
    comp(326, y0 + 104, ["fastapi"], f"pop-{key}  (FastAPI)",
         ["도착 시각을 µs 단위로 찍음 (엣지 스탬핑)", "로그인 토큰 검증 (공개키)",
          ("매크로 1차", ORANGE, "위조 토큰 · 요청 수 · AI 명단·규칙")], size=40)
    out.append(f'<rect x="326" y="{y0 + 212}" width="272" height="50" rx="9" fill="#EAF5EE" stroke="{GREEN}" stroke-width="1.5"/>')
    out.append(f'<text x="340" y="{y0 + 233}" font-size="14" font-weight="700" fill="{INK}">chrony</text>')
    out.append(f'<text x="340" y="{y0 + 252}" font-size="12.5" fill="{SUB}">시계 오차 · 보장 오차 측정 → 요청에 첨부</text>')

arrow([(210, 172), (318, 172)], GREY, "① 좌석 요청", 262, 161)
arrow([(284, 492), (318, 492)], GREY, "①", 300, 481)
arrow([(226, 788), (274, 788), (274, 347), (318, 347)], GREEN, None, dash=True, width=1.8)
arrow([(274, 667), (318, 667)], GREEN, None, dash=True, width=1.8)

# ---------- PoP ↔ 중앙 연결 (버스)
BX = 760
out.append(f'<line x1="{BX}" y1="150" x2="{BX}" y2="740" stroke="{INK}" stroke-width="6" stroke-linecap="round"/>')
out.append(f'<text x="{BX - 14}" y="445" font-size="13" font-weight="600" fill="{INK}" '
           f'transform="rotate(-90 {BX - 14} 445)" text-anchor="middle">PoP ↔ 중앙 메시지 (Kafka)</text>')
for y in (250, 570):
    out.append(f'<line x1="700" y1="{y}" x2="{BX}" y2="{y}" stroke="{INK}" stroke-width="4"/>')

# ---------- 판교 Core
box(880, 40, 520, 830, INK, "판교 Core · 중앙 판정 (Central)")
out.append(f'<rect x="900" y="90" width="190" height="740" rx="12" fill="#F4F6F9" stroke="#CBD2DC"/>')
out.append(f'<image href="{icon("kafka")}" x="912" y="102" width="26" height="26"/>')
out.append(f'<text x="944" y="121" font-size="15" font-weight="700" fill="{INK}">Apache Kafka</text>')
pill(915, 170, 160, NAVY, "seat-requests")
pill(915, 250, 160, NAVY, "seat-status")
pill(915, 600, 160, ORANGE, "bot-rules")
pill(915, 700, 160, ORANGE, "detection-log")

arrow([(BX, 187), (910, 187)], NAVY, ["② 도착 시각", "+ 보정값 · 오차"], 836, 159)
arrow([(915, 267), (BX + 4, 267)], NAVY, "④ 결과 · 결제 대기", 836, 258)
arrow([(915, 617), (BX + 4, 617)], ORANGE, "⑥ AI 명단·규칙", 836, 608)
arrow([(BX, 717), (910, 717)], ORANGE, "⑤ 탐지 로그", 836, 708)

# central 서버: 2차 검사 → 판정 → 좌석 선점 방지, 로그인
CX, CY, CW, CH = 1120, 110, 262, 312
out.append(f'<rect x="{CX}" y="{CY}" width="{CW}" height="{CH}" rx="11" fill="white" stroke="{NAVY}" stroke-width="1.6"/>')
out.append(f'<image href="{icon("fastapi")}" x="{CX + 12}" y="{CY + 9}" width="28" height="28"/>')
out.append(f'<text x="{CX + 48}" y="{CY + 29}" font-size="15" font-weight="700" fill="{INK}">central (FastAPI)</text>')
rows = [
    ("매크로 2차", ORANGE, ["신원 대조 · 기기당 계정 수", "미결제 선점 · 전국 요청 수"]),
    ("판정", NAVY, ["보정 시각이 빠른 순", "오차가 겹치면 sha1 추첨 (공개)"]),
    ("좌석 선점 방지", NAVY, ["승자 결제 대기 → 결제 시 확정", "기한 초과 시 좌석 반환"]),
    ("로그인", NAVY, ["토큰 발급 (RS256)"]),
]
ry = CY + 46
for i, (label, color, ls) in enumerate(rows):
    if i:
        out.append(f'<line x1="{CX + 10}" y1="{ry - 8}" x2="{CX + CW - 10}" y2="{ry - 8}" stroke="#E2E8F0" stroke-width="1.2"/>')
    tag(CX + 14, ry + 10, label, color, "#FDEDE3" if color == ORANGE else "#E8EDF8")
    for j, s in enumerate(ls):
        out.append(f'<text x="{CX + 14}" y="{ry + 30 + j * 17}" font-size="12.5" fill="{SUB}">{s}</text>')
    ry += 72 if len(ls) > 1 else 50
for y1 in (CY + 104, CY + 176):   # 2차 → 판정 → 결제 대기
    arrow([(CX + CW - 18, y1), (CX + CW - 18, y1 + 22)], NAVY, None, width=1.6)

arrow([(1075, 187), (CX - 4, 187)], NAVY, "③", 1097, 178, width=2)
# bot-rules → 2차 (④ 화살표와 겹치는 곳은 흰 틈으로 띄운다)
arrow([(1075, 608), (1102, 608), (1102, 212), (CX - 4, 212)], ORANGE, None, width=1.8)
out.append(f'<rect x="1096" y="261" width="12" height="12" fill="white"/>')
arrow([(CX, 267), (1080, 267)], NAVY, None, width=2)
out.append(f'<text x="1109" y="525" font-size="13.5" font-weight="600" fill="{ORANGE}" paint-order="stroke" '
           f'stroke="white" stroke-width="5">⑥</text>')

comp(1130, 448, ["redis"], "Redis", ["수집창 · 2차 전국 집계 · 결제 대기", "DB 배치 기록 대기열"], size=34)
comp(1130, 522, ["postgresql"], "PostgreSQL", ["확정 좌석 · 계정 · 세션"], size=34)
out.append(f'<line x1="1147" y1="{CY + CH}" x2="1147" y2="446" stroke="{GREY}" stroke-width="1.6"/>')
out.append(f'<line x1="1147" y1="484" x2="1147" y2="520" stroke="{GREY}" stroke-width="1.6"/>')
comp(1130, 790, ["prometheus", "grafana"], "Prometheus · Grafana", ["실시간 모니터링"], size=30, fs=14)

# ---------- 서울 AI (매크로 3차)
box(1420, 520, 325, 280, ORANGE, "KOREN AI Cloud (서울)")
comp(1436, 566, ["python"], "AI 학습 (scikit-learn)",
     [("매크로 3차", ORANGE, "계정별 행동 5가지 학습"), "→ 봇 확률 · 근거", "→ 명단 · 한도 · 규칙 배포",
      "(규칙은 그림자 검증 후 적용)"], size=34)
comp(1436, 700, ["python"], "collector", ["탐지 로그 수집 · 보관", "1·2차 차단 기록 → 학습 라벨"], size=34)
arrow([(1453, 696), (1453, 676)], ORANGE, None, width=1.8)
arrow([(1430, 617), (1080, 617)], ORANGE, "⑥", 1300, 608)
arrow([(1075, 717), (1428, 717)], ORANGE, "⑤", 1300, 708)

markers = "".join(
    f'<marker id="a{c.strip("#")}" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
    f'<path d="M0,0 L10,5 L0,10 z" fill="{c}"/></marker>'
    f'<marker id="s{c.strip("#")}" viewBox="0 0 10 10" refX="1" refY="5" markerWidth="7" markerHeight="7" orient="auto">'
    f'<path d="M10,0 L0,5 L10,10 z" fill="{c}"/></marker>' for c in (NAVY, ORANGE, GREEN, GREY, INK))
svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" font-family="{FONT}">'
       f'<defs>{markers}</defs><rect width="{W}" height="{H}" fill="white"/>' + "\n".join(out) + "</svg>")
open(f"{HERE}/doon_network.svg", "w").write(svg)
open(f"{HERE}/doon_network.html", "w").write(f'<html><body style="margin:0">{svg}</body></html>')
print("ok")
