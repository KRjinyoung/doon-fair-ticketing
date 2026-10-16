"""DOON 네트워크 구성도 — 좌표를 직접 잡은 SVG (doon_network.svg / .html)"""
import base64
import os

HERE = os.path.dirname(os.path.abspath(__file__))
W, H = 1720, 870
NAVY, ORANGE, GREEN, GREY, INK, SUB = "#1F3A93", "#D35400", "#1E8449", "#8A94A6", "#1B2230", "#4A5568"
FONT = "'Noto Sans CJK KR', sans-serif"


def icon(name):
    return "data:image/png;base64," + base64.b64encode(open(f"{HERE}/icons/{name}.png", "rb").read()).decode()


out = []
DA75, DA86, DA64 = 'stroke-dasharray="7 5"', 'stroke-dasharray="8 6"', 'stroke-dasharray="6 4"'
MS = 'marker-start="url(#s{})"'


def box(x, y, w, h, color, title, dash=True, fill="none", tcolor=None):
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{color}" '
               f'stroke-width="1.8" {DA75 if dash else ""}/>')
    out.append(f'<text x="{x + 16}" y="{y + 26}" font-size="16" font-weight="700" fill="{tcolor or color}">{title}</text>')


def comp(x, y, icons, title, lines=(), size=38, fs=15):
    for i, n in enumerate(icons):
        out.append(f'<image href="{icon(n)}" x="{x + i * (size + 4)}" y="{y}" width="{size}" height="{size}"/>')
    tx = x + len(icons) * (size + 4) + 8
    out.append(f'<text x="{tx}" y="{y + 15}" font-size="{fs}" font-weight="700" fill="{INK}">{title}</text>')
    for j, l in enumerate(lines):
        out.append(f'<text x="{tx}" y="{y + 34 + j * 17}" font-size="12.5" fill="{SUB}">{l}</text>')


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
        out.append(f'<text x="{lx}" y="{ly}" font-size="13.5" font-weight="600" fill="{color}" '
                   f'text-anchor="{anchor}" paint-order="stroke" stroke="white" stroke-width="5">{label}</text>')


# ---------- 사용자 · 시계
comp(30, 162, ["users"], "수도권 사용자", size=44, fs=16)
comp(30, 462, ["users"], "지방 사용자 (대전)", size=44, fs=16)
comp(30, 330, ["internet"], "외부 NTP 기준 시계", size=36, fs=14)

# ---------- PoP 두 곳
for key, city, y0 in (("pangyo", "판교", 120), ("daejeon", "대전", 420)):
    box(300, y0, 400, 270, NAVY, f"KOREN {city} PoP (Edge)")
    comp(326, y0 + 46, ["nginx"], "nginx", ["웹 · 요청 수신"], size=34, fs=14)
    comp(326, y0 + 104, ["fastapi"], f"pop-{key}  (FastAPI)",
         ["도착 시각을 µs 단위로 찍음 (엣지 스탬핑)", "1차 매크로 필터 (v0 + AI 규칙)", "로그인 토큰 검증 (공개키)"], size=40)
    out.append(f'<rect x="326" y="{y0 + 200}" width="210" height="50" rx="9" fill="#EAF5EE" stroke="{GREEN}" stroke-width="1.5"/>')
    out.append(f'<text x="340" y="{y0 + 221}" font-size="14" font-weight="700" fill="{INK}">chrony</text>')
    out.append(f'<text x="340" y="{y0 + 240}" font-size="12.5" fill="{SUB}">시계 오차 측정 · 보정값 첨부</text>')

arrow([(232, 183), (318, 183)], GREY, "① 좌석 요청", 275, 172)
arrow([(274, 483), (318, 483)], GREY, "①", 296, 472)
arrow([(222, 352), (318, 345)], GREEN, None, dash=True, width=1.8)
arrow([(222, 352), (262, 352), (262, 645), (318, 645)], GREEN, None, dash=True, width=1.8)
out.append(f'<text x="76" y="392" font-size="13" font-weight="600" fill="{GREEN}">시간 동기화</text>')

# ---------- PoP ↔ 중앙 연결 (버스)
BX = 760
out.append(f'<line x1="{BX}" y1="170" x2="{BX}" y2="690" stroke="{INK}" stroke-width="6" stroke-linecap="round"/>')
out.append(f'<text x="{BX - 14}" y="430" font-size="13" font-weight="600" fill="{INK}" '
           f'transform="rotate(-90 {BX - 14} 430)" text-anchor="middle">PoP ↔ 중앙 메시지 (Kafka)</text>')
for y in (255, 555):
    out.append(f'<line x1="700" y1="{y}" x2="{BX}" y2="{y}" stroke="{INK}" stroke-width="4"/>')

# ---------- 판교 Core
box(890, 60, 460, 790, INK, "판교 Core · 중앙 판정 (Central)")
out.append(f'<rect x="910" y="110" width="190" height="660" rx="12" fill="#F4F6F9" stroke="#CBD2DC"/>')
out.append(f'<image href="{icon("kafka")}" x="922" y="122" width="26" height="26"/>')
out.append(f'<text x="954" y="141" font-size="15" font-weight="700" fill="{INK}">Apache Kafka</text>')
pill(925, 190, 160, NAVY, "seat-requests")
pill(925, 270, 160, NAVY, "seat-status")
pill(925, 540, 160, ORANGE, "detection-log")
pill(925, 620, 160, ORANGE, "bot-rules")

arrow([(BX, 207), (920, 207)], NAVY, "② 시각 + 보정값", 826, 198)
arrow([(925, 287), (BX + 4, 287)], NAVY, "④ 당첨 / 낙첨", 826, 278)
arrow([(BX, 557), (920, 557)], ORANGE, "⑤ 탐지 로그", 826, 548)
arrow([(925, 637), (BX + 4, 637)], ORANGE, "⑥ AI 규칙", 826, 628)

comp(1125, 190, ["fastapi"], "central (FastAPI)",
     ["보정 시각으로 승자 판정", "시계 오차 범위 안 동점 →", "검증 가능한 추첨 (sha1)"], size=40)
arrow([(1085, 207), (1118, 207)], NAVY, "③", 1101, 196, width=2)
arrow([(1150, 262), (1150, 280), (1090, 287)], NAVY, None, width=2)
comp(1125, 340, ["redis"], "Redis", ["좌석별 수집창 상태"], size=36)
comp(1125, 420, ["postgresql"], "PostgreSQL", ["확정 좌석 · 계정 · 세션"], size=36)
out.append(f'<line x1="1200" y1="276" x2="1200" y2="336" stroke="{GREY}" stroke-width="1.6"/>')
comp(1118, 730, ["prometheus", "grafana"], "Prometheus", ["Grafana", "실시간 모니터링"], size=34, fs=14)

# ---------- 서울 AI
box(1390, 500, 310, 260, ORANGE, "KOREN AI Cloud (서울)")
comp(1410, 545, ["python"], "collector", ["탐지 로그 수집 · 보관"], size=36)
comp(1410, 640, ["python"], "AI 규칙 학습", ["회차별 도착 시각 분포 학습", "→ 짧은 규칙으로 압축", "→ 자동 검사 후 배포"], size=36)
arrow([(1440, 590), (1440, 632)], ORANGE, None, width=1.8)
arrow([(1085, 557), (1402, 557)], ORANGE, "⑤", 1245, 548)
arrow([(1402, 660), (1250, 660), (1250, 637), (1090, 637)], ORANGE, "⑥", 1320, 652)

# ---------- 하단

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
