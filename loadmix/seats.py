"""3000석 좌석 ID 생성 — pop/frontend/seat.html 의 buildVenue() ID 규칙과 1:1.
ID = "구역-열-번". node 없이 좌석 집합만 그대로 재현한다(화면 좌표는 ID와 무관).
  FLOOR  : A1,A2,A3,B1,B2,B3  (각 8열×13번 = 104)         → 624
  1층 BOWL: 101~110 (좌3·곡4·우3, 8열, 옆8/곡16번)        → 896
  2층 BOWL: 201~211 (좌3·곡5·우3, 10열, 옆8/곡20번)       → 1480
"""

def all_seats():
    seats = []
    # FLOOR
    for sec in ("A1", "A2", "A3", "B1", "B2", "B3"):
        for r in range(1, 9):
            for c in range(1, 14):
                seats.append(f"{sec}-{r}-{c}")
    # BOWL: (first, rows, side_n, side_cols, curve_n, curve_cols)
    for first, rows, sn, sc, cn, cc in ((101, 8, 3, 8, 4, 16), (201, 10, 3, 8, 5, 20)):
        no = first
        plan = [(sc, sn), (cc, cn), (sc, sn)]  # (번수, 구역수) 순: 좌·곡·우
        for cols, nsec in plan:
            for _ in range(nsec):
                sec = str(no); no += 1
                for r in range(1, rows + 1):
                    for c in range(1, cols + 1):
                        seats.append(f"{sec}-{r}-{c}")
    return seats


if __name__ == "__main__":
    s = all_seats()
    print("총", len(s), "유니크", len(set(s)))
    print("샘플", s[:3], s[620:627], s[-3:])
