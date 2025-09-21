
# =====================================================================
#  PNU로 DB 조회 → DevExpress Diagram XML(XtraSerializer) 생성 스크립트
# =====================================================================

# [목적]
# - MySQL(lan dmove.land_move)에서 입력 PNU와 관련된 이동 이력
#   (이동전_필지코드=입력PNU OR 이동후_필지코드=입력PNU)을 조회
# - 정리일자 오름차순으로 타임라인 배치하여 DevExpress Diagram 형식의 XML 생성
# - 라벨(연결선 위)에는 [토지이동종목 / 정리일자(YYYYMMDD) / 현재_소유자명] 3행을
#   CRLF 엔티티(&#xD;&#xA;)로 줄바꿈하여 기록
# - 좌/우 박스 텍스트는 행정구역명에서 '리' 단위만 추출하여 표시

# [입력]
# - DB: landmove
# - Table: land_move
# - 조회 컬럼(에일리어스):
#   * bf_pnu(이동전_필지코드), af_pnu(이동후_필지코드),
#     land_move_kind(토지이동종목), cre_ymd(정리일자),
#     owner_name(현재_소유자명), adm_name(행정구역명)

# [출력]
# - XML 파일: ./1.data/out/xml/diagram_<PNU>_<YYYYMMDD_HHMMSS>.xml
# - 루트 태그: <XtraSerializer version="23.2.3.0"><Items>...</Items></XtraSerializer>
# - 페이지/도형 배치 상수: PAGE_W/H, JIBUN_W/H, LABEL_W/H, ARROW_W, ROW_Y, START_X, LABEL_OFFSET_X

# [실행 방법]
# > python <이파일이름>.py --pnu 4425031524100010003 \
#     --host 127.0.0.1 --port 3306 --user root --password 1234
# 성공 시: "[OK] XML 생성 완료 → ..." 출력, 결과 목록 콘솔 표시

# [의존성]
# - pymysql (DB 연결)
# - Python 표준 라이브러리: argparse, re, datetime, xml.etree.ElementTree

# [주의]
# - DB 접속 정보(--host/--port/--user/--password)와 스키마/테이블이 유효해야 함
# - 정리일자가 8자(YYYYMMDD)가 아니면 숫자만 정제하여 그대로 표기(불완전 값 보존)
# - 현재 좌/우 지번 박스 텍스트는 동일 adm_name에서 '리' 단위만 추출하여 사용


import os
import re
import argparse
import pymysql
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Dict, List, Any

# -------------------- 화면/배치 상수 --------------------
# Diagram 페이지 크기 및 각 요소의 기본 배치/크기 설정
PAGE_W, PAGE_H   = 800, 600     # 페이지 폭/높이
JIBUN_W, JIBUN_H = 130, 40      # 지번(좌/우) 박스 크기
LABEL_W, LABEL_H = 100, 40      # 라벨 박스 크기(연결선 위)
ARROW_W          = 130          # 좌/우 박스 사이(연결선) 가로 길이
ROW_Y            = 30           # 첫 행의 Y 좌표(상단 여백)
START_X          = 0            # 첫 박스의 X 좌표
LABEL_OFFSET_X   = 10           # 연결선 시작점 대비 라벨 박스의 X 오프셋

# -------------------- 유틸 함수 --------------------
def extract_ri(name: str) -> str:
    """행정구역명에서 '리' 단위만 추출
    - 입력이 문자열이 아니면 빈 문자열 반환
    - (한글/영문/숫자)+리 패턴을 우선 탐색, 없으면 원문 반환
    """
    if not isinstance(name, str):
        return ""
    m = re.search(r'([가-힣A-Za-z0-9]+리)', name)
    return m.group(1) if m else name

def fmt_date8(d: str) -> str:
    """정리일자 포맷 정규화
    - 숫자만 남기고 길이 8(YYYYMMDD)이면 그대로 반환
    - 그 외는 정제된 숫자열 그대로 반환(불완전 값 보존)
    """
    if not isinstance(d, str):
        return ""
    d = re.sub(r'\D', '', d)
    if len(d) == 8:
        return d
    return d

def label_content(mov_kind: str, cre_ymd: str, owner: str) -> str:
    """라벨 텍스트(3행)
    - 1행: 토지이동종목
    - 2행: 정리일자(YYYYMMDD)
    - 3행: 현재_소유자명
    - 줄바꿈은 DevExpress XML에서 사용하는 CRLF 엔티티로 표기: &#xD;&#xA;
    """
    line1 = mov_kind or ""
    line2 = fmt_date8(cre_ymd)
    line3 = owner or ""
    return f"{line1}&#xD;&#xA;{line2}&#xD;&#xA;{line3}"

def xml_new(tag: str, **attrs: Any) -> ET.Element:
    """ElementTree 엘리먼트 생성 도우미
    - tag와 속성(dict)을 받아서 XML 요소를 생성한다.
    """
    el = ET.Element(tag)
    for k, v in attrs.items():
        el.set(k, str(v))
    return el

# -------------------- DB 조회 --------------------
def fetch_rows(conn: pymysql.connections.Connection, pnu: str) -> List[Dict[str, Any]]:
    """입력 PNU와 관련된 이동 이력 레코드 조회
    - 조건: 이동전_필지코드 = PNU OR 이동후_필지코드 = PNU
    - 정렬: 정리일자 ASC, 이동전/이동후 필지코드 보조 ASC
    - 반환: Dict 목록 (컬럼 에일리어싱으로 통일)
    """
    sql = """
    SELECT
        `이동전_필지코드`   AS bf_pnu,
        `이동후_필지코드`   AS af_pnu,
        `토지이동종목`     AS land_move_kind,
        `정리일자`         AS cre_ymd,
        `현재_소유자명`     AS owner_name,
        `행정구역명`       AS adm_name
    FROM `landmove`.`land_move`
    WHERE `이동전_필지코드` = %s OR `이동후_필지코드` = %s
    ORDER BY `정리일자` ASC, `이동전_필지코드` ASC, `이동후_필지코드` ASC
    """
    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute(sql, (pnu, pnu))
        return cur.fetchall()

# -------------------- XML 빌더 --------------------
def build_diagram(rows: List[Dict[str, Any]]) -> ET.Element:
    """조회 레코드를 기반으로 DevExpress Diagram XML 트리를 생성"""
    # 최상위 루트와 컨테이너 초기화
    root   = xml_new("XtraSerializer", version="23.2.3.0")
    items  = xml_new("Items")
    root.append(items)

    # 다이어그램 루트(페이지 설정 포함)
    root_item = xml_new(
        "Item1",
        ItemKind="DiagramRoot",
        PageSize=f"{PAGE_W},{PAGE_H}",
        SelectedStencils="BasicShapes, BasicFlowchartShapes",
    )
    items.append(root_item)

    # 실제 도형/커넥터들이 들어갈 컨테이너
    children = xml_new("Children")
    root_item.append(children)

    # 결과가 없으면 빈 템플릿 반환
    if not rows:
        return root

    # 좌표계/아이템ID 초기화
    x       = START_X
    y       = ROW_Y
    item_id = 1  # 이미 Item1를 사용했으므로 1부터 시작, 이후 ++

    # 각 레코드를 순차(시간 오름차순) 배치
    for r in rows:
        # 좌/우 박스에 동일 adm_name에서 '리' 단위만 추출
        bf_name = extract_ri(r.get("adm_name", ""))
        af_name = extract_ri(r.get("adm_name", ""))

        # (1) 왼쪽 지번 박스
        item_id += 1
        left_box = xml_new(
            f"Item{item_id}",
            ItemKind="DiagramShape",
            Position=f"{x},{y}",
            Size=f"{JIBUN_W},{JIBUN_H}",
            Content=bf_name,
        )
        children.append(left_box)

        # (2) 왼쪽→오른쪽 연결선
        begin_x = x + JIBUN_W
        begin_y = y + JIBUN_H // 2
        end_x   = begin_x + ARROW_W
        end_y   = begin_y
        item_id += 1
        connector = xml_new(
            f"Item{item_id}",
            ItemKind="DiagramConnector",
            Points="(Empty)",
            BeginPoint=f"{begin_x},{begin_y}",
            EndPoint=f"{end_x},{end_y}",
        )
        children.append(connector)

        # (3) 연결선 위 라벨 박스(이동종목/정리일자/소유자명)
        label_x = begin_x + LABEL_OFFSET_X
        label_y = y - 30  # 연결선 위에 보이도록 Y를 살짝 올림
        item_id += 1
        label = xml_new(
            f"Item{item_id}",
            ItemKind="DiagramShape",
            Position=f"{label_x},{label_y}",
            Size=f"{LABEL_W},{LABEL_H}",
            FontSize="8",
            ThemeStyleId="Variant2",
            Content=label_content(
                r.get("land_move_kind", ""),
                r.get("cre_ymd", ""),
                r.get("owner_name", ""),
            ),
        )
        children.append(label)

        # (4) 오른쪽 지번 박스
        item_id += 1
        right_box = xml_new(
            f"Item{item_id}",
            ItemKind="DiagramShape",
            Position=f"{x + JIBUN_W + ARROW_W},{y}",
            Size=f"{JIBUN_W},{JIBUN_H}",
            Content=af_name,
        )
        children.append(right_box)

        # 다음 이벤트를 오른쪽으로 이어 붙이기
        x = x + (JIBUN_W + ARROW_W)

    return root

# -------------------- 메인 엔트리포인트 --------------------
def main() -> None:
    """CLI 진입점: DB 접속 → 조회 → 출력 → XML 생성/저장"""
    # CLI 인자 정의/파싱
    ap = argparse.ArgumentParser(description="PNU로 이력 조회 → Diagram XML 생성")
    ap.add_argument("--host", default="127.0.0.1", help="DB 호스트")
    ap.add_argument("--port", type=int, default=3306, help="DB 포트")
    ap.add_argument("--user", default="root", help="DB 사용자")
    ap.add_argument("--password", default="1234", help="DB 비밀번호")
    ap.add_argument("--pnu", required=True, help="검색할 19자리 필지코드")
    args = ap.parse_args()

    # DB 연결 (예외는 상위로 전파하지 않고 finally에서 정리)
    conn = pymysql.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database="landmove",  # 스키마명
        charset="utf8mb4"
    )
    try:
        # 1) 데이터 조회
        rows = fetch_rows(conn, args.pnu)

        # 2) 결과 출력(요약)
        if not rows:
            print(f"[INFO] 검색 결과 없음: PNU={args.pnu}")
            return

        print(f"[INFO] 검색 결과 {len(rows)}건")
        for i, r in enumerate(rows, 1):
            print(
                f"#{i} "
                f"bf_pnu={r['bf_pnu']} "
                f"af_pnu={r['af_pnu']} "
                f"정리일자={fmt_date8(str(r['cre_ymd']))} "
                f"토지이동종목={r['land_move_kind']} "
                f"현재_소유자명={r['owner_name']} "
                f"행정구역명={r['adm_name']}"
            )

        # 3) XML 빌드
        root = build_diagram(rows)
        tree = ET.ElementTree(root)

        # 4) 파일 저장 (타임스탬프 포함)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = os.path.join(".", "1.data", "out", "xml")
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"diagram_{args.pnu}_{ts}.xml")

        # 들여쓰기(가독성) 적용 후 저장
        ET.indent(tree, space="  ", level=0)
        tree.write(out_path, encoding="utf-8", xml_declaration=False)
        print(f"[OK] XML 생성 완료 → {out_path}")

    finally:
        # 5) DB 연결 정리
        conn.close()

if __name__ == "__main__":
    main()
