# ====================================================================================
#  엑셀 → MySQL 업로드(재생성) → PNU 조회 → DevExpress Diagram XML 생성(타임라인)
# ====================================================================================

# [목적]
# - 엑셀(이동정리현황_기간내.xlsx)을 로드해 지정 DB/테이블에 업로드(if_exists="replace")
# - 입력한 PNU(19자리)로 DB에서 이동전/이동후 필지코드 매칭 행을 정리일자 오름차순 조회
# - 조회 결과를 좌→우 타임라인으로 배치한 DevExpress Diagram XML(XtraSerializer) 생성
#   * 라벨 3행: [토지이동종목 / 현재_소유자명 / 정리일자(YYYYMMDD)] — 줄바꿈은 &#xD;&#xA;

# [입력]
# - 기본 엑셀 경로: ./out/이동정리현황_기간내.xlsx
#   * 없으면 대안 경로도 탐색: ./44250/out/이동정리현황_기간내.xlsx
# - DB/테이블(업로드 및 조회 대상): 인자 --db, --table 로 지정 (기본 testdb.land_move_tb)

# [출력]
# - XML 파일: ./out/xml/diagram_<PNU>_<YYYYMMDD_HHMMSS>.xml
# - 페이지/도형 배치 상수: PAGE_W/H, JIBUN_W/H, LABEL_W/H, ARROW_W, ROW_Y, START_X, LABEL_OFFSET_X

# [실행 방법]
# > python <이파일이름>.py --pnu 4425031524100010003 \
#     --excel ./out/이동정리현황_기간내.xlsx \
#     --host 127.0.0.1 --port 3307 --user root --password 1234 \
#     --db testdb --table land_move_tb
# 성공 시:
# - 업로드/조회 로그 출력 후 "[OK] XML 생성 완료 → ..." 메시지 표기

# [의존성]
# - pandas
# - SQLAlchemy (sqlalchemy)
# - PyMySQL (pymysql)
# - Python 표준 라이브러리: argparse, os, re, datetime, xml.etree.ElementTree

# [주의]
# - 업로드는 if_exists="replace"로 테이블을 재생성(기존 데이터 삭제)합니다.
# - 정리일자 포맷은 숫자만 추출하여 YYYYMMDD로 표기(불완전 값은 숫자열 그대로 유지).
# - 좌/우 지번 박스 텍스트는 행정구역명에서 '리' 단위만 추출하여 표시합니다.

import os
import re
import argparse
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
import xml.etree.ElementTree as ET

# -------- 화면/배치 상수 (대략치) --------
PAGE_W, PAGE_H = 800, 600
JIBUN_W, JIBUN_H = 130, 40
LABEL_W, LABEL_H = 100, 40
ARROW_W = 130
ROW_Y = 30
START_X = 0
LABEL_OFFSET_X = 10  # 연결선 시작점에서 라벨 박스까지 x 오프셋

# -------- 유틸 --------
def find_excel(path: str) -> str:
    """지정 경로가 없으면 44250/out 밑에서도 찾아봄."""
    abs1 = os.path.abspath(path)
    if os.path.exists(abs1):
        return abs1
    alt = os.path.abspath(os.path.join("./44250", "out", os.path.basename(path)))
    if os.path.exists(alt):
        return alt
    raise FileNotFoundError(f"엑셀 파일을 찾을 수 없습니다: {path} (대안: {alt})")

def extract_ri(adm_name: str) -> str:
    """행정구역명에서 '리' 단위만 추출 (없으면 원문)."""
    if not isinstance(adm_name, str):
        return ""
    m = re.search(r'([가-힣A-Za-z0-9]+리)', adm_name)
    return m.group(1) if m else adm_name

def yyyymmdd(d: str) -> str:
    """정리일자를 YYYYMMDD(숫자만)로 정규화."""
    if not isinstance(d, str):
        d = "" if d is None else str(d)
    return re.sub(r'\D', '', d)

def label_text(move_kind: str, owner: str, cre_ymd: str) -> str:
    """
    라벨 3줄: 토지이동종목, 현재_소유자명, 정리일자(YYYYMMDD)
    XML 줄바꿈은 &#xD;&#xA; 사용
    """
    return f"{(move_kind or '')}&#xD;&#xA;{(owner or '')}&#xD;&#xA;{yyyymmdd(cre_ymd)}"

def xml_new(tag, **attrs):
    el = ET.Element(tag)
    for k, v in attrs.items():
        el.set(k, str(v))
    return el

def prettify_and_write(root: ET.Element, out_path: str):
    """
    VSCode에서 보기 좋게 들여쓰기 저장.
    Python 3.9+ 은 ET.indent, 아니면 minidom fallback.
    """
    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ", level=0)  # type: ignore[attr-defined]
        tree.write(out_path, encoding="utf-8", xml_declaration=False)
    except Exception:
        import xml.dom.minidom as md
        xml_bytes = ET.tostring(root, encoding="utf-8")
        parsed = md.parseString(xml_bytes)
        pretty_xml = parsed.toprettyxml(indent="  ", encoding="utf-8")
        with open(out_path, "wb") as f:
            f.write(pretty_xml)

# -------- 1~2) 엑셀 로딩 → DB 업로드(테이블 재생성) --------
def upload_excel_to_db(excel_path: str, engine, db: str, table: str):
    print(f"[INFO] 엑셀 로딩: {excel_path}")
    df = pd.read_excel(excel_path, dtype=str)  # 선행 0 보존을 위해 전체 문자열
    print(f"[INFO] 로딩 완료: {df.shape}")

    print(f"[INFO] DB 업로드 → {db}.{table} (기존 테이블 있으면 삭제 후 재생성)")
    df.to_sql(table, con=engine, if_exists="replace", index=False)  # DROP/CREATE 효과
    print(f"[INFO] 업로드 완료")

# -------- 3~4) DB에서 PNU로 조회(이동전/이동후) --------
def fetch_rows_by_pnu(engine, db: str, table: str, pnu: str):
    sql = text(f"""
        SELECT
            `이동전_필지코드`   AS bf_pnu,
            `이동후_필지코드`   AS af_pnu,
            `토지이동종목`     AS land_move_kind,
            `정리일자`         AS cre_ymd,
            `현재_소유자명`     AS owner_name,
            `행정구역명`       AS adm_name
        FROM `{db}`.`{table}`
        WHERE `이동전_필지코드` = :p OR `이동후_필지코드` = :p
        ORDER BY `정리일자` ASC, `이동전_필지코드` ASC, `이동후_필지코드` ASC
    """)
    with engine.begin() as conn:
        return [dict(r) for r in conn.execute(sql, {"p": pnu}).mappings()]

# -------- 5~6) XML 빌드 --------
def build_diagram(rows):
    root = xml_new("XtraSerializer", version="23.2.3.0")
    items = xml_new("Items")
    root.append(items)

    root_item = xml_new(
        "Item1",
        ItemKind="DiagramRoot",
        PageSize=f"{PAGE_W},{PAGE_H}",
        SelectedStencils="BasicShapes, BasicFlowchartShapes",
    )
    items.append(root_item)

    children = xml_new("Children")
    root_item.append(children)

    if not rows:
        return root

    x = START_X
    y = ROW_Y
    item_id = 1

    for r in rows:
        jibun_left  = extract_ri(r.get("adm_name", ""))
        jibun_right = extract_ri(r.get("adm_name", ""))

        # 왼쪽 지번 박스
        item_id += 1
        children.append(xml_new(
            f"Item{item_id}",
            ItemKind="DiagramShape",
            Position=f"{x},{y}",
            Size=f"{JIBUN_W},{JIBUN_H}",
            Content=jibun_left,
        ))

        # 연결선
        begin_x = x + JIBUN_W
        begin_y = y + JIBUN_H // 2
        end_x   = begin_x + ARROW_W
        end_y   = begin_y
        item_id += 1
        children.append(xml_new(
            f"Item{item_id}",
            ItemKind="DiagramConnector",
            Points="(Empty)",
            BeginPoint=f"{begin_x},{begin_y}",
            EndPoint=f"{end_x},{end_y}",
        ))

        # 라벨 박스 (연결선 위쪽)
        label_x = begin_x + LABEL_OFFSET_X
        label_y = y - 30
        item_id += 1
        children.append(xml_new(
            f"Item{item_id}",
            ItemKind="DiagramShape",
            Position=f"{label_x},{label_y}",
            Size=f"{LABEL_W},{LABEL_H}",
            FontSize="8",
            ThemeStyleId="Variant2",
            Content=label_text(
                r.get("land_move_kind", ""),
                r.get("owner_name", ""),
                r.get("cre_ymd", ""),
            ),
        ))

        # 오른쪽 지번 박스
        item_id += 1
        children.append(xml_new(
            f"Item{item_id}",
            ItemKind="DiagramShape",
            Position=f"{x + JIBUN_W + ARROW_W},{y}",
            Size=f"{JIBUN_W},{JIBUN_H}",
            Content=jibun_right,
        ))

        x = x + (JIBUN_W + ARROW_W)

    return root

# -------- 메인 --------
def main():
    ap = argparse.ArgumentParser(description="엑셀→DB(testdb.land_move_tb) 업로드→PNU 조회→XML 생성")
    ap.add_argument("--excel", default="./out/이동정리현황_기간내.xlsx")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=3307)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="1234")
    ap.add_argument("--db", default="testdb")
    ap.add_argument("--table", default="land_move_tb")
    ap.add_argument("--pnu", required=True, help="검색할 19자리 필지코드")
    args = ap.parse_args()

    excel_path = find_excel(args.excel)

    # SQLAlchemy 엔진
    engine = create_engine(
        f"mysql+pymysql://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}?charset=utf8mb4",
        future=True,
    )

    # 1~2) 엑셀 로딩 → 업로드(재생성)
    upload_excel_to_db(excel_path, engine, args.db, args.table)

    # 3~4) 조회
    rows = fetch_rows_by_pnu(engine, args.db, args.table, args.pnu)
    if not rows:
        print(f"[INFO] 검색 결과 없음: PNU={args.pnu}")
        return

    print(f"[INFO] 검색 결과 {len(rows)}건 (정리일자 오름차순)")
    for i, r in enumerate(rows, 1):
        print(
            f"#{i} bf_pnu={r['bf_pnu']} af_pnu={r['af_pnu']} | "
            f"토지이동종목={r['land_move_kind']} | "
            f"현재_소유자명={r['owner_name']} | "
            f"정리일자={yyyymmdd(r['cre_ymd'])} | "
            f"행정구역명={r['adm_name']}"
        )

    # 5~6) XML 생성/저장
    root = build_diagram(rows)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = os.path.join(".", "out", "xml")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"diagram_{args.pnu}_{ts}.xml")
    prettify_and_write(root, out_path)
    print(f"[OK] XML 생성 완료 → {out_path}")

if __name__ == "__main__":
    main()
