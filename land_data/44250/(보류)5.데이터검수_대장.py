# ===========================================================
#  토지(임야)기본_기간내.xlsx → 이동사유 기준 검색 · 저장 스크립트
# ===========================================================

# [목적]
# - ./1.data/out/토지(임야)기본_기간내.xlsx를 로드하여
#   '이동사유/이동종목' 컬럼을 기준으로 사용자가 입력한 키워드로 필터링
# - PNU(필지코드)가 존재할 경우 19자리로 정규화한 보조 컬럼 추가
# - 결과를 콘솔에 출력하고 ./1.data/out/find/ 아래 엑셀로 저장

# [입력 파일]
# - ./1.data/out/토지(임야)기본_기간내.xlsx

# [출력 파일]  (SAVE_DIR = ./1.data/out/find)
# - ./1.data/out/find/검색결과_토지(임야)기본_기간내_이동사유=<키워드>_<모드>.xlsx
#   * 파일명에 사용 불가 문자는 자동 치환됨

# [실행 방법]
# > python 3-2.데이터검수_대장.py
# 프롬프트:
# - "이동사유(검색어)" 입력
# - 매칭 방식: exact(완전일치) / contains(부분일치) 중 선택 (기본 exact)

# [비고]
# - 이동사유/종목 컬럼 후보: ["이동사유","이동 사유","이동_사유","이동사유명","이동종목","이동 사유명"]
# - PNU 컬럼 후보: ["필지코드","pnu","PNU","필지 코드"]
# - PNU 정규화: 숫자만 추출 후 19자리 zfill

# [의존성]
# - pandas, openpyxl

import re
from pathlib import Path
import pandas as pd

# -------------------------------
# 경로/파일 지정
# -------------------------------
BASE_DIR  = Path("./1.data/out")
SRC_FILE  = BASE_DIR / "토지(임야)기본_기간내.xlsx"
SAVE_DIR  = BASE_DIR / "find"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# 컬럼명 후보
REASON_COL_CANDIDATES = [
    "이동사유", "이동 사유", "이동_사유", "이동사유명", "이동종목", "이동 사유명"
]
PNU_COL_CANDIDATES = ["필지코드", "pnu", "PNU", "필지 코드"]

def normalize_pnu(x: str) -> str:
    if x is None:
        return ""
    s = re.sub(r"\D", "", str(x))
    return s.zfill(19) if s else ""

def load_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, dtype=str)  # 첫 시트, 선행0 보존
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str)
    return df

def pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    # 느슨한 매칭(공백/대소문자 무시)
    lowered = {c.lower().replace(" ", ""): c for c in df.columns}
    for c in candidates:
        key = c.lower().replace(" ", "")
        if key in lowered:
            return lowered[key]
    return None

def filter_by_reason(df: pd.DataFrame, reason_col: str, keyword: str, mode: str) -> pd.DataFrame:
    s = df[reason_col].fillna("")
    if mode == "exact":
        mask = (s == keyword)
    else:  # contains
        mask = s.str.contains(re.escape(keyword), case=False, na=False)
    return df.loc[mask].copy()

def pretty_print(df: pd.DataFrame, title: str):
    print("=" * 100)
    print(f"[{title}] 매칭 건수: {len(df)}")
    if len(df) == 0:
        print("(일치하는 레코드 없음)")
        return
    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.width", 240):
        print(df.to_string(index=False))

if __name__ == "__main__":
    # 입력값 받기
    if not SRC_FILE.exists():
        raise FileNotFoundError(f"원본 파일을 찾을 수 없습니다: {SRC_FILE}")

    keyword = input("이동사유(검색어)를 입력하세요: ").strip()
    mode = input("매칭 방식 선택 (exact / contains) [기본 exact]: ").strip().lower() or "exact"
    if mode not in ("exact", "contains"):
        print("[INFO] 잘못된 모드 입력 → exact로 진행합니다.")
        mode = "exact"

    print(f"[INFO] 대상 파일   : {SRC_FILE}")
    print(f"[INFO] 저장 폴더   : {SAVE_DIR}")
    print(f"[INFO] 검색 키워드 : {keyword}")
    print(f"[INFO] 매칭 모드   : {mode}")

    # 로딩
    df = load_excel(SRC_FILE)

    # 컬럼 선택
    reason_col = pick_column(df, REASON_COL_CANDIDATES)
    if reason_col is None:
        raise RuntimeError(f"이동사유 컬럼을 찾지 못했습니다. 후보: {REASON_COL_CANDIDATES}")

    pnu_cols_found = [c for c in PNU_COL_CANDIDATES if c in df.columns]

    # 필터
    matched = filter_by_reason(df, reason_col, keyword, mode)

    # PNU 정규화 부가 컬럼(있을 때만)
    for pcol in pnu_cols_found:
        matched[f"{pcol}_정규화19"] = matched[pcol].map(normalize_pnu)

    # 출력
    title = f"토지(임야)기본_기간내.xlsx | 기준: {reason_col} | 모드: {mode}"
    pretty_print(matched, title)

    # 저장
    safe_kw = re.sub(r"[\\/:*?\"<>| ]+", "_", keyword)[:50] or "blank"
    save_name = f"검색결과_토지(임야)기본_기간내_이동사유={safe_kw}_{mode}.xlsx"
    save_path = SAVE_DIR / save_name
    try:
        matched.to_excel(save_path, index=False)
        print(f"[SAVE] 결과 저장 완료 → {save_path}")
    except Exception as e:
        print(f"[ERROR] 저장 실패: {e}")
