# ====================================================
#  PNU 검색기: 기간내 결과 엑셀들에서 필지코드 매칭·저장
# ====================================================

# [목적]
# - 입력한 PNU(필지코드)를 숫자만 추출해 19자리로 정규화한 뒤
#   다음 엑셀 파일들에서 컬럼(이동전_필지코드/이동후_필지코드/필지코드/PNU)
#   중 존재하는 항목을 대상으로 행 단위 매칭 검색
# - 매칭 결과를 콘솔에 출력하고, 파일별로 결과 엑셀을 저장

# [입력 파일]  (BASE_DIR = ./1.data/out)
# - ./1.data/out/이동정리현황_기간내.xlsx
# - ./1.data/out/일반용조서(말소용)_기간내.xlsx
# - ./1.data/out/토지(임야)기본_기간내.xlsx

# [출력 파일]  (SAVE_DIR = ./1.data/out/find)
# - ./1.data/out/find/검색결과_이동정리현황_기간내.xlsx
# - ./1.data/out/find/검색결과_일반용조서(말소용)_기간내.xlsx
# - ./1.data/out/find/검색결과_토지(임야)기본_기간내.xlsx
#   * 대상 파일이 없으면 건너뜀
#   * 매칭 결과가 없으면 파일 저장 생략

# [실행 방법]
# > python 3-1데이터검수.py
# 입력 프롬프트에 검색할 PNU를 입력 (예: 4425031524100010003)

# [의존성]
# - pandas, openpyxl

import re
from pathlib import Path
import pandas as pd

# -------------------------------
# 경로 및 파일 지정
# -------------------------------
BASE_DIR = Path("./1.data/out")             # 원본 파일 폴더
TARGET_FILES = [
    "이동정리현황_기간내.xlsx",
    "일반용조서(말소용)_기간내.xlsx",
    "토지(임야)기본_기간내.xlsx",
]

TARGET_COLS = ["이동전_필지코드", "이동후_필지코드", "필지코드", "PNU"]

# 결과 저장 폴더
SAVE_DIR = BASE_DIR / "find"
SAVE_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------
# 유틸 함수
# -------------------------------
def normalize_pnu(x: str) -> str:
    """숫자만 남기고 19자리 zfill. None/NaN은 빈 문자열."""
    if x is None:
        return ""
    s = re.sub(r"\D", "", str(x))
    return s.zfill(19) if s else ""


def load_excel(path: Path) -> pd.DataFrame:
    """엑셀 첫 시트를 dtype=str로 로딩"""
    df = pd.read_excel(path, dtype=str)
    df.columns = [str(c).strip() for c in df.columns]
    for c in df.columns:
        df[c] = df[c].astype(str)
    return df


def find_matches(df: pd.DataFrame, pnu_norm: str) -> tuple[pd.DataFrame, list[str]]:
    """이동전_필지, 이동후_필지, 필지코드, pnu 중 존재하는 컬럼에서 검색"""
    cols = [c for c in TARGET_COLS if c in df.columns]
    if not cols:
        return df.iloc[0:0], []

    mask = None
    for c in cols:
        norm = df[c].map(normalize_pnu)
        m = (norm == pnu_norm)
        mask = m if mask is None else (mask | m)

    return df.loc[mask].copy(), cols


def print_and_save(title: str, df: pd.DataFrame, used_cols, save_path: Path):
    print("=" * 90)
    print(f"[{title}] 검색 사용 컬럼: {used_cols}")
    print(f"[{title}] 매칭 건수: {len(df)}")

    if len(df) == 0:
        print("(일치하는 레코드 없음)")
        return

    # 화면 출력
    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.width", 240):
        print(df.to_string(index=False))

    # 엑셀 저장
    try:
        df.to_excel(save_path, index=False)
        print(f"[{title}] 결과 저장 완료 → {save_path}")
    except Exception as e:
        print(f"[{title}] 저장 실패: {e}")


# -------------------------------
# 메인
# -------------------------------
if __name__ == "__main__":
    # 검색할 필지코드 입력
    target_pnu = input("검색할 필지코드를 입력하세요: ").strip()
    pnu_norm = normalize_pnu(target_pnu)

    print(f"[INFO] 검색 PNU(정규화): {pnu_norm}")
    print(f"[INFO] 원본 경로: {BASE_DIR}")
    print(f"[INFO] 결과 저장 경로: {SAVE_DIR}")

    for fname in TARGET_FILES:
        fpath = BASE_DIR / fname
        if not fpath.exists():
            print("=" * 90)
            print(f"[{fname}] 파일 없음: {fpath}")
            continue

        try:
            df = load_excel(fpath)
            matched, used_cols = find_matches(df, pnu_norm)

            save_name = f"검색결과_{Path(fname).stem}.xlsx"
            save_path = SAVE_DIR / save_name

            print_and_save(fname, matched, used_cols, save_path)

        except Exception as e:
            print("=" * 90)
            print(f"[{fname}] 처리 오류: {e}")
