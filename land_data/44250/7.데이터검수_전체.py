# =====================================================================
#  BFS 연계 탐색: 기간내 엑셀 3종 가로질러 PNU 연결·확장 · 결과 저장
# =====================================================================

# [목적]
# - 시작 PNU를 숫자만 추출해 19자리로 정규화한 뒤,
#   아래 3개 파일의 PNU 후보 컬럼(이동전_필지코드/이동후_필지코드/필지코드/PNU)
#   전체에서 OR 매칭하여 연결된 레코드를 너비우선탐색(BFS)으로 확장
# - 매칭된 각 행에 최소 hop(연결 깊이)을 기록해 콘솔 출력 및 파일별 결과 저장
# - MAX_DEPTH로 탐색 깊이 제한 가능(기본: 무제한, 신규 PNU가 없을 때 종료)

# [입력 파일]  (BASE_DIR = ./1.data/out)
# - ./1.data/out/이동정리현황_기간내.xlsx
# - ./1.data/out/일반용조서(말소용)_기간내.xlsx
# - ./1.data/out/토지(임야)기본_기간내.xlsx

# [출력 파일]  (SAVE_DIR = BASE_DIR/"out"/"find")
# - ./1.data/out/out/find/검색결과_이동정리현황_기간내_연계.xlsx
# - ./1.data/out/out/find/검색결과_일반용조서(말소용)_기간내_연계.xlsx
# - ./1.data/out/out/find/검색결과_토지(임야)기본_기간내_연계.xlsx
#   * 파일이 없거나 PNU 후보 컬럼이 없으면 해당 파일은 건너뜀

# [실행 방법]
# > python <이파일이름>.py
# 프롬프트:
# - 시작 PNU 입력 (예: 4425031524100010003)
# 출력:
# - 파일별 매칭 행과 __hop__ 컬럼(연결 깊이) 표시
# - SAVE_DIR에 결과 엑셀 저장

# [옵션]
# - MAX_DEPTH = None  → 무제한(신규 PNU 없을 때까지)
# - MAX_DEPTH = 5     → 깊이 5까지 탐색

# [정규화 규칙]
# - PNU: 숫자만 남긴 뒤 19자리로 zfill

# [의존성]
# - pandas, openpyxl(엑셀 저장 시)

# [주의]
# - 현재 SAVE_DIR이 BASE_DIR/"out"/"find"로 설정되어 실제 경로가
#   "./1.data/out/out/find"가 됩니다. 일반적으로 "./1.data/out/find"를 원한다면
#   코드에서 SAVE_DIR = BASE_DIR/"find" 로 수정하세요.

import re
from pathlib import Path
import pandas as pd
from collections import defaultdict

# -------------------------------
# 경로 및 파일 지정
# -------------------------------
BASE_DIR = Path("./1.data/out")             # 원본 파일 폴더
TARGET_FILES = [
    "이동정리현황_기간내.xlsx",
    "일반용조서(말소용)_기간내.xlsx",
    "토지(임야)기본_기간내.xlsx",
]

# 연결/검색에 사용할 PNU 후보 컬럼
TARGET_COLS = ["이동전_필지코드", "이동후_필지코드", "필지코드", "PNU"]

# 결과 저장 폴더
SAVE_DIR = BASE_DIR /"out"/ "find"
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# 연계 탐색 깊이 (None 이면 신규 PNU가 더 안 나올 때까지)
MAX_DEPTH = None  # 예: 5 로 제한하려면 5로 설정


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


def pnu_cols_in(df: pd.DataFrame) -> list[str]:
    """데이터프레임에 존재하는 PNU 후보 컬럼 반환"""
    return [c for c in TARGET_COLS if c in df.columns]


def add_norm_columns(df: pd.DataFrame, cols: list[str]) -> list[str]:
    """cols 각각에 대해 정규화 컬럼을 추가하고, 생성된 정규화 컬럼명을 반환"""
    norm_cols = []
    for c in cols:
        nc = f"__norm__{c}"
        if nc not in df.columns:
            df[nc] = df[c].map(normalize_pnu)
        norm_cols.append(nc)
    return norm_cols


def extract_pnus_from_rows(df: pd.DataFrame, rows_idx, norm_cols: list[str]) -> set[str]:
    """선택된 행들에서 norm_cols의 PNU들을 모두 추출하여 집합으로 반환"""
    if len(rows_idx) == 0:
        return set()
    sub = df.loc[rows_idx, norm_cols]
    vals = set()
    for c in norm_cols:
        vals.update(sub[c].dropna().tolist())
    vals.discard("")  # 빈값 제거
    return vals


# -------------------------------
# BFS (모든 파일 전역 확장)
# -------------------------------
def bfs_expand(all_dfs: dict, start_pnu: str) -> tuple[dict, set[str]]:
    """
    all_dfs: {fname: (df, norm_cols, used_cols)}
    start_pnu: 시작 PNU(정규화)
    반환:
      - per_file_matches: {fname: {row_index: hop}}  # 발견 행의 최소 hop
      - discovered_pnus: set[str]  # 최종 발견된 모든 PNU
    """
    discovered = set([start_pnu])
    frontier = set([start_pnu])
    per_file_matches = {fname: dict() for fname in all_dfs.keys()}  # row_index -> hop

    depth = 0
    while frontier and (MAX_DEPTH is None or depth <= MAX_DEPTH):
        next_frontier = set()
        # 각 파일에서 frontier와 매칭되는 행 찾기
        for fname, (df, norm_cols, _) in all_dfs.items():
            # 행 매칭 (OR)
            mask = None
            for nc in norm_cols:
                m = df[nc].isin(frontier)
                mask = m if mask is None else (mask | m)
            matched_idx = df.index[mask] if mask is not None else []

            # 행 hop 기록(최소 hop만 유지)
            for ridx in matched_idx:
                if ridx not in per_file_matches[fname]:
                    per_file_matches[fname][ridx] = depth

            # 매칭된 행에서 신규 PNU 추출
            new_pnus = extract_pnus_from_rows(df, matched_idx, norm_cols)
            # 아직 발견되지 않았던 것만 다음 frontier로
            for p in new_pnus:
                if p and p not in discovered:
                    next_frontier.add(p)

        # frontier 갱신
        discovered |= next_frontier
        frontier = next_frontier
        depth += 1

        # 깊이 제한 체크 (depth는 0부터 시작하므로, MAX_DEPTH가 0이면 시작 PNU만)
        if MAX_DEPTH is not None and depth > MAX_DEPTH:
            break

    return per_file_matches, discovered


def print_and_save(fname: str, df: pd.DataFrame, used_cols: list[str], row_hops: dict[int, int], save_dir: Path):
    print("=" * 96)
    print(f"[{fname}] 사용 PNU 컬럼: {used_cols or '없음'} / 매칭 행: {len(row_hops)}")
    if len(row_hops) == 0:
        print("(일치하는 레코드 없음)")
        return

    # 매칭된 행만 추출 + hop 정보 추가
    rows = sorted(row_hops.keys())
    out = df.loc[rows].copy()
    out["__hop__"] = [row_hops[i] for i in rows]

    with pd.option_context("display.max_rows", None,
                           "display.max_columns", None,
                           "display.width", 240):
        print(out.to_string(index=False))

    save_name = f"검색결과_{Path(fname).stem}_연계.xlsx"
    save_path = save_dir / save_name
    try:
        out.to_excel(save_path, index=False)
        print(f"[{fname}] 결과 저장 완료 → {save_path}")
    except Exception as e:
        print(f"[{fname}] 저장 실패: {e}")


# -------------------------------
# 메인
# -------------------------------
if __name__ == "__main__":
    # 검색할 필지코드 입력
    target_pnu = input("검색할 필지코드를 입력하세요: ").strip()
    pnu_norm = normalize_pnu(target_pnu)

    print(f"[INFO] 시작 PNU(정규화): {pnu_norm}")
    print(f"[INFO] 원본 경로       : {BASE_DIR}")
    print(f"[INFO] 결과 저장 경로  : {SAVE_DIR}")
    print(f"[INFO] 깊이 제한       : {'무제한' if MAX_DEPTH is None else MAX_DEPTH}")

    # 1) 파일 로딩 및 정규화 컬럼 준비
    all_dfs = {}  # fname -> (df, norm_cols, used_cols)
    for fname in TARGET_FILES:
        fpath = BASE_DIR / fname
        if not fpath.exists():
            print("=" * 96)
            print(f"[{fname}] 파일 없음: {fpath}")
            continue
        try:
            df = load_excel(fpath)
            used_cols = pnu_cols_in(df)
            if not used_cols:
                print("=" * 96)
                print(f"[{fname}] 참고: PNU 후보 컬럼이 없습니다. ({TARGET_COLS})")
                # 그래도 all_dfs에는 넣지 않음(연계에 기여하지 못함)
                continue
            norm_cols = add_norm_columns(df, used_cols)
            all_dfs[fname] = (df, norm_cols, used_cols)
        except Exception as e:
            print("=" * 96)
            print(f"[{fname}] 로딩 실패: {e}")

    if not all_dfs:
        raise SystemExit("[ERROR] 로딩 가능한 파일이 없습니다.")

    # 2) BFS로 모든 파일을 가로질러 연계 확장
    per_file_matches, discovered_pnus = bfs_expand(all_dfs, pnu_norm)

    print("=" * 96)
    print(f"[SUMMARY] 발견된 PNU 개수: {len(discovered_pnus)}")
    # 필요 시 전체 PNU 목록을 찍고 싶다면 주석 해제
    # print(sorted(discovered_pnus))

    # 3) 파일별 출력/저장
    for fname, (df, norm_cols, used_cols) in all_dfs.items():
        row_hops = per_file_matches.get(fname, {})
        print_and_save(fname, df, used_cols, row_hops, SAVE_DIR)
