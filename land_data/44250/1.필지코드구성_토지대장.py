# ==========================================
#  토지(임야) 기본 CSV → 필지코드(19자리) 생성
# ==========================================

# [목적]
# - 원본 CSV(토지(임야)기본(전체)(지방세용).csv)에서 행정구역, 소재지, 대장구분, 본번, 부번 정보를 결합하여
#   19자리 필지코드를 생성하고 검증 길이(len) 컬럼 추가
# - 사용된 원본 컬럼 제거 후 새로운 Excel 파일로 저장
# - 모든 셀은 텍스트 서식(@)으로 저장하여 선행 0 보존

# [입력 파일]
# - ./44250/1.data/in/토지(임야)기본(전체)(지방세용).csv

# [출력 파일]
# - ./1.data/out/토지(임야)기본_필지코드추가.xlsx

# [실행 방법]
# > python 1.필지코드구성_토지대장.py


import pandas as pd
import re
from pathlib import Path
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
#지목 하이픈 제거 후 분리 필요여부 확인
# ── 경로 설정
BASE_DIR = Path(__file__).resolve().parent.parent
file1 = BASE_DIR /"44250"/"1.data"/"in"/"토지(임야)기본(전체)(지방세용).csv"
out_dir = Path.cwd() / "./44250/1.data/out"
out_dir.mkdir(parents=True, exist_ok=True)

# ── CSV 파일 읽기
def read_csv_try(path, encodings=("utf-8", "utf-8-sig", "cp949")):
    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc)
            print(f"{path} → 인코딩 {enc} 성공 (shape={df.shape})")
            return df
        except Exception as e:
            last_err = e
    raise last_err

# ── 숫자만 추출
def digits_only(x):
    if pd.isna(x):
        return ""
    return re.sub(r"[^0-9]", "", str(x))

# ── 엑셀 저장 (항상 1번 시트에 기록)
def save_as_text_excel(df, out_path, sheet_name="Sheet1"):
    from openpyxl import Workbook
    from openpyxl.utils.dataframe import dataframe_to_rows

    wb = Workbook()
    ws = wb.active            # 기본으로 생성된 1번 시트
    ws.title = sheet_name     # 1번 시트 이름 변경

    # 데이터 쓰기 (헤더 포함, 모든 셀 텍스트 서식)
    for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), 1):
        for c_idx, value in enumerate(row, 1):
            cell = ws.cell(row=r_idx, column=c_idx, value=str(value))
            cell.number_format = "@"

    wb.save(out_path)


# ── 컬럼 자동 탐색
def find_col(df, keywords):
    keywords = [k.lower() for k in keywords]
    for col in df.columns:
        low = col.lower()
        for k in keywords:
            if k in low:
                return col
    return None

# ── 필지코드 생성 함수
def make_concat_pnu(df, components, result_col_name, zfill_map=None):
    parts = []
    for c in components:
        if c in df.columns:
            col_series = df[c].astype(str).fillna("").apply(digits_only)
            if zfill_map and c in zfill_map:
                col_series = col_series.apply(lambda x: x.zfill(zfill_map[c]))
            parts.append(col_series)
        else:
            parts.append(pd.Series([digits_only(c)] * len(df)))
    pnu = parts[0]
    for part in parts[1:]:
        pnu = pnu.str.cat(part, na_rep="")

    # 맨 앞(0번 위치)에 삽입 (len 컬럼은 생성 안 함)
    df.insert(0, result_col_name, pnu)
    return df

#---필지코드 19자리 길이 검증
def count_bad_pnu_length(df, col="필지코드(19자리)", expected=19):
    return df[col].astype(str).str.len().ne(expected).sum()

# ── 첫 번째 파일 처리
df1 = read_csv_try(file1)

col_region = find_col(df1, ["행정", "행정구역", "지역코드", "지역", "시도", "시군구"])
col_landloc = find_col(df1, ["토지소재", "토지소재지", "토지소재코드", "소재", "지번"])
col_deung = find_col(df1, ["대장구분", "구분", "대장"])
col_bon = find_col(df1, ["본번", "본", "본번_"])
col_bu = find_col(df1, ["부번", "부", "부번_"])

print("\n[컬럼 자동 탐지 결과]")
print(f"행정구역 코드  : {col_region}")
print(f"토지소재코드   : {col_landloc}")
print(f"대장구분       : {col_deung}")
print(f"본번           : {col_bon}")
print(f"부번           : {col_bu}")

#지목 / 소유구분 / 토지이동사유 / 소유권변동원인 정제
col_jimok = find_col(df1, ["지목"])
if col_jimok:
    df1[col_jimok] = df1[col_jimok].astype(str).apply(digits_only)
    print(f"지목 컬럼 처리 완료 → {col_jimok}")

col_owner = find_col(df1, ["소유구분"])
if col_owner:
    df1[col_owner] = df1[col_owner].astype(str).apply(digits_only)
    print(f"소유구분 컬럼 처리 완료 → {col_owner}")

col_move_reason = find_col(df1, ["토지이동사유", "이동사유", "토지이동종목"])
if col_move_reason:
    df1[col_move_reason] = df1[col_move_reason].astype(str).apply(digits_only)
    print(f"토지이동사유 컬럼 처리 완료 → {col_move_reason}")

col_owner_reason = find_col(df1, ["소유권변동원인", "변동원인", "원인"])
if col_owner_reason:
    df1[col_owner_reason] = df1[col_owner_reason].astype(str).apply(digits_only)
    print(f"소유권변동원인 컬럼 처리 완료 → {col_owner_reason}")



components1 = [col_region, col_landloc, col_deung, col_bon, col_bu]
zfill_map1 = {
    col_region: 5,    # 지역코드: 5자리
    col_landloc: 5,   # 토지소재코드: 5자리
    col_deung: 1,     # 대장구분: 1자리
    col_bon: 4,       # 본번: 4자리
    col_bu: 4         # 부번: 4자리
}

df1 = make_concat_pnu(df1, components1, "필지코드(19자리)", zfill_map=zfill_map1)

# ── 사용된 원본 컬럼 제거
cols_to_drop = [col_region, col_landloc, col_deung, col_bon, col_bu]
df1.drop(columns=[c for c in cols_to_drop if c in df1.columns], inplace=True)

# ── 엑셀 저장
out = out_dir / "토지(임야)기본_필지코드추가.xlsx"
save_as_text_excel(df1, out, sheet_name="토지기본_필지코드")

# ── 유효성(길이) 확인: 컬럼 없이 즉석 계산
pnu_col = "필지코드(19자리)"
bad_len_count = df1[pnu_col].astype(str).str.len().ne(19).sum()

print(f"\n엑셀 저장 완료: {out}")
print(f"총 행 수: {len(df1)}")
print(f"필지코드(19자리) 길이≠19 행 수: {bad_len_count}")

# 필요하면 문제 행 샘플 확인 (옵션)
if bad_len_count:
    print("\n[길이 불일치 샘플 5건]")
    print(df1.loc[df1[pnu_col].astype(str).str.len().ne(19), [pnu_col]].head())
