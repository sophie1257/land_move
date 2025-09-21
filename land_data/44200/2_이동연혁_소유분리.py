"""
===========================================
토지이동정리현황 → 이동연혁 / 소유자이력 분리
===========================================

[목적]
- 하나의 통합 엑셀(기간내 자료)에서
  1) 토지이동연혁 관련 컬럼만 분리 저장
  2) 소유자변경이력 관련 컬럼만 분리 저장
- 선행 0(문자열) 보존

[입력 파일]
- ./1.data/out/44200_기간내_자료.xlsx

[출력 파일]
- ./1.data/out/44200_20240102-20250630_토지이동연혁.xlsx
- ./1.data/out/44200_24010102-20250630_소유자변경이력.xlsx

[실행 방법]
터미널에서:
    python 2_이동연혁_소유분리.py
"""

from pathlib import Path
import pandas as pd
#---------------------------------------------------
# 경로
#---------------------------------------------------

# 입력/출력 경로
INFILE = Path("./1.data/out/44200_기간내_자료.xlsx")  # 필요에 맞게 수정
OUT1   = Path("./1.data/out/44200_20240102-20250630_토지이동연혁.xlsx")
OUT2   = Path("./1.data/out/44200_24010102-20250630_소유자변경이력.xlsx")
OUT1.parent.mkdir(parents=True, exist_ok=True)

# 시군구
DISTRICT_CODE = "44200"

# 1) 로드: 모든 값을 문자열로 불러와 선행 0 보존
df = pd.read_excel(INFILE, dtype=str)

# 2) 컬럼 선택 (파일에 실제 존재하는 컬럼만 교집합으로 안전하게 선택)
cols_set_1 = [
    "이동전_필지코드","이동후_필지코드","토지이동종목","정리일자","신청구분","행정구역명",
    "이동전_지목","이동전_면적","이동후_지목","이동후_면적","이동전지번수","이동후지번수"
]
cols_set_2 = ["현재_소유구분","현재_소유자명","현재_소유자주소"]

pick1 = [c for c in cols_set_1 if c in df.columns]
pick2 = [c for c in cols_set_2 if c in df.columns]

df1 = df[pick1].copy()
df2 = df[pick2].copy()

# 3) 저장: pandas → openpyxl 엔진 (dtype=str로 로드했으므로 선행 0 그대로 보존)
with pd.ExcelWriter(OUT1, engine="openpyxl") as w:
    df1.to_excel(w, sheet_name="토지이동연혁", index=False)

with pd.ExcelWriter(OUT2, engine="openpyxl") as w:
    df2.to_excel(w, sheet_name="소유자변경이력", index=False)
