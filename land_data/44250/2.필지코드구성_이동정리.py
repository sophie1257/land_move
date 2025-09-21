# ===========================================
# 토지이동정리현황 CSV 정제 → 필지코드(19자리) 생성 → Excel 저장
# ===========================================

# [목적]
# - CSV 파일에서 이동전/이동후 지번을 이용하여 19자리 필지코드 생성
# - 지목, 소유구분 등 값 정제
# - 불필요한 컬럼 삭제
# - openpyxl을 이용해 모든 셀을 텍스트 형식으로 지정하여 Excel로 저장

# [실행파일]
# 1-3.필지코드구성_이동정리_종목코드매핑.py

# [실행방법]
# 터미널에서:
#     python 3.필지코드_생성_정제.py

# [필요한 모듈]
# - pandas
# - openpyxl
# - pathlib (표준 모듈, 별도 설치 불필요)
# - re (표준 모듈, 별도 설치 불필요)

# [필요한 모듈 설치 방법]
# 가상환경 또는 시스템 환경에서 다음 명령 실행:
#     pip install pandas openpyxl

# (윈도우 PowerShell 환경)
#     python -m pip install pandas openpyxlimport pandas as pd

from pathlib import Path
import re
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import numbers

SRC = Path("./1.data/in/토지이동정리현황(소유권포함).csv")  # <- 업로드 파일 경로
OUT = Path("./44250/1.data/out/토지이동정리현황_필지코드추가.xlsx")  # <- 저장 파일 경로
DROP_COLS = [
    "지역코드", "대장구분", "이동전_지번","이동후_지번",
    "일련번호", "공시지가", "공시지가_수시", "전년지가", "전년지가_수시",
    "2년전지가", "2년전지가_수시", "3년전지가", "3년전지가_수시",
    "4년전지가", "4년전지가_수시",
    "신청_소유구분", "신청_소유자명", "신청_소유자등록번호", "신청_소유자주소",
]

def read_csv_keep_strings(path: Path) -> pd.DataFrame:
    encodings = ["utf-8-sig", "utf-8", "cp949"]
    last_err = None
    for enc in encodings:
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except Exception as e:
            last_err = e
    raise last_err

def digits_only(x) -> str:
    if x is None:
        return ""
    return re.sub(r"\D", "", str(x))

def mk_pnu(region, ledger, jibun) -> str:
    reg = digits_only(region).zfill(10)
    led = (str(ledger) if ledger is not None else "").strip()[:1].zfill(1)
    jbn = digits_only(jibun).zfill(8)
    return (reg + led + jbn).zfill(19)

df = read_csv_keep_strings(SRC)

# 필수 컬럼 확인
need_cols = ["지역코드", "대장구분", "이동전_지번", "이동후_지번"]
missing = [c for c in need_cols if c not in df.columns]
if missing:
    raise ValueError(f"필수 컬럼이 없습니다: {missing}")

# 19자리 필지코드 생성
df["이동전_필지코드"] = [mk_pnu(r, l, jb) for r, l, jb in zip(df["지역코드"], df["대장구분"], df["이동전_지번"])]
df["이동후_필지코드"] = [mk_pnu(r, l, jb) for r, l, jb in zip(df["지역코드"], df["대장구분"], df["이동후_지번"])]

# 지목 정제: 하이픈 제거 후 숫자만
for col in ["이동전_지목", "이동후_지목"]:
    if col in df.columns:
        df[col] = (
            df[col].fillna("").astype(str)
            .str.replace("-", "", regex=False)
            .apply(digits_only)
        )

# 현재_소유구분: 숫자만 + 선행 0 제거
if "현재_소유구분" in df.columns:
    cleaned = df["현재_소유구분"].fillna("").astype(str).apply(digits_only)
    df["현재_소유구분"] = cleaned.apply(lambda s: s.lstrip("0") if s != "" else "")

# 지정 컬럼 삭제
df = df.drop(columns=[c for c in DROP_COLS if c in df.columns], errors="ignore")

# PNU 컬럼을 맨 앞으로
front = ["이동전_필지코드", "이동후_필지코드"]
df = df[[c for c in front if c in df.columns] + [c for c in df.columns if c not in front]]

# 엑셀(텍스트 서식) 저장
OUT.parent.mkdir(parents=True, exist_ok=True)
wb = Workbook()
ws = wb.active
ws.title = "data"

for r_idx, row in enumerate(dataframe_to_rows(df, index=False, header=True), start=1):
    ws.append(row)
    for c_idx in range(1, len(row) + 1):
        ws.cell(row=r_idx, column=c_idx).number_format = numbers.FORMAT_TEXT #텍스트 형식으로 저장

wb.save(OUT)
print(f"Saved: {OUT}")
