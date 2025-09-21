# ==============================================
# 토지이동종목별 레코드 추출 및 엑셀 저장
# ==============================================

# [목적]
# - 이동정리현황.xlsx 파일에서 '토지이동종목' 기준으로 특정 종목만 필터링
# - 매핑된 코드(10, 20, 30, 40)에 따라 레코드 전체를 시트별로 저장

# [입력 파일]
# - ./1.data/out/이동정리현황.xlsx

# [출력 파일]
# - ./1.data/out/이동정리현황_종목별.xlsx

# [필요 모듈 설치]
# pip install pandas openpyxl


from pathlib import Path
import pandas as pd

# -------------------- 설정 --------------------
INPUT_FILE = Path("./1.data/out/이동정리현황_기간내.xlsx")
OUTPUT_FILE = INPUT_FILE.parent / "이동정리현황_종목별.xlsx"

# 이동종목 매핑 (문자열 → 코드)
CATEGORY_MAP = {
    "등록사항정정(토지대장)": "10",
    "분할(임야대장)": "20",
    "분할(토지대장)": "20",
    "합병(토지대장)": "30",
    "지목변경(토지대장)": "40",
}

# -------------------- 실행 --------------------
def main():
    # 엑셀 읽기 (모든 셀 텍스트로 처리)
    df = pd.read_excel(INPUT_FILE, dtype=str)

    # 엑셀 저장 준비
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        for name, code in CATEGORY_MAP.items():
            # 조건 필터링
            subset = df[df["토지이동종목"] == name].copy()
            if subset.empty:
                print(f"[건너뜀] {name} ({code}) → 레코드 없음")
                continue

            # 시트명 = 코드
            sheet_name = f"{code}_{name[:4]}"  # 너무 길면 앞 4글자만
            subset.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"[저장됨] {name} ({code}) → {len(subset)}건")

    print(f"\n완료! 결과 파일: {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
