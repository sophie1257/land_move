# =========================================================
#  이동정리현황_기간내.xlsx → MySQL 테이블 업로드 스크립트
# =========================================================

# [목적]
# - 기간내 엑셀 파일(이동정리현황_기간내.xlsx)을 읽어
#   모든 컬럼을 문자열(VARCHAR)로 변환하여 MySQL DB에 적재
# - 선행 0 보존을 위해 dtype=str 로 로딩, VARCHAR(255) 매핑
# - 문자셋/콜레이션은 utf8mb4_general_ci 로 강제 설정

# [입력 파일]
# - ./1.data/out/이동정리현황_기간내.xlsx

# [출력 대상 (DB)]
# - DB: landmove
# - Table: land_move
# - 컬럼 타입: VARCHAR(255)

# [실행 방법]
# > python <이파일이름>.py
# 성공 시: "[OK] landmove.land_move 적재 완료" 출력

# [의존성]
# - pandas
# - sqlalchemy
# - pymysql (MySQL 드라이버)

# [주의]
# - DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME, TABLE 값을
#   실제 환경에 맞게 수정해야 함
# - 테이블이 기존에 존재하면 if_exists="replace" 로 DROP 후 재생성됨

import re
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String

EXCEL_PATH = "./1.data/out/이동정리현황_기간내.xlsx"
DB_USER, DB_PASS = "root", 1234 
DB_HOST, DB_PORT = "127.0.0.1", 3306
DB_NAME, TABLE   = "landmove", "land_move"

# 1) 엑셀 로딩
df = pd.read_excel(EXCEL_PATH, dtype=str).fillna("")
df.columns = [re.sub(r"\s+", "", str(c)) for c in df.columns]  # 컬럼명 공백 제거

# 2) 엔진 (utf8mb4 지정)
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4",
    pool_pre_ping=True,
    isolation_level="AUTOCOMMIT",
)

# 3) 세션 문자셋/콜레이션 고정(방어적) - general_ci로 맞춤
with engine.begin() as conn:
    conn.execute(text("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci"))
    conn.execute(text("SET character_set_client = utf8mb4"))
    conn.execute(text("SET character_set_results = utf8mb4"))
    conn.execute(text("SET collation_connection = utf8mb4_general_ci"))

# 4) 적재
dtype_map = {col: String(255) for col in df.columns}  # 모두 VARCHAR로 (선행0 보존)
df.to_sql(TABLE, engine, if_exists="replace", index=False,
          dtype=dtype_map, method="multi", chunksize=1000)

print(f"[OK] {DB_NAME}.{TABLE} 적재 완료")
