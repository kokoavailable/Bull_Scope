# migrations/init_db.py
import psycopg2
from helper.common import DB_PARAMS

def run_migration(sql_path: str = "migrations/20250528_create_tables.sql"):
    conn = psycopg2.connect(**DB_PARAMS)
    try:
        with conn.cursor() as cur, open(sql_path, "r") as f:
            cur.execute(f.read())
        conn.commit()
        print("✅ 테이블 생성 완료")
    except Exception as e:
        conn.rollback()
        print("❌ 마이그레이션 실패:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    run_migration()