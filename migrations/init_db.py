# migrations/init_db.py
import psycopg2

from pathlib import Path
from dotenv import load_dotenv
import os

# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

app_env = os.getenv('APP_ENV', 'LOCAL')
env_path = Path(__file__).resolve().parent / f".env.{app_env}"
load_dotenv(dotenv_path=env_path)

rdb_user = os.environ.get("RDB_USER")
rdb_password = os.environ.get("RDB_PASSWORD")
rdb_host = os.environ.get("RDB_HOST")
rdb_port = os.environ.get("RDB_PORT")
rdb_name = os.environ.get("RDB_NAME")
log_level = os.environ.get("LOG_LEVEL")

DB_PARAMS = {
    "dbname": rdb_name,
    "user": rdb_user,
    "password": rdb_password,
    "host": rdb_host,
    "port": rdb_port
}

def run_migration(sql_path: str):
    conn = psycopg2.connect(**DB_PARAMS)
    sql_path= "sql/seeds/20250528_create_tables.sql"
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