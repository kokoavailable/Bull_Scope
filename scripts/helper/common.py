"""
 Script 전용. FAST API 앱에서 쓰이는 공통 함수와 설정을 정의한 파일입니다.
"""
import configparser
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from loguru import logger
from pathlib import Path
from dotenv import load_dotenv
import os
import pandas as pd
import requests
import sys

from functools import lru_cache

from typing import List

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

# 셀레니움 옵션 객체
opts = Options()
opts.add_argument("--headless")
opts.add_argument("--disable-gpu")
opts.add_argument("--no-sandbox")

def get_driver():
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)

    return driver

DB_PARAMS = {
    "dbname": rdb_name,
    "user": rdb_user,
    "password": rdb_password,
    "host": rdb_host,
    "port": rdb_port
}

MARKET_INDICATOR_CODES = ["vix", "fear_greed_index"]

URL_NASDAQ_LISTING = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"

@lru_cache  # 한 번만 내려받아 메모리에 캐싱
def fetch_stock_symbols() -> list[str]:
    df = pd.read_csv(URL_NASDAQ_LISTING, sep="|")

    # 티커 전처리
    df = df[df["Test Issue"] != "Y"]
    df = df[df["Security Name"].str.contains("Common Stock", na=False)]
    df = df[~df["Symbol"].str.contains(r"[.$]", regex=True)]
    df = df[df["Symbol"].notna()]
    return df.loc[df["Symbol"].notna(), "Symbol"].tolist()

##### 로컬 환경일 때만 config.ini 읽기

# if app_env == 'LOCAL':
#     config = configparser.ConfigParser()
#     current_dir = Path(__file__).resolve().parent
#     config_path = current_dir / 'config.ini'

#     if not config_path.exists():
#         raise FileNotFoundError(f"Configuration file not found: {config_path}")


#     config_read = config.read(config_path, encoding='utf-8')
#     if not config_read:
#         raise FileNotFoundError(f"Configuration file not read: {config_path}")

###### 로거

# ──────────────────────────────────────────────
# 헬퍼: 리스트 → 고정 크기 배치
# ──────────────────────────────────────────────
def chunked(seq: List[str], n: int = 100):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]

import os
print("LOG_LEVEL:", os.environ.get("LOG_LEVEL"))

def setup_logging():
    """
    로깅 설정 함수
    파일 핸들러와 로그 핸들러를 사용합니다.
    """
    logger.remove()

    if 'AIRFLOW_HOME' in os.environ:
        log_dir = os.path.join(os.environ['AIRFLOW_HOME'], 'logs', 'custom')
    else:
        log_dir = './logs'
    
    log_file_path = os.path.join(
        log_dir,
        f"{app_env}_{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    
    # 로그 디렉토리 생성
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)


    logger.add(
        sink=sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level="INFO",
        colorize=True
    )

    # 파일 핸들러 추가
    logger.add(
        sink=log_file_path,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="INFO",
        rotation="1 day",  # 하루마다 로그 파일 회전
        retention="30 days",  # 30일간 로그 파일 보관
        compression="zip",  # 압축 저장
        encoding="utf-8"
    )
    
    return logger


# 로그 설정 함수 호출 싱글톤
logger = setup_logging()

driver = get_driver()

# ### RDB 연결 정보
# rdb_user = config.get(app_env, 'RDB_USER')
# rdb_password = config.get(app_env, 'RDB_PASSWORD')
# rdb_host = config.get(app_env, 'RDB_HOST')
# rdb_port = config.get(app_env, 'RDB_PORT')
# rdb_name = config.get(app_env, 'RDB_NAME')

# ##### 동기 RDB
# engine = create_engine(
#     f"postgresql+psycopg2://{rdb_user}:{rdb_password}@"
#     f"{rdb_host}:{rdb_port}/{rdb_name}"
# )
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# # 동기 DB 세션 종속성
# def get_db():
#     """
#     동기 함수에서 사용할 DB 세션 종속성입니다.
#     """
#     db = SessionLocal()
#     try:
#         yield db
#         db.commit()
#     except Exception:
#         logger.error("An error occurred", exc_info=True)
#         db.rollback()
#         raise
#     finally:
#         db.close()


# ##### 비동기 RDB
# async_engine = create_async_engine(
#     f"postgresql+asyncpg://{rdb_user}:{rdb_password}@"
#     f"{rdb_host}:{rdb_port}/{rdb_name}', echo=True)"
# )
# AsyncSessionLocal = sessionmaker(bind=async_engine, expire_on_commit=False, class_=AsyncSession)

# # 비동기 DB 세션 종속성
# async def get_async_db():
#     """
#     비동기 함수에서 사용할 DB 세션 종속성입니다.
#     """
#     async with AsyncSessionLocal() as db:
#         try:
#             yield db
#             await db.commit()
#         except Exception:
#             logger.error("An error occurred", exc_info=True)
#             await db.rollback()
#             raise


if __name__ == "__main__":
    logger.info("이건 무조건 떠야 정상 (loguru 단독 테스트)")
    print("이건 print 테스트")