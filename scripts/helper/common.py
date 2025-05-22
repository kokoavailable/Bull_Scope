"""
 Script 전용. FAST API 앱에서 쓰이는 공통 함수와 설정을 정의한 파일입니다.
"""
import configparser
from datetime import datetime
import logging
from pathlib import Path
from dotenv import load_dotenv
import os

# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

app_env = os.getenv('APP_ENV', 'LOCAL')
env_file = f".env.{app_env}"

load_dotenv(env_file)

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




def setup_logging():
    """
    로깅 설정 함수
    파일 핸들러와 로그 핸들러를 사용합니다.
    """
    _logger = logging.getLogger('main')

    # 로그 레벨 설정
    _logger.setLevel(logging.DEBUG)  # 개별 로거 레벨 설정

    # 로그 파일 경로 설정
    log_file_path = os.path.join(
        '..', 
        'logs', 
        f"{app_env}_{datetime.now().strftime('%Y-%m-%d')}.log"
    )

    # 로그 디렉토리 생성
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

    # 파일 핸들러 설정
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)  # 파일에는 INFO 이상의 로그만 저장
    file_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s', 
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_formatter)
    _logger.addHandler(file_handler)

    # 콘솔 핸들러
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)  # 콘솔 로그 레벨
    console_formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console_handler.setFormatter(console_formatter)
    logging.getLogger('').addHandler(console_handler)

    return _logger

# 로그 설정 함수 호출 싱글톤
logger = setup_logging()

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
