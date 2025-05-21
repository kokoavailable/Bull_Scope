from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
import logging
from sqlalchemy.orm import Session

from app.models.database import get_db, engine, Base
from app.routers import stocks

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 데이터베이스 테이블 생성
Base.metadata.create_all(bind=engine)

# FastAPI 애플리케이션 생성
app = FastAPI(
    title="BullScope API",
    description="단기 급등주 검색 시스템 API",
    version="1.0.0"
)

# CORS 미들웨어 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 실제 환경에서는 허용할 도메인만 지정
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(stocks.router)

@app.get("/")
def read_root():
    """
    루트 엔드포인트
    """
    return {
        "name": "BullScope API",
        "version": "1.0.0",
        "description": "단기 급등주 검색 시스템 API"
    }

@app.get("/health")
def health_check(db: Session = Depends(get_db)):
    """
    헬스 체크 엔드포인트
    """
    try:
        # 데이터베이스 연결 확인
        db.execute("SELECT 1")
        db_status = "healthy"
    except Exception as e:
        logger.error(f"데이터베이스 연결 실패: {str(e)}")
        db_status = "unhealthy"
        
    return {
        "status": "running",
        "database": db_status
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)