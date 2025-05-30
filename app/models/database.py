from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Boolean, ForeignKey, Table, BigInteger, Identity
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

# 데이터베이스 URL 설정
SQLALCHEMY_DATABASE_URL = "postgresql://postgres:password@localhost:5432/bullscope"

# SQLAlchemy 엔진 생성
engine = create_engine(SQLALCHEMY_DATABASE_URL)

# 세션 생성
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 모델 베이스 클래스 생성
Base = declarative_base()

# 데이터베이스 세션 의존성
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 주식 정보 모델
class Stock(Base):
    __tablename__ = "stocks"

    id = Column(BigInteger, Identity(always=True), primary_key=True)
    symbol = Column(String, unique=True, index=True)
    company_name = Column(String)
    sector = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    country = Column(String, nullable=True)
    last_updated = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 관계 설정
    prices = relationship("StockPrice", back_populates="stock")
    fundamentals = relationship("StockFundamental", back_populates="stock")
    technicals = relationship("StockTechnical", back_populates="stock")

# 주가 정보 모델
class StockPrice(Base):
    __tablename__ = "stock_prices"

    id = Column(BigInteger, Identity(always=True), primary_key=True)
    stock_id = Column(BigInteger, ForeignKey("stocks.id"))
    date = Column(Date, index=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    adj_close = Column(Float)
    volume = Column(Integer)
    
    # 관계 설정
    stock = relationship("Stock", back_populates="prices")

# 재무 정보 모델
class StockFundamental(Base):
    __tablename__ = "stock_fundamentals"

    id = Column(BigInteger, Identity(always=True), primary_key=True)
    stock_id = Column(BigInteger, ForeignKey("stocks.id"))
    date = Column(Date, index=True)
    market_cap = Column(Float, nullable=True)
    pe_ratio = Column(Float, nullable=True)
    pb_ratio = Column(Float, nullable=True)
    debt_to_equity = Column(Float, nullable=True)  # D/E 비율
    current_ratio = Column(Float, nullable=True)   # 유동비율
    quick_ratio = Column(Float, nullable=True)     # 당좌비율
    roe = Column(Float, nullable=True)             # 자기자본이익률
    roa = Column(Float, nullable=True)             # 총자산이익률
    eps = Column(Float, nullable=True)             # 주당순이익
    revenue = Column(Float, nullable=True)         # 매출액
    net_income = Column(Float, nullable=True)      # 순이익
    
    # 관계 설정
    stock = relationship("Stock", back_populates="fundamentals")

# 기술적 지표 모델
class StockTechnical(Base):
    __tablename__ = "stock_technicals"

    id = Column(BigInteger, Identity(always=True), primary_key=True)
    stock_id = Column(BigInteger, ForeignKey("stocks.id"))
    date = Column(Date, index=True)
    rsi_14 = Column(Float, nullable=True)          # RSI(14)
    macd = Column(Float, nullable=True)            # MACD 라인
    macd_signal = Column(Float, nullable=True)     # MACD 시그널 라인
    macd_histogram = Column(Float, nullable=True)  # MACD 히스토그램
    ma_20 = Column(Float, nullable=True)           # 20일 이동평균
    ma_50 = Column(Float, nullable=True)           # 50일 이동평균
    ma_200 = Column(Float, nullable=True)          # 200일 이동평균
    bolinger_upper = Column(Float, nullable=True)  # 볼린저 밴드 상단
    bolinger_middle = Column(Float, nullable=True) # 볼린저 밴드 중간
    bolinger_lower = Column(Float, nullable=True)  # 볼린저 밴드 하단
    ppo = Column(Float, nullable=True)             # PPO (Percentage Price Oscillator)
    ma_golden_cross = Column(Boolean, default=False)  # ma 골든 크로스 여부
    macd_golden_cross = Column(Boolean, default=False)  # MACD 골든 크로스 여부
    
    # 관계 설정
    stock = relationship("Stock", back_populates="technicals")

# 저항선/지지선 정보 모델
class SupportResistance(Base):
    __tablename__ = "support_resistance"

    id = Column(BigInteger, Identity(always=True), primary_key=True)
    stock_id = Column(BigInteger, ForeignKey("stocks.id"))
    date = Column(Date, index=True)
    price_level = Column(Float, nullable=False)     # 가격 수준
    is_support = Column(Boolean, default=True)      # 지지선 여부 (False면 저항선)
    strength = Column(Integer, default=1)           # 강도 (1-5)
    
    # 관계 설정
    stock = relationship("Stock")

# 시장 지표 모델
class MarketIndicator(Base):
    __tablename__ = "market_indicators"

    id = Column(BigInteger, Identity(always=True), primary_key=True)
    date = Column(Date, index=True, unique=True)
    vix = Column(Float, nullable=True)                  # VIX 지수
    fear_greed_index = Column(Integer, nullable=True)   # 공포/탐욕 지수 (0-100)
    market_trend = Column(String, nullable=True)        # 시장 추세 (Bullish, Bearish, Neutral)