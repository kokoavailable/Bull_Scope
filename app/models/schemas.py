from pydantic import BaseModel, Field
from datetime import date, datetime
from typing import List, Dict, Any, Optional, Union

# 필터링 기준 스키마
class StockFilterCriteria(BaseModel):
    rsi_max: Optional[float] = Field(None, description="RSI 최대값 (예: 35)")
    debt_to_equity_max: Optional[float] = Field(None, description="부채비율 최대값 (예: 3.0)")
    current_ratio_min: Optional[float] = Field(None, description="유동비율 최소값 (예: 0.5)")
    roe_min: Optional[float] = Field(None, description="ROE 최소값 (예: 0.1)")
    pe_ratio_max: Optional[float] = Field(None, description="P/E 최대값 (예: 20)")
    pb_ratio_max: Optional[float] = Field(None, description="P/B 최대값 (예: 3)")
    check_golden_cross: Optional[bool] = Field(None, description="MACD 골든 크로스 확인")

# 주식 정보 스키마
class StockBase(BaseModel):
    symbol: str
    company_name: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    country: Optional[str] = None

class StockPrice(BaseModel):
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: int

class StockFundamental(BaseModel):
    date: date
    market_cap: Optional[float] = None
    pe_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    roe: Optional[float] = None
    roa: Optional[float] = None
    eps: Optional[float] = None
    revenue: Optional[float] = None
    net_income: Optional[float] = None

class StockTechnical(BaseModel):
    date: date
    rsi_14: Optional[float] = None
    macd: Optional[float] = None
    macd_signal: Optional[float] = None
    macd_histogram: Optional[float] = None
    ma_20: Optional[float] = None
    ma_50: Optional[float] = None
    ma_200: Optional[float] = None
    bolinger_upper: Optional[float] = None
    bolinger_middle: Optional[float] = None
    bolinger_lower: Optional[float] = None
    ppo: Optional[float] = None
    golden_cross: Optional[bool] = None

class SupportResistance(BaseModel):
    price_level: float
    is_support: bool
    strength: int

class StockOpportunity(BaseModel):
    symbol: str
    company_name: str
    sector: Optional[str] = None
    latest_price: Optional[float] = None
    latest_date: Optional[date] = None
    rsi: Optional[float] = None
    debt_to_equity: Optional[float] = None
    current_ratio: Optional[float] = None
    nearest_support: Optional[float] = None
    nearest_resistance: Optional[float] = None
    potential_gain: Optional[float] = None
    potential_loss: Optional[float] = None
    risk_reward_ratio: Optional[float] = None

class StockAnalysis(BaseModel):
    symbol: str
    company_name: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    latest_price: Optional[float] = None
    latest_date: Optional[date] = None
    technical: Dict[str, Any]
    fundamental: Dict[str, Any]
    support_resistance: List[Dict[str, Any]]
    opportunity_score: Optional[float] = None
    analysis_summary: str

# 시장 지표 스키마
class MarketIndicator(BaseModel):
    date: date
    vix: Optional[float] = None
    fear_greed_index: Optional[int] = None
    market_trend: Optional[str] = None

class MarketIndex(BaseModel):
    latest: Optional[float] = None
    change: Optional[float] = None
    change_percent: Optional[float] = None

class MarketIndices(BaseModel):
    kospi: Optional[MarketIndex] = None
    kosdaq: Optional[MarketIndex] = None
    sp500: Optional[MarketIndex] = None
    nasdaq: Optional[MarketIndex] = None
    dow: Optional[MarketIndex] = None
    date: Optional[date] = None

class SectorPerformance(BaseModel):
    sector: str
    etf_symbol: str
    performance_1m: float
    latest_price: float

class MarketNews(BaseModel):
    title: str
    source: str
    url: str
    published_date: date
    summary: str

class MarketAnalysis(BaseModel):
    latest_indicators: Dict[str, Any]
    vix_trend: List[Dict[str, Any]]
    fear_greed: Dict[str, Any]
    market_indices: Dict[str, Any]
    sector_performance: List[Dict[str, Any]]
    market_news: List[Dict[str, Any]]
    summary: str
    analysis_date: date

# 응답 스키마
class StockListResponse(BaseModel):
    stocks: List[StockBase]
    count: int

class StockPriceResponse(BaseModel):
    symbol: str
    company_name: str
    prices: List[StockPrice]

class StockOpportunityResponse(BaseModel):
    opportunities: List[StockOpportunity]
    count: int
    filter_criteria: Dict[str, Any]
    analysis_date: date

class StockAnalysisResponse(BaseModel):
    analysis: StockAnalysis
    analysis_date: date

class MarketAnalysisResponse(BaseModel):
    market_analysis: MarketAnalysis