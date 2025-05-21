from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta

from app.models.database import get_db
from app.models.schemas import (
    StockBase, StockPrice, StockFundamental, StockTechnical,
    StockOpportunity, StockAnalysis, MarketAnalysis,
    StockFilterCriteria, StockListResponse, StockPriceResponse,
    StockOpportunityResponse, StockAnalysisResponse, MarketAnalysisResponse
)
from app.services.data_collector import DataCollector
from app.services.technical_analyzer import TechnicalAnalyzer
from app.services.fundamental_analyzer import FundamentalAnalyzer
from app.services.market_analyzer import MarketAnalyzer

router = APIRouter(
    prefix="/api/v1",
    tags=["stocks"]
)

# 모든 종목 목록 가져오기
@router.get("/stocks", response_model=StockListResponse)
def get_stocks(
    market: Optional[str] = Query("KRX", description="시장 (예: KRX, KOSPI, KOSDAQ)"),
    sector: Optional[str] = Query(None, description="섹터별 필터링"),
    db: Session = Depends(get_db)
):
    """
    종목 목록을 가져옵니다.
    """
    from app.models.database import Stock
    
    query = db.query(Stock)
    
    # 섹터 필터링
    if sector:
        query = query.filter(Stock.sector == sector)
        
    stocks = query.all()
    
    return {
        "stocks": stocks,
        "count": len(stocks)
    }

# 특정 종목 가격 데이터 가져오기
@router.get("/stocks/{symbol}/prices", response_model=StockPriceResponse)
def get_stock_prices(
    symbol: str,
    period: Optional[str] = Query("1y", description="기간 (예: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"),
    db: Session = Depends(get_db)
):
    """
    특정 종목의 가격 데이터를 가져옵니다.
    """
    from app.models.database import Stock, StockPrice
    
    # 종목 정보 조회
    stock = db.query(Stock).filter(Stock.symbol == symbol).first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"종목 {symbol}을(를) 찾을 수 없습니다.")
        
    # 기간에 따른 시작 날짜 계산
    end_date = datetime.now().date()
    if period == "1d":
        start_date = end_date - timedelta(days=1)
    elif period == "5d":
        start_date = end_date - timedelta(days=5)
    elif period == "1mo":
        start_date = end_date - timedelta(days=30)
    elif period == "3mo":
        start_date = end_date - timedelta(days=90)
    elif period == "6mo":
        start_date = end_date - timedelta(days=180)
    elif period == "1y":
        start_date = end_date - timedelta(days=365)
    elif period == "2y":
        start_date = end_date - timedelta(days=2*365)
    elif period == "5y":
        start_date = end_date - timedelta(days=5*365)
    elif period == "10y":
        start_date = end_date - timedelta(days=10*365)
    elif period == "ytd":
        start_date = datetime(end_date.year, 1, 1).date()
    else:  # "max" 또는 기타
        start_date = datetime(1900, 1, 1).date()
        
    # 가격 데이터 조회
    prices = db.query(StockPrice).filter(
        StockPrice.stock_id == stock.id,
        StockPrice.date >= start_date,
        StockPrice.date <= end_date
    ).order_by(StockPrice.date.asc()).all()
    
    return {
        "symbol": stock.symbol,
        "company_name": stock.company_name,
        "prices": prices
    }

# 종목 기회 탐색
@router.post("/stocks/opportunities", response_model=StockOpportunityResponse)
def find_stock_opportunities(
    criteria: StockFilterCriteria,
    db: Session = Depends(get_db)
):
    """
    설정된 기준에 맞는 투자 기회를 찾습니다.
    """
    # 기술적 분석기 초기화
    technical_analyzer = TechnicalAnalyzer(db)
    
    # 기회 스캔
    opportunities = technical_analyzer.scan_for_opportunities({
        "rsi_max": criteria.rsi_max,
        "debt_to_equity_max": criteria.debt_to_equity_max,
        "current_ratio_min": criteria.current_ratio_min,
        "check_golden_cross": criteria.check_golden_cross
    })
    
    return {
        "opportunities": opportunities,
        "count": len(opportunities),
        "filter_criteria": criteria.dict(exclude_none=True),
        "analysis_date": datetime.now().date()
    }

# 특정 종목 분석
@router.get("/stocks/{symbol}/analysis", response_model=StockAnalysisResponse)
def analyze_stock(
    symbol: str,
    db: Session = Depends(get_db)
):
    """
    특정 종목의 종합 분석을 수행합니다.
    """
    # 종목 정보 조회
    from app.models.database import Stock
    stock = db.query(Stock).filter(Stock.symbol == symbol).first()
    if not stock:
        raise HTTPException(status_code=404, detail=f"종목 {symbol}을(를) 찾을 수 없습니다.")
        
    # 기술적 분석
    technical_analyzer = TechnicalAnalyzer(db)
    technical_analysis = technical_analyzer.analyze_stock(symbol)
    
    # 재무 분석
    fundamental_analyzer = FundamentalAnalyzer(db)
    fundamental_analysis = fundamental_analyzer.analyze_stock(symbol)
    
    # 종합 분석 결과 생성
    analysis = {
        "symbol": symbol,
        "company_name": stock.company_name,
        "sector": stock.sector,
        "industry": stock.industry,
        "latest_price": technical_analysis.get("latest_price"),
        "latest_date": technical_analysis.get("latest_date"),
        "technical": {
            "rsi": technical_analysis.get("rsi"),
            "macd": technical_analysis.get("macd"),
            "ppo": technical_analysis.get("ppo"),
            "moving_averages": technical_analysis.get("moving_averages"),
            "is_oversold": technical_analysis.get("is_oversold"),
            "is_overbought": technical_analysis.get("is_overbought"),
            "is_uptrend": technical_analysis.get("is_uptrend"),
            "is_momentum_increasing": technical_analysis.get("is_momentum_increasing"),
            "recent_golden_cross": technical_analysis.get("recent_golden_cross"),
            "recent_dead_cross": technical_analysis.get("recent_dead_cross")
        },
        "fundamental": {
            "latest_data": fundamental_analysis.get("latest_data"),
            "industry_comparison": fundamental_analysis.get("industry_comparison"),
            "growth_metrics": fundamental_analysis.get("growth_metrics"),
            "financial_health": fundamental_analysis.get("financial_health"),
            "valuation": fundamental_analysis.get("valuation")
        },
        "support_resistance": technical_analysis.get("support_resistance", []),
        "opportunity_score": None,  # 기회 점수 계산 필요
        "analysis_summary": ""  # 분석 요약 생성 필요
    }
    
    # 기회 점수 계산 (0-100)
    score = 0
    score_count = 0
    
    # 기술적 기회 요소
    if analysis["technical"]["is_oversold"]:
        score += 100
        score_count += 1
        
    if analysis["technical"]["is_momentum_increasing"]:
        score += 75
        score_count += 1
        
    if analysis["technical"]["recent_golden_cross"]:
        score += 100
        score_count += 1
        
    # 재무적 기회 요소
    if analysis["fundamental"]["financial_health"] and analysis["fundamental"]["financial_health"].get("overall_score"):
        score += analysis["fundamental"]["financial_health"]["overall_score"]
        score_count += 1
        
    if analysis["fundamental"]["valuation"] and analysis["fundamental"]["valuation"].get("overall_rating") == "Undervalued":
        score += 100
        score_count += 1
    elif analysis["fundamental"]["valuation"] and analysis["fundamental"]["valuation"].get("overall_rating") == "Fair":
        score += 50
        score_count += 1
        
    # 기회 점수 계산
    analysis["opportunity_score"] = score / score_count if score_count > 0 else None
    
    # 분석 요약 생성
    summary = f"{stock.company_name}({symbol})은(는) "
    
    if analysis["technical"]["is_oversold"]:
        summary += "현재 과매도 상태로 반등 가능성이 있습니다. "
    elif analysis["technical"]["is_overbought"]:
        summary += "현재 과매수 상태로 조정 가능성이 있습니다. "
        
    if analysis["technical"]["is_uptrend"]:
        summary += "중장기 상승 추세를 보이고 있으며, "
    else:
        summary += "중장기 하락 추세를 보이고 있으며, "
        
    if analysis["technical"]["is_momentum_increasing"]:
        summary += "모멘텀이 증가하고 있습니다. "
    else:
        summary += "모멘텀이 감소하고 있습니다. "
        
    if analysis["technical"]["recent_golden_cross"]:
        summary += "최근 MACD 골든 크로스가 발생했습니다. "
        
    if analysis["fundamental"]["financial_health"] and analysis["fundamental"]["financial_health"].get("overall_score"):
        health_score = analysis["fundamental"]["financial_health"]["overall_score"]
        if health_score > 75:
            summary += "재무 건전성이 매우 우수합니다. "
        elif health_score > 50:
            summary += "재무 건전성이 양호합니다. "
        else:
            summary += "재무 건전성이 다소 낮습니다. "
            
    if analysis["fundamental"]["valuation"] and analysis["fundamental"]["valuation"].get("overall_rating"):
        valuation = analysis["fundamental"]["valuation"]["overall_rating"]
        if valuation == "Undervalued":
            summary += "현재 저평가 상태로 판단됩니다."
        elif valuation == "Fair":
            summary += "현재 적정 가치에 거래되고 있습니다."
        else:
            summary += "현재 고평가 상태로 판단됩니다."
            
    analysis["analysis_summary"] = summary
    
    return {
        "analysis": analysis,
        "analysis_date": datetime.now().date()
    }

# 시장 분석
@router.get("/market/analysis", response_model=MarketAnalysisResponse)
def get_market_analysis(
    db: Session = Depends(get_db)
):
    """
    시장 종합 분석을 가져옵니다.
    """
    # 시장 분석기 초기화
    market_analyzer = MarketAnalyzer(db)
    
    # 시장 데이터 업데이트
    market_analyzer.update_market_data()
    
    # 시장 분석 가져오기
    market_analysis = market_analyzer.get_market_analysis()
    
    return {
        "market_analysis": market_analysis
    }

# 종목 데이터 업데이트
@router.post("/admin/update-stock/{symbol}")
def update_stock_data(
    symbol: str,
    period: Optional[str] = Query("1y", description="기간 (예: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)"),
    analyze: Optional[bool] = Query(True, description="분석 수행 여부"),
    db: Session = Depends(get_db)
):
    """
    특정 종목의 데이터를 업데이트합니다.
    """
    # 데이터 수집기 초기화
    data_collector = DataCollector(db)
    
    # 종목 데이터 업데이트
    success = data_collector.save_stock_data(symbol, period)
    
    if not success:
        raise HTTPException(status_code=500, detail=f"{symbol} 데이터 업데이트에 실패했습니다.")
        
    # 분석 수행 (선택적)
    if analyze:
        technical_analyzer = TechnicalAnalyzer(db)
        technical_analyzer.analyze_stock(symbol)
        
    return {"message": f"{symbol} 데이터가 성공적으로 업데이트되었습니다."}

# 모든 종목 데이터 업데이트
@router.post("/admin/update-all-stocks")
def update_all_stocks(
    market: Optional[str] = Query("KRX", description="시장 (예: KRX, KOSPI, KOSDAQ)"),
    period: Optional[str] = Query("1y", description="기간 (예: 1y, 6mo, 3mo)"),
    db: Session = Depends(get_db)
):
    """
    모든 종목의 데이터를 업데이트합니다.
    """
    # 데이터 수집기 초기화
    data_collector = DataCollector(db)
    
    # 모든 종목 데이터 업데이트
    results = data_collector.update_all_stocks(market, period)
    
    success_count = sum(1 for success in results.values() if success)
    fail_count = len(results) - success_count
    
    return {
        "message": f"총 {len(results)}개 종목 중 {success_count}개 업데이트 성공, {fail_count}개 실패",
        "results": results
    }

# 시장 데이터 업데이트
@router.post("/admin/update-market")
def update_market_data(
    db: Session = Depends(get_db)
):
    """
    시장 데이터를 업데이트합니다.
    """
    # 시장 분석기 초기화
    market_analyzer = MarketAnalyzer(db)
    
    # 시장 데이터 업데이트
    success = market_analyzer.update_market_data()
    
    if not success:
        raise HTTPException(status_code=500, detail="시장 데이터 업데이트에 실패했습니다.")
        
    return {"message": "시장 데이터가 성공적으로 업데이트되었습니다."}