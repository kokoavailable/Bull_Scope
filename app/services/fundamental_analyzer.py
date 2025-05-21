import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta

from app.models.database import Stock, StockFundamental

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class FundamentalAnalyzer:
    def __init__(self, db: Session):
        self.db = db
        
    def analyze_stock(self, symbol: str) -> Dict[str, Any]:
        """
        종목의 재무 분석 수행
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            재무 분석 결과 딕셔너리
        """
        try:
            # 주식 정보 조회
            stock = self.db.query(Stock).filter(Stock.symbol == symbol).first()
            if not stock:
                logger.error(f"{symbol} 종목 정보를 찾을 수 없습니다.")
                return {}
                
            # 재무 데이터 조회 (최근 5년)
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=5*365)
            
            fundamental_data = self.db.query(StockFundamental).filter(
                StockFundamental.stock_id == stock.id,
                StockFundamental.date >= start_date,
                StockFundamental.date <= end_date
            ).order_by(StockFundamental.date.asc()).all()
            
            if not fundamental_data:
                logger.error(f"{symbol} 재무 데이터를 찾을 수 없습니다.")
                return {}
                
            # DataFrame 생성
            df = pd.DataFrame([{
                'date': f.date,
                'market_cap': f.market_cap,
                'pe_ratio': f.pe_ratio,
                'pb_ratio': f.pb_ratio,
                'debt_to_equity': f.debt_to_equity,
                'current_ratio': f.current_ratio,
                'quick_ratio': f.quick_ratio,
                'roe': f.roe,
                'roa': f.roa,
                'eps': f.eps,
                'revenue': f.revenue,
                'net_income': f.net_income
            } for f in fundamental_data])
            
            # 최근 데이터
            latest = df.iloc[-1] if not df.empty else None
            
            # 업계 평균과 비교 (TODO: 실제 업계 평균 데이터 필요)
            # 예시로 하드코딩된 값 사용
            industry_avg = {
                'pe_ratio': 20.5,
                'pb_ratio': 3.2,
                'debt_to_equity': 1.5,
                'current_ratio': 1.8,
                'roe': 0.15,
                'roa': 0.08
            }
            
            # 업계 평균 대비 비율 계산
            industry_comparison = {}
            if latest is not None:
                for key in industry_avg:
                    if key in latest and latest[key] is not None and latest[key] != 0:
                        industry_comparison[key] = latest[key] / industry_avg[key]
            
            # 성장성 분석 (최근 3년간 복합 연간 성장률)
            growth_metrics = {}
            if len(df) >= 4:  # 최소 3년 데이터가 필요
                years = 3
                # EPS 성장률
                if 'eps' in df.columns and df.iloc[-1]['eps'] is not None and df.iloc[-years-1]['eps'] is not None and df.iloc[-years-1]['eps'] != 0:
                    eps_cagr = (df.iloc[-1]['eps'] / df.iloc[-years-1]['eps']) ** (1/years) - 1
                    growth_metrics['eps_cagr'] = eps_cagr
                
                # 매출 성장률
                if 'revenue' in df.columns and df.iloc[-1]['revenue'] is not None and df.iloc[-years-1]['revenue'] is not None and df.iloc[-years-1]['revenue'] != 0:
                    revenue_cagr = (df.iloc[-1]['revenue'] / df.iloc[-years-1]['revenue']) ** (1/years) - 1
                    growth_metrics['revenue_cagr'] = revenue_cagr
                
                # 순이익 성장률
                if 'net_income' in df.columns and df.iloc[-1]['net_income'] is not None and df.iloc[-years-1]['net_income'] is not None and df.iloc[-years-1]['net_income'] != 0:
                    net_income_cagr = (df.iloc[-1]['net_income'] / df.iloc[-years-1]['net_income']) ** (1/years) - 1
                    growth_metrics['net_income_cagr'] = net_income_cagr
            
            # 재무 건전성 평가
            financial_health = {}
            if latest is not None:
                # 부채비율 평가
                if 'debt_to_equity' in latest and latest['debt_to_equity'] is not None:
                    if latest['debt_to_equity'] < 0.5:
                        financial_health['debt_to_equity_rating'] = 'Excellent'
                    elif latest['debt_to_equity'] < 1.0:
                        financial_health['debt_to_equity_rating'] = 'Good'
                    elif latest['debt_to_equity'] < 2.0:
                        financial_health['debt_to_equity_rating'] = 'Fair'
                    else:
                        financial_health['debt_to_equity_rating'] = 'Poor'
                
                # 유동비율 평가
                if 'current_ratio' in latest and latest['current_ratio'] is not None:
                    if latest['current_ratio'] > 2.0:
                        financial_health['current_ratio_rating'] = 'Excellent'
                    elif latest['current_ratio'] > 1.5:
                        financial_health['current_ratio_rating'] = 'Good'
                    elif latest['current_ratio'] > 1.0:
                        financial_health['current_ratio_rating'] = 'Fair'
                    else:
                        financial_health['current_ratio_rating'] = 'Poor'
                
                # ROE 평가
                if 'roe' in latest and latest['roe'] is not None:
                    if latest['roe'] > 0.20:
                        financial_health['roe_rating'] = 'Excellent'
                    elif latest['roe'] > 0.15:
                        financial_health['roe_rating'] = 'Good'
                    elif latest['roe'] > 0.10:
                        financial_health['roe_rating'] = 'Fair'
                    else:
                        financial_health['roe_rating'] = 'Poor'
                        
                # 종합 재무 건전성 점수 (0-100)
                health_score = 0
                health_count = 0
                
                if 'debt_to_equity_rating' in financial_health:
                    if financial_health['debt_to_equity_rating'] == 'Excellent':
                        health_score += 100
                    elif financial_health['debt_to_equity_rating'] == 'Good':
                        health_score += 75
                    elif financial_health['debt_to_equity_rating'] == 'Fair':
                        health_score += 50
                    else:
                        health_score += 25
                    health_count += 1
                    
                if 'current_ratio_rating' in financial_health:
                    if financial_health['current_ratio_rating'] == 'Excellent':
                        health_score += 100
                    elif financial_health['current_ratio_rating'] == 'Good':
                        health_score += 75
                    elif financial_health['current_ratio_rating'] == 'Fair':
                        health_score += 50
                    else:
                        health_score += 25
                    health_count += 1
                    
                if 'roe_rating' in financial_health:
                    if financial_health['roe_rating'] == 'Excellent':
                        health_score += 100
                    elif financial_health['roe_rating'] == 'Good':
                        health_score += 75
                    elif financial_health['roe_rating'] == 'Fair':
                        health_score += 50
                    else:
                        health_score += 25
                    health_count += 1
                    
                if health_count > 0:
                    financial_health['overall_score'] = health_score / health_count
                else:
                    financial_health['overall_score'] = None
            
            # 밸류에이션 평가
            valuation = {}
            if latest is not None:
                # P/E 비율 평가
                if 'pe_ratio' in latest and latest['pe_ratio'] is not None:
                    if latest['pe_ratio'] < industry_avg.get('pe_ratio', 20) * 0.7:
                        valuation['pe_rating'] = 'Undervalued'
                    elif latest['pe_ratio'] < industry_avg.get('pe_ratio', 20) * 1.2:
                        valuation['pe_rating'] = 'Fair'
                    else:
                        valuation['pe_rating'] = 'Overvalued'
                
                # P/B 비율 평가
                if 'pb_ratio' in latest and latest['pb_ratio'] is not None:
                    if latest['pb_ratio'] < industry_avg.get('pb_ratio', 3) * 0.7:
                        valuation['pb_rating'] = 'Undervalued'
                    elif latest['pb_ratio'] < industry_avg.get('pb_ratio', 3) * 1.2:
                        valuation['pb_rating'] = 'Fair'
                    else:
                        valuation['pb_rating'] = 'Overvalued'
                        
                # 종합 밸류에이션 상태
                valuation_states = []
                if 'pe_rating' in valuation:
                    valuation_states.append(valuation['pe_rating'])
                if 'pb_rating' in valuation:
                    valuation_states.append(valuation['pb_rating'])
                    
                if valuation_states:
                    # 가장 많이 나타난 상태를 종합 상태로 설정
                    from collections import Counter
                    valuation['overall_rating'] = Counter(valuation_states).most_common(1)[0][0]
                else:
                    valuation['overall_rating'] = None
            
            # 분석 결과 반환
            result = {
                "symbol": symbol,
                "company_name": stock.company_name,
                "sector": stock.sector,
                "industry": stock.industry,
                "latest_data": latest.to_dict() if latest is not None else None,
                "industry_comparison": industry_comparison,
                "growth_metrics": growth_metrics,
                "financial_health": financial_health,
                "valuation": valuation,
                "analysis_date": datetime.now().date().isoformat()
            }
            
            return result
            
        except Exception as e:
            logger.error(f"{symbol} 재무 분석 실패: {str(e)}")
            return {}
            
    def filter_by_criteria(self, criteria: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        설정된 재무 기준에 따라 종목 필터링
        
        Args:
            criteria: 필터링 기준 (기본값: D/E < 3.0, 유동비율 > 0.5)
            
        Returns:
            필터링된 종목 목록
        """
        if criteria is None:
            criteria = {
                "debt_to_equity_max": 3.0,   # D/E 3 이하
                "current_ratio_min": 0.5     # 유동비율 0.5 이상
            }
            
        try:
            # 모든 주식 가져오기
            stocks = self.db.query(Stock).all()
            filtered_stocks = []
            
            for stock in stocks:
                # 각 종목에 대한 최신 재무 지표 조회
                latest_fundamental = self.db.query(StockFundamental).filter(
                    StockFundamental.stock_id == stock.id
                ).order_by(StockFundamental.date.desc()).first()
                
                if not latest_fundamental:
                    continue
                    
                # 기준 충족 여부 확인
                meets_criteria = True
                
                # D/E 비율 기준 확인
                if criteria.get("debt_to_equity_max") is not None and latest_fundamental.debt_to_equity is not None:
                    if latest_fundamental.debt_to_equity > criteria["debt_to_equity_max"]:
                        meets_criteria = False
                    
                # 유동비율 기준 확인
                if criteria.get("current_ratio_min") is not None and latest_fundamental.current_ratio is not None:
                    if latest_fundamental.current_ratio < criteria["current_ratio_min"]:
                        meets_criteria = False
                        
                # ROE 기준 확인
                if criteria.get("roe_min") is not None and latest_fundamental.roe is not None:
                    if latest_fundamental.roe < criteria["roe_min"]:
                        meets_criteria = False
                        
                # P/E 기준 확인
                if criteria.get("pe_ratio_max") is not None and latest_fundamental.pe_ratio is not None:
                    if latest_fundamental.pe_ratio > criteria["pe_ratio_max"]:
                        meets_criteria = False
                        
                # P/B 기준 확인
                if criteria.get("pb_ratio_max") is not None and latest_fundamental.pb_ratio is not None:
                    if latest_fundamental.pb_ratio > criteria["pb_ratio_max"]:
                        meets_criteria = False
                
                # 기준 충족 시 필터링된 목록에 추가
                if meets_criteria:
                    filtered_stocks.append({
                        "symbol": stock.symbol,
                        "company_name": stock.company_name,
                        "sector": stock.sector,
                        "industry": stock.industry,
                        "date": latest_fundamental.date.isoformat(),
                        "debt_to_equity": latest_fundamental.debt_to_equity,
                        "current_ratio": latest_fundamental.current_ratio,
                        "roe": latest_fundamental.roe,
                        "pe_ratio": latest_fundamental.pe_ratio,
                        "pb_ratio": latest_fundamental.pb_ratio,
                        "market_cap": latest_fundamental.market_cap
                    })
                    
            # 시가총액별로 정렬 (큰 순)
            filtered_stocks.sort(key=lambda x: x.get("market_cap", 0) if x.get("market_cap") is not None else 0, reverse=True)
            
            return filtered_stocks
            
        except Exception as e:
            logger.error(f"재무 필터링 실패: {str(e)}")
            return []