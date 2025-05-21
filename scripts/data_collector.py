import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import logging
from typing import List, Dict, Any, Tuple, Optional
import time

from app.models.database import Stock, StockPrice, StockFundamental

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataCollector:
    def __init__(self, db: Session):
        self.db = db
        
    def fetch_stock_symbols(self, market: str = "KRX") -> List[str]:
        """
        특정 시장(예: KRX, KOSPI, KOSDAQ)의 모든 종목 심볼을 가져옵니다.
        """
        try:
            if market.upper() == "NASDAQ":
                url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
                df = pd.read_csv(url, sep="|")
                df = df.dropna(subset=['Symbol'])

                return df['Symbol'].tolist()

            if market.upper() == "KRX":
                # 한국 시장의 경우 KOSPI + KOSDAQ
                kospi_symbols = self._fetch_symbols_by_market("KOSPI")
                kosdaq_symbols = self._fetch_symbols_by_market("KOSDAQ")
                return kospi_symbols + kosdaq_symbols
            else:
                return self._fetch_symbols_by_market(market)
        except Exception as e:
            logger.error(f"시장 종목 가져오기 실패: {str(e)}")
            return []
            
    def _fetch_symbols_by_market(self, market: str) -> List[str]:
        """
        특정 시장의 종목 심볼을 가져옵니다.
        """
        if market.upper() == "KOSPI":
            # KOSPI 종목 심볼 가져오기 (pandas-datareader 또는 웹 스크래핑 필요)
            # 예시로 일부 대표 종목만 반환
            return ["005930.KS", "000660.KS", "035420.KS", "005380.KS"]
        elif market.upper() == "KOSDAQ":
            # KOSDAQ 종목 심볼 가져오기
            return ["066570.KQ", "035720.KQ", "068270.KQ"]
        else:
            # 다른 시장은 추가 구현 필요
            return []
    
    def fetch_stock_data(self, symbol: str, period: str = "1y") -> pd.DataFrame:
        """
        yfinance를 사용하여 주식 데이터를 가져옵니다.
        
        Args:
            symbol: 주식 심볼 (예: "005930.KS")
            period: 기간 (예: "1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max")
            
        Returns:
            주식 가격 데이터가 포함된 DataFrame
        """
        try:
            stock = yf.Ticker(symbol)
            # 가격 데이터 가져오기
            df = stock.history(period=period)
            
            if df.empty:
                logger.warning(f"{symbol}에 대한 데이터가 없습니다.")
                return pd.DataFrame()
                
            # 인덱스 재설정
            df.reset_index(inplace=True)
            
            return df
        except Exception as e:
            logger.error(f"{symbol} 데이터 가져오기 실패: {str(e)}")
            return pd.DataFrame()
            
    def fetch_stock_info(self, symbol: str) -> Dict[str, Any]:
        """
        종목 기본 정보를 가져옵니다.
        """
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            
            # 필요한 정보만 추출
            result = {
                "symbol": symbol,
                "company_name": info.get("longName", info.get("shortName", symbol)),
                "sector": info.get("sector"),
                "industry": info.get("industry"),
                "country": info.get("country")
            }
            
            return result
        except Exception as e:
            logger.error(f"{symbol} 정보 가져오기 실패: {str(e)}")
            return {"symbol": symbol, "company_name": symbol}
            
    def fetch_financial_data(self, symbol: str) -> Dict[str, Any]:
        """
        종목의 재무 데이터를 가져옵니다.
        """
        try:
            stock = yf.Ticker(symbol)
            
            # 재무제표 가져오기
            balance_sheet = stock.balance_sheet
            income_stmt = stock.income_stmt
            cash_flow = stock.cashflow
            
            if balance_sheet.empty or income_stmt.empty:
                logger.warning(f"{symbol}에 대한 재무 데이터가 없습니다.")
                return {}
                
            # 가장 최근 데이터 (첫 번째 열)
            latest_bs = balance_sheet.iloc[:, 0]
            latest_is = income_stmt.iloc[:, 0]
            
            # 주요 재무 지표 계산
            total_assets = latest_bs.get('Total Assets', None)
            total_liabilities = latest_bs.get('Total Liabilities Net Minority Interest', None)
            total_equity = latest_bs.get('Total Equity Gross Minority Interest', None)
            current_assets = latest_bs.get('Current Assets', None)
            current_liabilities = latest_bs.get('Current Liabilities', None)
            inventory = latest_bs.get('Inventory', 0)
            
            net_income = latest_is.get('Net Income', None)
            revenue = latest_is.get('Total Revenue', None)
            
            # 비율 계산
            debt_to_equity = total_liabilities / total_equity if total_equity and total_liabilities else None
            current_ratio = current_assets / current_liabilities if current_assets and current_liabilities else None
            quick_ratio = (current_assets - inventory) / current_liabilities if current_assets and current_liabilities else None
            roe = net_income / total_equity if net_income and total_equity else None
            roa = net_income / total_assets if net_income and total_assets else None
            
            # 시가총액 및 주가 관련 정보
            info = stock.info
            market_cap = info.get('marketCap')
            pe_ratio = info.get('trailingPE')
            pb_ratio = info.get('priceToBook')
            eps = info.get('trailingEPS')
            
            # 결과 반환
            result = {
                "date": balance_sheet.columns[0].date(),
                "market_cap": market_cap,
                "pe_ratio": pe_ratio,
                "pb_ratio": pb_ratio,
                "debt_to_equity": debt_to_equity,
                "current_ratio": current_ratio,
                "quick_ratio": quick_ratio,
                "roe": roe,
                "roa": roa,
                "eps": eps,
                "revenue": revenue,
                "net_income": net_income
            }
            
            return result
        except Exception as e:
            logger.error(f"{symbol} 재무 데이터 가져오기 실패: {str(e)}")
            return {}
            
    def save_stock_data(self, symbol: str, period: str = "1y") -> bool:
        """
        종목 데이터를 가져와서 데이터베이스에 저장합니다.
        """
        try:
            # 종목 기본 정보 가져오기
            stock_info = self.fetch_stock_info(symbol)
            
            # DB에 종목 정보 저장/업데이트
            db_stock = self.db.query(Stock).filter(Stock.symbol == symbol).first()
            if not db_stock:
                db_stock = Stock(
                    symbol=stock_info["symbol"],
                    company_name=stock_info["company_name"],
                    sector=stock_info.get("sector"),
                    industry=stock_info.get("industry"),
                    country=stock_info.get("country")
                )
                self.db.add(db_stock)
                self.db.commit()
                self.db.refresh(db_stock)
            
            # 가격 데이터 가져오기
            price_data = self.fetch_stock_data(symbol, period)
            if price_data.empty:
                return False
                
            # 기존 데이터 삭제 (특정 기간에 대해서만)
            start_date = datetime.now() - timedelta(days=365 if period == "1y" else 30)
            self.db.query(StockPrice).filter(
                StockPrice.stock_id == db_stock.id,
                StockPrice.date >= start_date
            ).delete()
            
            # 새 가격 데이터 저장
            for _, row in price_data.iterrows():
                date = row["Date"].date() if isinstance(row["Date"], pd.Timestamp) else row["Date"]
                stock_price = StockPrice(
                    stock_id=db_stock.id,
                    date=date,
                    open=row.get("Open"),
                    high=row.get("High"),
                    low=row.get("Low"),
                    close=row.get("Close"),
                    adj_close=row.get("Adj Close", row.get("Close")),
                    volume=row.get("Volume")
                )
                self.db.add(stock_price)
            
            # 재무 데이터 가져오기
            financial_data = self.fetch_financial_data(symbol)
            if financial_data:
                # 기존 재무 데이터가 있는지 확인
                db_fundamental = self.db.query(StockFundamental).filter(
                    StockFundamental.stock_id == db_stock.id,
                    StockFundamental.date == financial_data["date"]
                ).first()
                
                if not db_fundamental:
                    db_fundamental = StockFundamental(
                        stock_id=db_stock.id,
                        date=financial_data["date"],
                        market_cap=financial_data.get("market_cap"),
                        pe_ratio=financial_data.get("pe_ratio"),
                        pb_ratio=financial_data.get("pb_ratio"),
                        debt_to_equity=financial_data.get("debt_to_equity"),
                        current_ratio=financial_data.get("current_ratio"),
                        quick_ratio=financial_data.get("quick_ratio"),
                        roe=financial_data.get("roe"),
                        roa=financial_data.get("roa"),
                        eps=financial_data.get("eps"),
                        revenue=financial_data.get("revenue"),
                        net_income=financial_data.get("net_income")
                    )
                    self.db.add(db_fundamental)
            
            self.db.commit()
            return True
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"{symbol} 데이터 저장 실패: {str(e)}")
            return False
    
    def update_all_stocks(self, market: str = "KRX", period: str = "1y", delay: int = 1) -> Dict[str, bool]:
        """
        특정 시장의 모든 종목 데이터를 업데이트합니다.
        """
        symbols = self.fetch_stock_symbols(market)
        results = {}
        
        for symbol in symbols:
            logger.info(f"{symbol} 데이터 업데이트 중...")
            success = self.save_stock_data(symbol, period)
            results[symbol] = success
            
            # API 호출 제한 회피를 위한 딜레이
            time.sleep(delay)
            
        return results