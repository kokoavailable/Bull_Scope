import requests
from bs4 import BeautifulSoup
import yfinance as yf
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, date
from helper.common import logger, YF_SESSION
from src.base_collector import BaseCollector

class MarketCollector(BaseCollector):
    connection_pool = None

    def __init__(self):
        super().__init__()

    def fetch_vix(self) -> float | None:
        try:
            vix_data = yf.Ticker("^VIX").history(period="1d")
            if not vix_data.empty:
                return float(vix_data["Close"].iloc[-1])
        except Exception as e:
            logger.error(f"VIX 지수 가져오기 실패: {e}")
        return None

    def fetch_fear_greed(self) -> int | None:
        try:
            url = "https://edition.cnn.com/markets/fear-and-greed"
            headers = {"User-Agent": "Mozilla/5.0"}
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            # 첫 번째 위치 (큰 숫자)
            index_element = soup.find("div", class_="FearGreedIndex__Dial-nn12s3-2")
            if index_element:
                return int(index_element.text.strip())

            # 두 번째 백업 위치
            fallback = soup.find("div", class_="FearGreedIndex__Value-nn12s3-3")
            if fallback:
                return int(fallback.text.strip())

            raise ValueError("Fear & Greed 지수 값을 찾을 수 없음")
        except Exception as e:
            logger.error(f"Fear & Greed Index 크롤링 실패: {e}")
            return None

    def infer_market_trend(self, vix: float, fear: int) -> str:
        if vix is None or fear is None:
            return "Unknown"
        if vix > 25 or fear < 30:
            return "Bearish"
        elif vix < 15 and fear > 60:
            return "Bullish"
        return "Neutral"

    def save_market_indicator(self, vix: float, fear: int, trend: str) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO market_indicators (date, vix, fear_greed_index, market_trend)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE SET
                    vix = EXCLUDED.vix,
                    fear_greed_index = EXCLUDED.fear_greed_index,
                    market_trend = EXCLUDED.market_trend;
            """, (date.today(), vix, fear, trend))
            conn.commit()
            logger.info(f"시장 지표 저장 완료 - VIX: {vix}, FearGreed: {fear}, Trend: {trend}")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"시장 지표 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    def run(self):
        vix = self.fetch_vix()
        fear = self.fetch_fear_greed()
        trend = self.infer_market_trend(vix, fear)
        return self.save_market_indicator(vix, fear, trend)
    
    def close(self):
        super().close()


if __name__ == "__main__":
    collector = MarketCollector()
    collector.run()
    collector.close()
