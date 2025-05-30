import requests
from bs4 import BeautifulSoup
import yfinance as yf
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, date, timezone
from helper.common import logger, driver
from src.base_collector import BaseCollector
from typing import Optional
from requests_html import HTMLSession
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By   
from selenium.webdriver.support import expected_conditions as EC

class MarketCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.vix = self.fetch_vix()
        self.fear = self.fetch_fear_greed()
        self.indicator_values = {
            "vix": self.vix,
            "fear_greed_index": self.fear,
        }
        self._load_indicator_type_ids()

    def _load_indicator_type_ids(self):
        """indicator_types에서 코드→ID 매핑을 불러와 self.type_ids에 저장."""
        codes = list(self.indicator_values.keys())

        conn = self._get_conn()
        cur = conn.cursor()

        placeholders = ",".join(["%s"] * len(codes))
        try:
            sql = f"""
                    SELECT code, id
                    FROM indicator_types
                    WHERE code IN ({placeholders});
                """

            cur.execute(sql, codes)
            self.type_ids = {code: idx for code, idx in cur.fetchall()}
        finally:
            cur.close()
            self._put_conn(conn)

    def fetch_vix(self) -> float | None:
        try:
            vix_data = yf.Ticker("^VIX").history(period="1d")
            if not vix_data.empty:
                return float(vix_data["Close"].iloc[-1])
        except Exception as e:
            logger.error(f"VIX 지수 가져오기 실패: {e}")
        return None
    
    @staticmethod
    def fetch_fear_greed() -> int | None:
        # 1) Chrome 옵션 세팅 (headless)
        # 로컬에 설치된 크롬 사용: (필요시 경로 지정)
        # opts.binary_location = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"


        try:
            # 3) 페이지 열고, 숫자 엘리먼트가 보일 때까지 최대 10초 대기
            driver.get("https://edition.cnn.com/markets/fear-and-greed")
            span = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR,
                  "span.market-fng-gauge__dial-number-value"))
            )
            text = span.text.strip()
            return int(text) if text.isdigit() else None

        finally:
            driver.quit()

    def save_market_indicator(self, vix: float, fear: int) -> bool:
        today = date.today()
        payload = [
            ("vix",                 vix),
            ("fear_greed_index",    fear),
        ]
        
        conn = self._get_conn()
        cur = conn.cursor()
    
        try:
            for code, val in payload:
                type_id = self.type_ids.get(code)
                val_str = None if val is None else str(val)
                last_updated = datetime.now(timezone.utc)
                cur.execute("""
                    INSERT INTO market_indicators (date, indicator_type_id, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (date, indicator_type_id) DO UPDATE
                        SET value = EXCLUDED.value;
                """, (today, type_id, val_str, last_updated))
            conn.commit()
            logger.info(f"시장 지표 저장 완료 - VIX: {vix}, FearGreed: {fear}")
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"시장 지표 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    def run(self):
        return self.save_market_indicator(self.vix, self.fear)



if __name__ == "__main__":
    collector = MarketCollector()
    collector.run()
    collector.close_pool()
