import pandas as pd
import yfinance as yf
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime
from helper.common import logger, DB_PARAMS, YF_SESSION
import concurrent.futures
from typing import List, Dict
import requests
from src.base_collector import BaseCollector

class StockCollector(BaseCollector):
    def __init__(self):
        super().__init__()

    def fetch_stock_symbols(self):
        url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
        df = pd.read_csv(url, sep="|")
        df = df[df['Test Issue'] != 'Y']
        df = df.dropna(subset=['Symbol', 'Security Name'])
        df = df.rename(columns={
            'Symbol': 'symbol',
            'Security Name': 'company_name'
        })
        return df[['symbol', 'company_name']]

    def enrich_metadata(self, row):
        symbol = row['symbol']
        company_name = row['company_name']
        try:
            info = yf.Ticker(symbol, session=YF_SESSION).info
            return {
                'symbol': symbol,
                'company_name': company_name,
                'sector': info.get('sector'),
                'industry': info.get('industry'),
                'country': info.get('country'),
                'last_updated': datetime.utcnow()
            }
        except Exception as e:
            logger.warning(f"{symbol} info 수집 실패: {e}")
            return None

    def fetch_and_enrich_all(self, df: pd.DataFrame, max_workers=8):
        records = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.enrich_metadata, row) for row in df.itertuples(index=False)]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    records.append(result)
        return records

    def save_stocks_bulk(self, enriched_data: list[dict]):
        if not enriched_data:
            return False
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            records = [
                (
                    d['symbol'],
                    d['company_name'],
                    d.get('sector'),
                    d.get('industry'),
                    d.get('country'),
                    d['last_updated']
                )
                for d in enriched_data
            ]

            sql = """
                INSERT INTO stocks (symbol, company_name, sector, industry, country, last_updated)
                VALUES %s
                ON CONFLICT (symbol) DO UPDATE SET
                    company_name = EXCLUDED.company_name,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    country = EXCLUDED.country,
                    last_updated = EXCLUDED.last_updated;
            """
            execute_values(cur, sql, records)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"종목 저장 실패: {str(e)}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    def update_stock_list(self, max_symbols=300, max_workers=8):
        try:
            df = self.fetch_stock_symbols()
            if max_symbols:
                df = df.head(max_symbols)  # 일단 300개 제한 (API 과부하 방지)
            enriched = self.fetch_and_enrich_all(df, max_workers=max_workers)
            success = self.save_stocks_bulk(enriched)
            if success:
                logger.info(f"{len(enriched)}개 종목 저장 완료")
            return success
        except Exception as e:
            logger.error(f"업데이트 실패: {e}")
            return False

    def close(self):
        super().close()


if __name__ == "__main__":
    collector = StockCollector()
    collector.update_stock_list(max_symbols=300, max_workers=12)
    collector.close()
