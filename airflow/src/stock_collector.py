import pandas as pd
import yfinance as yf
from psycopg2.extras import execute_values
from datetime import datetime
from helper.common import logger
import concurrent.futures
from src.base_collector import BaseCollector
import time
from datetime import datetime, timezone

class StockCollector(BaseCollector):
    def __init__(self):
        super().__init__()

    def fetch_stock_symbols(self):
        url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
        df = pd.read_csv(url, sep="|")
        df = df[df['Test Issue'] != 'Y']
        df = df.dropna(subset=['Symbol', 'Security Name'])
        df = df[df["Security Name"].str.contains("Common Stock", na=False)]
        df = df[~df["Symbol"].str.contains(r"[.$]", regex=True)]
        df = df.rename(columns={
            'Symbol': 'symbol',
            'Security Name': 'company_name'
        })

        return df[['symbol', 'company_name']]

    def enrich_metadata(self, row, delay):
        symbol = row.symbol
        company_name = row.company_name
        try:
            info = yf.Ticker(symbol).info
            time.sleep(delay)
            return {
                'symbol': symbol,
                'company_name': company_name,
                'sector': info.get('sector'),
                'industry': info.get('industry'),
                'country': info.get('country'),
                'last_updated': datetime.now(timezone.utc)
            }
        except Exception as e:
            logger.warning(f"{symbol} info 수집 실패: {e}")
            return None

    def fetch_and_enrich_all(self, df: pd.DataFrame, max_workers=8, delay = 0.3):
        records = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.enrich_metadata, row, delay) for row in df.itertuples(index=False)]
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

            logger.info(f"enriched 샘플: {records[:3]}")
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
            logger.info(f"execute_values 영향받은 row: {cur.rowcount}")
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"종목 저장 실패: {str(e)}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    def update_stock_list(self, batch_size=100, max_workers=8, delay=0.05):
        try:
            logger.info(f"Stock 리스트 업데이트 시작: 배치당 {batch_size}개, 워커 {max_workers}개")
            df = self.fetch_stock_symbols()
            logger.info(f"총 {len(df)}개 심볼 수집됨")
            total = len(df)
            results = []
            for start in range(0, total, batch_size):
                batch_num = start // batch_size + 1
                batch_df = df.iloc[start:start+batch_size]
                logger.info(f"[Batch {batch_num}] {len(batch_df)}개 종목 enrich 시도")
                enriched = self.fetch_and_enrich_all(batch_df, max_workers=max_workers, delay=delay)
                logger.info(f"[Batch {batch_num}] enrich 완료: {len(enriched)}개, 저장 시도")
                success = self.save_stocks_bulk(enriched)
                if success:
                    logger.info(f"[Batch {batch_num}] {len(enriched)}개 종목 DB 저장 성공")
                    results.append(True)
                else:
                    logger.warning(f"[Batch {batch_num}] {len(enriched)}개 종목 DB 저장 실패")
                    results.append(False)
            all_success = all(results)
            if all_success:
                logger.info("모든 배치 저장 성공 (update_stock_list 완료)")
            else:
                logger.error("일부 배치 저장 실패 (update_stock_list 실패)")
            return all_success
        except Exception as e:
            logger.exception(f"업데이트 중 예외 발생: {e}")
            return False



if __name__ == "__main__":
    collector = StockCollector()
    collector.update_stock_list(max_workers=4, delay=0.05)
    collector.close_pool()
