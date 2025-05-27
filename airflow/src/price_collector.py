import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timedelta
from helper.common import logger, DB_PARAMS
import concurrent.futures
import time

class PriceCollector:

    # 클래스 레벨 풀

    connection_pool = None

    def __init__(self, minconn=4, maxconn=16):
        if PriceCollector.connection_pool is None:
            PriceCollector.connection_pool = ThreadedConnectionPool(
                minconn, maxconn, **DB_PARAMS
            )
        self._ensure_tables()

    def _get_conn(self):
        # 풀에서 커넥션 할당
        return PriceCollector.connection_pool.getconn()
    
    def _put_conn(self, conn):
        # 커넥션 반환
        PriceCollector.connection_pool.putconn(conn)

    def _ensure_tables(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_price (
                id BIGSERIAL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                st
                symbol VARCHAR(16) NOT NULL,
                date DATE NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                adj_close NUMERIC,
                volume BIGINT,
                UNIQUE (symbol, date)
            );
            """)
            conn.commit()
            
        finally:
            cur.close()
            self._put_conn(conn)

    def fetch_stock_symbols(self, market: str = "US"):
        if market.upper() == "US":
            url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
            df = pd.read_csv(url, sep="|")
            df = df.dropna(subset=['Symbol'])
            return df['Symbol'].tolist()
        else:
            raise NotImplementedError("Only US supported for now")

    def fetch_stock_data(self, symbol: str, period: str = "1y"):
        try:
            df = yf.Ticker(symbol).history(period=period)
            if df.empty:
                logger.warning(f"{symbol}에 대한 데이터가 없습니다.")
                return pd.DataFrame()
            df.reset_index(inplace=True)
            return df
        except Exception as e:
            logger.error(f"{symbol} 데이터 가져오기 실패: {str(e)}")
            return pd.DataFrame()

    def save_price_data_bulk(self, df: pd.DataFrame, symbol: str):
        if df.empty:
            return False
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            df["symbol"] = symbol
            df["date"] = pd.to_datetime(df["Date"]).dt.date
            df["adj_close"] = df.get("Adj Close", df["Close"])
            records = df[["symbol", "date", "Open", "High", "Low", "Close", "adj_close", "Volume"]].to_records(index=False)

            # UPSERT INTO with ON CONFLICT
            sql = """
                INSERT INTO stock_price (symbol, date, open, high, low, close, adj_close, volume)
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    open=EXCLUDED.open,
                    high=EXCLUDED.high,
                    low=EXCLUDED.low,
                    close=EXCLUDED.close,
                    adj_close=EXCLUDED.adj_close,
                    volume=EXCLUDED.volume;
            """
            execute_values(cur, sql, records)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"{symbol} 데이터 저장 실패: {str(e)}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    def fetch_and_save(self, symbol: str, period: str = "1y", delay: float = 0.2):
        try:
            df = self.fetch_stock_data(symbol, period)
            success = self.save_price_data_bulk(df, symbol)
            time.sleep(delay)
            return symbol, success
        except Exception as e:
            logger.error(f"{symbol} 처리 중 에러: {e}")
            return symbol, False

    def update_all_stocks_parallel(self, market="US", period="1y", delay=0.2, max_workers=8):
        symbols = self.fetch_stock_symbols(market)
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.fetch_and_save, symbol, period, delay): symbol
                for symbol in symbols
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol, success = future.result()
                results[symbol] = success
        return results

    def close(self):
        if PriceCollector.connection_pool:
            PriceCollector.connection_pool.closeall()

if __name__ == "__main__":
    collector = PriceCollector()
    result = collector.update_all_stocks_parallel(market="US", period="1y", delay=0.2)
    # 결과 출력
    for symbol, success in result.items():
        if success:
            logger.info(f"{symbol} 데이터 업데이트 성공")
        else:
            logger.error(f"{symbol} 데이터 업데이트 실패")

    collector.close()
