import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timedelta
from helper.common import logger, fetch_stock_symbols, YF_SESSION
import time
from typing import Dict, List
from src.base_collector import BaseCollector

class PriceCollector(BaseCollector):
    def __init__(self):
        super().__init__()

    def fetch_stock_symbols(self, market: str = "US"):
        if market.upper() == "US":
            return fetch_stock_symbols()
        raise NotImplementedError("Only US supported for now")

    # ───────── 멀티-티커 가격 수집
    @staticmethod
    def fetch_prices_batch(symbols: List[str],
                           period="1y",
                           interval="1d") -> Dict[str, pd.DataFrame]:
        joined = " ".join(symbols)
        df = yf.download(
            tickers     = joined,
            period      = period,
            interval    = interval,
            group_by    = "ticker",
            auto_adjust = False,
            threads     = True,
            progress    = False,
            session     = YF_SESSION
        )

        out: Dict[str, pd.DataFrame] = {}
        for sym in symbols:
            # (sym, 'Open') 같은 멀티인덱스 존재 여부로 데이터 판단
            if (sym, "Open") not in df.columns:   # 데이터 없음
                continue
            sub = df[sym].dropna(how="all")
            if sub.empty:
                continue
            sub.reset_index(inplace=True)         # Date → 컬럼
            out[sym] = sub
        return out


    def _save_bulk(self, df: pd.DataFrame, symbol: str):
        if df.empty:
            return False
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            # 1) symbol → stock_id
            cur.execute("SELECT id FROM stocks WHERE symbol = %s;", (symbol,))
            row = cur.fetchone()
            if not row:
                logger.warning(f"{symbol} stock_id 없음")
                return False
            stock_id = row[0]

            df["stock_id"] = stock_id
            df["date"] = pd.to_datetime(df["Date"]).dt.date
            df["adj_close"] = df.get("Adj Close", df["Close"])
            
            records = df[[
                "stock_id", "date", "Open", "High", "Low", 
                "Close", "adj_close", "Volume"
            ]].to_records(index=False)

            # UPSERT INTO with ON CONFLICT
            sql = """
            INSERT INTO stock_prices (
                stock_id, date, open, high, low, close, adj_close, volume
            ) VALUES %s
            ON CONFLICT (stock_id, date) DO UPDATE SET
                open        = EXCLUDED.open,
                high        = EXCLUDED.high,
                low         = EXCLUDED.low,
                close       = EXCLUDED.close,
                adj_close   = EXCLUDED.adj_close,
                volume      = EXCLUDED.volume;
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

    # ───────── 전체 업데이트 (배치 루프)
    def update_all(self,
                   period="1y",
                   interval="1d",
                   batch_size=100,
                   pause=1.0):
        symbols = self.fetch_stock_symbols()
        results = {}

        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            price_map = self.fetch_prices_batch(batch, period, interval)
            # 정상 수집 종목
            for sym, df in price_map.items():
                results[sym] = self._save_bulk(df, sym)

            # 수집 실패 종목
            fail_syms = set(batch) - set(price_map.keys())
            for sym in fail_syms:
                logger.warning(f"{sym} 데이터 없음 (야후 응답 누락)")
                results[sym] = False

            time.sleep(pause)   # polite pause between batches

        return results


    def close(self):
        super().close()

# if __name__ == "__main__":
#     collector = PriceCollector()
#     result = collector.update_all_stocks_parallel(market="US", period="1y", delay=0.2)
#     # 결과 출력
#     for symbol, success in result.items():
#         if success:
#             logger.info(f"{symbol} 데이터 업데이트 성공")
#         else:
#             logger.error(f"{symbol} 데이터 업데이트 실패")

#     collector.close()
