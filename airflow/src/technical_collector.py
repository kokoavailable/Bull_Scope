import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime
from helper.common import logger, DB_PARAMS, YF_SESSION
import concurrent.futures
import talib
from src.base_collector import BaseCollector

class TechnicalCollector(BaseCollector):
    connection_pool = None

    def __init__(self):
        self._ensure_table()

    def _ensure_table(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_technicals (
                    id BIGSERIAL GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                    stock_id BIGINT NOT NULL REFERENCES stocks(id),
                    date DATE NOT NULL,
                    rsi_14 FLOAT,
                    macd FLOAT,
                    macd_signal FLOAT,
                    macd_histogram FLOAT,
                    ma_20 FLOAT,
                    ma_50 FLOAT,
                    ma_200 FLOAT,
                    bolinger_upper FLOAT,
                    bolinger_middle FLOAT,
                    bolinger_lower FLOAT,
                    ppo FLOAT,
                    golden_cross BOOLEAN DEFAULT FALSE,
                    UNIQUE (stock_id, date)
                );
            """)
            conn.commit()
        finally:
            cur.close()
            self._put_conn(conn)

    def _get_price_data(self, symbol: str):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT s.id, p.date, p.close, p.volume
                FROM stock_price p
                JOIN stocks s ON s.symbol = p.symbol
                WHERE s.symbol = %s
                ORDER BY p.date;
            """, (symbol,))
            rows = cur.fetchall()
            if not rows:
                return None, None
            df = pd.DataFrame(rows, columns=["stock_id", "date", "close", "volume"])
            return df["stock_id"].iloc[0], df
        finally:
            cur.close()
            self._put_conn(conn)

    def _compute_technicals(self, df: pd.DataFrame):
        close = df["close"].values
        index = df["date"]

        rsi_14 = talib.RSI(close, timeperiod=14)
        macd, macd_signal, macd_hist = talib.MACD(close)
        ma_20 = talib.SMA(close, timeperiod=20)
        ma_50 = talib.SMA(close, timeperiod=50)
        ma_200 = talib.SMA(close, timeperiod=200)
        upper, middle, lower = talib.BBANDS(close)
        ppo = talib.PPO(close)

        golden_cross = (ma_50 > ma_200) & (ma_50.shift(1) <= ma_200.shift(1))

        return pd.DataFrame({
            "date": index,
            "rsi_14": rsi_14,
            "macd": macd,
            "macd_signal": macd_signal,
            "macd_histogram": macd_hist,
            "ma_20": ma_20,
            "ma_50": ma_50,
            "ma_200": ma_200,
            "bolinger_upper": upper,
            "bolinger_middle": middle,
            "bolinger_lower": lower,
            "ppo": ppo,
            "golden_cross": golden_cross.fillna(False)
        }).dropna(subset=["macd"])

    def save_technicals_bulk(self, df: pd.DataFrame, stock_id: int):
        if df.empty:
            return False
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            df["stock_id"] = stock_id
            df["date"] = pd.to_datetime(df["date"]).dt.date

            records = df[[
                "stock_id", "date", "rsi_14", "macd", "macd_signal", "macd_histogram",
                "ma_20", "ma_50", "ma_200",
                "bolinger_upper", "bolinger_middle", "bolinger_lower",
                "ppo", "golden_cross"
            ]].to_records(index=False)

            sql = """
                INSERT INTO stock_technicals (
                    stock_id, date, rsi_14, macd, macd_signal, macd_histogram,
                    ma_20, ma_50, ma_200,
                    bolinger_upper, bolinger_middle, bolinger_lower,
                    ppo, golden_cross
                )
                VALUES %s
                ON CONFLICT (stock_id, date) DO UPDATE SET
                    rsi_14 = EXCLUDED.rsi_14,
                    macd = EXCLUDED.macd,
                    macd_signal = EXCLUDED.macd_signal,
                    macd_histogram = EXCLUDED.macd_histogram,
                    ma_20 = EXCLUDED.ma_20,
                    ma_50 = EXCLUDED.ma_50,
                    ma_200 = EXCLUDED.ma_200,
                    bolinger_upper = EXCLUDED.bolinger_upper,
                    bolinger_middle = EXCLUDED.bolinger_middle,
                    bolinger_lower = EXCLUDED.bolinger_lower,
                    ppo = EXCLUDED.ppo,
                    golden_cross = EXCLUDED.golden_cross;
            """
            execute_values(cur, sql, records)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"기술적 지표 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    def process_symbol(self, symbol: str):
        try:
            stock_id, df = self._get_price_data(symbol)
            if df is None:
                logger.warning(f"{symbol}: 가격 데이터 없음")
                return symbol, False
            tech_df = self._compute_technicals(df)
            success = self.save_technicals_bulk(tech_df, stock_id)
            return symbol, success
        except Exception as e:
            logger.error(f"{symbol} 처리 실패: {e}")
            return symbol, False

    def update_all_technicals_parallel(self, symbols: list[str], max_workers=8):
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.process_symbol, symbol): symbol
                for symbol in symbols
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol, success = future.result()
                results[symbol] = success
        return results

    def close(self):
        if TechnicalCollector.connection_pool:
            TechnicalCollector.connection_pool.closeall()


if __name__ == "__main__":
    # 예시: DB에 저장된 심볼 목록 가져와서 처리
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM stocks LIMIT 100;")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    collector = TechnicalCollector()
    result = collector.update_all_technicals_parallel(symbols, max_workers=12)
    for symbol, success in result.items():
        logger.info(f"{symbol} 기술적 지표 저장 {'성공' if success else '실패'}")
    collector.close()