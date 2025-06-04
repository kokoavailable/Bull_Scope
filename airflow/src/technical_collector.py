import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from helper.common import logger, DB_PARAMS, fetch_stock_symbols
import talib
from src.base_collector import BaseCollector
import concurrent.futures
from typing import Dict, List

class TechnicalCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.indicator_id_map = self._load_indicator_id_map()

    def _load_indicator_id_map(self) -> Dict[str, int]:
        """technical_indicator_types에서 {code: id} dict 반환"""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT code, id FROM technical_indicator_types;")
            result = dict(cur.fetchall())
            return result
        finally:
            cur.close()
            self._put_conn(conn)

    def get_indicator_id(self, code: str) -> int:
        iid = self.indicator_id_map.get(code)
        if iid is None:
            raise ValueError(f"Indicator code '{code}' not found in technical_indicator_types.")
        return iid


    def fetch_stock_symbols(self, market: str = "US") -> List[str]:
        return fetch_stock_symbols()

    def _get_price_data(self, symbol: str):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT s.id, p.date, p.close, p.volume
                FROM stock_prices p
                JOIN stocks s ON s.id = p.stock_id
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

        upper, middle, lower = talib.BBANDS(close)

        rsi_14 = talib.RSI(close, timeperiod=14)
        macd, macd_signal, macd_hist = talib.MACD(close)
        macd = pd.Series(macd, index = df.index)
        macd_signal = pd.Series(macd_signal, index = df.index)

        ma_5   = df["close"].rolling(window=5).mean()
        ma_10  = df["close"].rolling(window=10).mean()
        ma_20 = middle

        ma_50  = df["close"].rolling(window=50).mean()
        ma_200 = df["close"].rolling(window=200).mean()
        ppo = talib.PPO(close)

        st_ma_gap = ma_5 - ma_10
        lt_ma_gap = ma_50 - ma_200
        macd_gap = macd - macd_signal

        st_ma_golden_cross = (ma_5 > ma_10) & (ma_5.shift(1) <= ma_10.shift(1))
        lt_ma_golden_cross = (ma_50 > ma_200) & (ma_50.shift(1) <= ma_200.shift(1))
        macd_golden_cross = (macd > macd_signal) & (macd.shift(1) <= macd_signal.shift(1))

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
            "st_ma_gap": st_ma_gap,
            "lt_ma_gap": lt_ma_gap,
            "macd_gap": macd_gap,
            "st_golden_cross": st_ma_golden_cross.fillna(False).astype(float),
            "lt_golden_cross": lt_ma_golden_cross.fillna(False).astype(float),
            "macd_golden_cross": macd_golden_cross.fillna(False).astype(float),
        }).dropna(subset=["macd"])

    def save_technicals_bulk(self, df: pd.DataFrame, stock_id: int):
        if df.empty:
            return False
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            df["stock_id"] = stock_id
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["last_updated"] = datetime.now()

            df_melt = df.melt(
                id_vars=["date", "stock_id"],
                value_vars=[c for c in df.columns if c not in ["date", "stock_id", "last_updated"]],
                var_name="indicator_code",
                value_name="value"
            )

            df_melt["indicator_type_id"] = df_melt["indicator_code"].map(self.indicator_id_map)
            df_melt = df_melt.dropna(subset=["indicator_type_id", "value"])
            df_melt["indicator_type_id"] = df_melt["indicator_type_id"].astype(int)

            records = [
                (row.stock_id, row.date, row.indicator_type_id, float(row.value), datetime.now())
                for row in df_melt.itertuples()
            ]

            sql = """
                INSERT INTO technical_indicators 
                    (stock_id, date, indicator_type_id, value, last_updated)
                VALUES %s
                ON CONFLICT (stock_id, date, indicator_type_id) DO UPDATE SET
                    value = EXCLUDED.value,
                    last_updated = EXCLUDED.last_updated;
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

    def update_all_technicals_parallel(self, max_workers=8):
        symbols = self.fetch_stock_symbols()
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.process_symbol, symbol): symbol
                for symbol in symbols
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol, success = future.result()
                results[symbol] = success

        for symbol, success in results.items():
            if success:
                logger.info(f"{symbol} 기술적 지표 저장 성공")
            else:
                logger.warning(f"{symbol} 기술적 지표 저장 실패")

        return results


if __name__ == "__main__":
    collector = TechnicalCollector()
    collector.update_all_technicals_parallel( max_workers=12)
    collector.close_pool()