# fundamentals_collector.py
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import execute_values
from typing import List, Dict
from datetime import datetime, date
from helper.common import logger, fetch_stock_symbols
import concurrent.futures
import time
from src.base_collector import BaseCollector

class FundamentalCollector(BaseCollector):
    
    """
    • 심볼 목록 → yfinance .info 에서 핵심 지표만 추출
    • stock_fundamentals 테이블 (symbol, date) 단위로 UPSERT
    """
    # ──────────────────────────────────────────────
    # 초기화 & 테이블 보장
    # ──────────────────────────────────────────────
    def __init__(self):
        super().__init__()
        self.indicator_types = self._load_indicator_types()

    def _load_indicator_types(self) -> Dict[str, dict]:
        """
        {code: {"id": id, "info_key": info_key, "name": name, ...}} 형태로 반환
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT code, id, info_key FROM fundamental_indicator_types;")
            rows = cur.fetchall()
            return {code: {"id": id, "info_key": info_key} for code, id, info_key in rows}
        finally:
            cur.close()
            self._put_conn(conn)
        

    def fetch_stock_symbols(self, market: str = "US") -> List[str]:
        if market.upper() != "US":
            raise NotImplementedError("Only US supported")
        return fetch_stock_symbols()


    def fetch_fundamental_data(self, symbol: str):
        """
        yfinance에서 해당 심볼의 핵심 재무 지표(info)만 뽑아서 dict로 반환
        반환 dict에는 아직 stock_id가 없으므로, update_all_parallel에서 매핑을 넣어줘야 함.
        """
        try:
            info = yf.Ticker(symbol).info
            if not info:
                logger.warning(f"[{symbol}] fundamentals 없음")
                return None
            
            data = {"symbol": symbol, "date": date.today()}
            for code, meta in self.indicator_types.items():
                val = info.get(meta['info_key'])
                val = pd.to_numeric(val, errors="coerce")
                data[code] = None if pd.isna(val) else float(val)
            return data
        
        except Exception as e:
            logger.error(f"[{symbol}] fundamentals 가져오기 실패: {e}")
            return None

    # ──────────────────────────────────────────────
    # 3) 저장 (UPSERT)
    # ──────────────────────────────────────────────
    def save_fundamental_bulk(self, rows: list[dict], stock_id: int) -> bool:
        """
        rows: [{indicator_code, value, date}]
        """
        if not rows:
            return False
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            sql = """
                INSERT INTO fundamental_indicators
                    (stock_id, date, indicator_type_id, value, last_updated)
                VALUES %s
                ON CONFLICT (stock_id, date, indicator_type_id) DO UPDATE SET
                    value = EXCLUDED.value,
                    last_updated = EXCLUDED.last_updated
            """
            records = [
                (
                    stock_id,
                    r["date"],
                    self.indicator_types[r["indicator_code"]]["id"],
                    r["value"],
                    datetime.now()
                )
                for r in rows if r["value"] is not None and r["indicator_code"] in self.indicator_types
            ]
            if not records:
                logger.warning(f"저장할 유효 데이터 없음 (stock_id={stock_id})")
                return False
            execute_values(cur, sql, records)
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.error(f"Fundamentals 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    # ──────────────────────────────────────────────
    # 4) 심볼 하나 처리
    # ──────────────────────────────────────────────
    def fetch_and_save(self, symbol: str, stock_id:int, delay: float = 0.2):
        data = self.fetch_fundamental_data(symbol)

        date_ = data["date"]
        rows = [
            {"indicator_code": k, "value": data[k], "date": date_}
            for k in self.indicator_types.keys()
        ]
        ok = self.save_fundamental_bulk(rows, stock_id)
        time.sleep(delay)
        return symbol, ok

    # ──────────────────────────────────────────────
    # 5) 병렬 실행
    # ──────────────────────────────────────────────
    def update_all_parallel(self, 
                            market="US",  
                            max_workers=8,
                            delay=0.1):
        symbols = self.fetch_stock_symbols(market)

        mapping = self._get_stock_id_map(symbols)
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_to_sym = {}
            for sym in symbols:
                sid = mapping.get(sym)
                future = ex.submit(self.fetch_and_save, sym, sid, delay)
                future_to_sym[future] = sym
                time.sleep(delay)

            for future in concurrent.futures.as_completed(future_to_sym):
                sym = future_to_sym[future]
                try:
                    sym, ok = future.result()
                    results[sym] = ok

                except Exception as exc:
                    logger.error(f"[{sym}] 병렬 처리 중 예외: {exc}")
                    results[sym] = False

        return results

# ──────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    collector = FundamentalCollector()
    summary = collector.update_all_parallel(max_workers=4, delay=0.2)

    for sym, ok in summary.items():
        if ok:
            logger.info(f"{sym} fundamentals 업데이트 성공")
        else:
            logger.error(f"{sym} fundamentals 업데이트 실패")

    collector.close_pool()
