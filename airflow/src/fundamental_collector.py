# fundamentals_collector.py
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import threading
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, date
from helper.common import logger, fetch_stock_symbols, YF_SESSION
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

    # ──────────────────────────────────────────────
    # 1) 심볼 목록 가져오기
    # ──────────────────────────────────────────────
    def fetch_stock_symbols(self, market: str = "US"):
        if market.upper() != "US":
            raise NotImplementedError("Only US supported")

        return fetch_stock_symbols()

    # ──────────────────────────────────────────────
    # 2) yfinance → fundamentals dict
    # ──────────────────────────────────────────────
    _MAP = {
        "market_cap":      "marketCap",
        "pe_ratio":        "trailingPE",
        "pb_ratio":        "priceToBook",
        "debt_to_equity":  "debtToEquity",
        "current_ratio":   "currentRatio",
        "quick_ratio":     "quickRatio",
        "roe":             "returnOnEquity",
        "roa":             "returnOnAssets",
        "eps":             "trailingEps",
        "revenue":         "totalRevenue",
        "net_income":      "netIncomeToCommon",
    }

    def fetch_fundamental_data(self, symbol: str):
        try:
            info = yf.Ticker(symbol).info
            if not info:
                logger.warning(f"[{symbol}] fundamentals 없음")
                return None
            data = {k: info.get(src) for k, src in self._MAP.items()}
            data["symbol"] = symbol
            data["date"] = date.today()     # 일(UTC) 단위 스냅샷

            for k in list(self._MAP):           # market_cap, pe_ratio, ...
                val = data.get(k)
                # 문자열 → 숫자 / 변환 실패 시 NaN
                val = pd.to_numeric(val, errors="coerce")
                # NaN → None  (psycopg2 가 NULL 로 보냄)
                if pd.isna(val):
                    val = None
                data[k] = float(val) if val is not None else None

            return data
        except Exception as e:
            logger.error(f"[{symbol}] fundamentals 가져오기 실패: {e}")
            return None

    # ──────────────────────────────────────────────
    # 3) 저장 (UPSERT)
    # ──────────────────────────────────────────────
    def save_fundamental_bulk(self, rows: list[dict]) -> bool:
        if not rows:
            return False
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cols = [
                "symbol", "date", "market_cap", "pe_ratio", "pb_ratio",
                "debt_to_equity", "current_ratio", "quick_ratio",
                "roe", "roa", "eps", "revenue", "net_income"
            ]
            records = [tuple(r.get(c) for c in cols) for r in rows]

            sql = f"""
                INSERT INTO stock_fundamentals ({', '.join(cols)})
                VALUES %s
                ON CONFLICT (symbol, date) DO UPDATE SET
                    market_cap     = EXCLUDED.market_cap,
                    pe_ratio       = EXCLUDED.pe_ratio,
                    pb_ratio       = EXCLUDED.pb_ratio,
                    debt_to_equity = EXCLUDED.debt_to_equity,
                    current_ratio  = EXCLUDED.current_ratio,
                    quick_ratio    = EXCLUDED.quick_ratio,
                    roe            = EXCLUDED.roe,
                    roa            = EXCLUDED.roa,
                    eps            = EXCLUDED.eps,
                    revenue        = EXCLUDED.revenue,
                    net_income     = EXCLUDED.net_income;
            """
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
    def fetch_and_save(self, symbol: str, delay: float = 0.2):
        data = self.fetch_fundamental_data(symbol)
        ok = self.save_fundamental_bulk([data]) if data else False
        time.sleep(delay)
        return symbol, ok

    # ──────────────────────────────────────────────
    # 5) 병렬 실행
    # ──────────────────────────────────────────────
    def update_all_parallel(self, market="US", delay=0.2, max_workers=8):
        symbols = self.fetch_stock_symbols(market)
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            fut2sym = {ex.submit(self.fetch_and_save, s, delay): s for s in symbols}
            for fut in concurrent.futures.as_completed(fut2sym):
                sym, ok = fut.result()
                results[sym] = ok
        return results

# ──────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    collector = FundamentalCollector()
    summary = collector.update_all_parallel(max_workers=8)

    for sym, ok in summary.items():
        if ok:
            logger.info(f"{sym} fundamentals 업데이트 성공")
        else:
            logger.error(f"{sym} fundamentals 업데이트 실패")

    collector.close_pool()
