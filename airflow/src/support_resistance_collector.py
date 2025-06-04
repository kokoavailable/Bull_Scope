import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from helper.common import logger, DB_PARAMS
from src.base_collector import BaseCollector
import concurrent.futures

class SupportResistanceCollector(BaseCollector):
    def __init__(self):
        super().__init__()
        self.method_id_map = self._load_method_id_map()

    def _load_method_id_map(self):
        """support_resistance_methods에서 {code: id} dict 반환"""
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("SELECT code, id FROM support_resistance_methods;")
            result = dict(cur.fetchall())
            return result
        finally:
            cur.close()
            self._put_conn(conn)

    def get_method_id(self, method_code: str) -> int:
        mid = self.method_id_map.get(method_code)
        if mid is None:
            raise ValueError(f"Method code '{method_code}' not found in support_resistance_methods.")
        return mid

    # ────────────────────────────────────────────────────────────────────────────
    # 1) 데일리 피벗 포인트 계산 및 저장
    def save_pivot_daily(self, symbol: str) -> bool:
        """
        전일 가격을 기준으로 오늘 데일리 피벗(R1, R2, S1, S2)을 계산하여 support_resistance(pivot_daily)에 Upsert
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            # "어제"가 주말/공휴일일 수 있으므로, 실제 가장 최근 거래일을 가져오려면
            # ORDER BY date DESC LIMIT 1 로 조회
            today = datetime.now().date()
            cur.execute("""
                SELECT p.high, p.low, p.close, p.open
                  FROM stock_prices p
                  JOIN stocks s ON s.id = p.stock_id
                 WHERE s.symbol = %s
                   AND p.date < %s
                 ORDER BY p.date DESC
                 LIMIT 1;
            """, (symbol, today))
            # 가장 최근의 데이터 조회
            row = cur.fetchone()
            if not row:
                logger.warning(f"[{symbol}] 전일 가격 데이터 없음 → pivot_daily 건너뜀")
                return False

            H, L, C, O = row  # 전일 고가, 저가, 종가, 시가
            # 2) 피벗 공식 계산 (P, R1, S1, R2, S2 등)
            #    P = (O + H + L + C) / 4
            P = (O + H + L + C) / 4
            R1 = 2 * P - L
            S1 = 2 * P - H
            R2 = P + (H - L)
            S2 = P - (H - L)

            method_id = self.get_method_id('pivot_daily')
            recs = [
                (symbol, today, float(R1), False, None, method_id, datetime.now()),
                (symbol, today, float(R2), False, None, method_id, datetime.now()),
                (symbol, today, float(S1), True, None, method_id, datetime.now()),
                (symbol, today, float(S2), True, None, method_id, datetime.now()),
            ]

            # 4) stock_id 조회, support_resistance Upsert
            #    (symbol → stock_id) 매핑 한번에 해 두고 내부적으로 재활용하거나,  
            #    여기선 간단히 subquery로 처리
            sql = """
            INSERT INTO support_resistance (
                stock_id, date, price_level, is_support, strength, method_id, last_updated
            )
            VALUES (
                (SELECT id FROM stocks WHERE symbol = %s),
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (stock_id, date, price_level, is_support, method_id) DO UPDATE SET
                strength     = EXCLUDED.strength,
                last_updated = EXCLUDED.last_updated;
            """
            # # execute 여러 번 반복하기보다는 execute_values로 batch 처리해도 됩니다.
            # values = []
            # for rec in recs:
            #     # rec = (symbol, today, price_level, is_support, strength, method, last_updated)
            #     values.append((rec[0], rec[1], rec[2], rec[3], rec[4], rec[5], rec[6]))

            # execute_values 용 placeholder: 
            # (SELECT id FROM stocks WHERE symbol = %s), %s, %s, %s, %s, %s, %s
            # symbol은 각 튜플마다 중복이지만 잘 작동합니다.
            execute_values(cur, sql, recs,
                           template=None, page_size=100)
            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"[{symbol}] pivot_daily 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)


    # ────────────────────────────────────────────────────────────────────────────
    # 2) 주간 피벗 포인트 계산 및 저장
    def save_pivot_weekly(self, symbol: str) -> bool:
        """
        전주(上週) 가격을 기준으로 금주 주간 피벗 포인트(P, R1, R2, S1, S2)를 계산하여
        support_resistance(pivot_weekly)에 Upsert.
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            today = datetime.now().date()
            # 1) 이번주 월요일 날짜 구하기
            this_monday = today - timedelta(days=today.weekday())
            # 2) 전주 월~금까지 데이터 가져오기 (주말은 제외, 한국 주식시장은 보통 월~금)
            last_monday = this_monday - timedelta(days=7)

            cur.execute("""
                SELECT MAX(p.high), MIN(p.low),
                    FIRST_VALUE(p.open) OVER (ORDER BY p.date ASC) AS first_open,
                    LAST_VALUE(p.close) OVER (ORDER BY p.date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
                FROM stock_prices p
                JOIN stocks s ON s.id = p.stock_id
                WHERE s.symbol = %s
                AND p.date >= %s
                AND p.date < %s
            """, (symbol, last_monday, this_monday))  # 전주 월요일 이상, 이번주 월요일 미만 (즉, 전주 한 주)

            row = cur.fetchone()
            if not row or any(v is None for v in row):
                logger.warning(f"[{symbol}] 주간 피벗 포인트: 전주 데이터 없음 → pivot_weekly 건너뜀")
                return False

            H, L, O, C = row  # 전주 high, low, open(월), close(금)
            # 피벗 공식 계산 (주간)
            P = (O + H + L + C) / 4
            R1 = 2 * P - L
            S1 = 2 * P - H
            R2 = P + (H - L)
            S2 = P - (H - L)

            # 오늘(또는 이번주 월요일 날짜 기준)으로 저장
            method_id = self.get_method_id('pivot_weekly')
            recs = [
                (symbol, this_monday, float(R1), False, None, method_id, datetime.now()),
                (symbol, this_monday, float(R2), False, None, method_id, datetime.now()),
                (symbol, this_monday, float(S1), True, None, method_id, datetime.now()),
                (symbol, this_monday, float(S2), True, None, method_id, datetime.now()),
            ]

            sql = """
            INSERT INTO support_resistance (
                stock_id, date, price_level, is_support, strength, method_id, last_updated
            )
            VALUES (
                (SELECT id FROM stocks WHERE symbol = %s),
                %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (stock_id, date, price_level, is_support, method_id) DO UPDATE SET
                strength     = support_resistance.strength,
                last_updated = EXCLUDED.last_updated;
            """
            execute_values(cur, sql, recs, template=None, page_size=100)
            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"[{symbol}] pivot_weekly 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)


    # ────────────────────────────────────────────────────────────────────────────
    # 2) 데일리 매물대(Volume Profile) 계산 및 저장
    def save_vp_daily(self, symbol: str, lookback_days: int = 60) -> bool:
        """
        최근 lookback_days 일봉 데이터를 기준으로 데일리 매물대 상위 top_n개 가격 구간(bins)을 계산하여
        support_resistance(vp_daily)에 Upsert
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            # 1) stock_id 및 lookback_days만큼의 일별 가격(high, low, volume) 불러오기
            cur.execute("""
                SELECT s.id, p.date, p.high, p.low, p.close, p.volume
                  FROM stock_prices p
                  JOIN stocks s ON s.id = p.stock_id
                 WHERE s.symbol = %s
                 ORDER BY p.date DESC
                 LIMIT %s;
            """, (symbol, lookback_days * 2))  # 여유 있게 두 배 가져온 뒤 최신 N개만 사용
            rows = cur.fetchall()
            if not rows:
                logger.warning(f"[{symbol}] 데일리 VP용 가격 데이터 없음")
                return False

            # DataFrame으로 정리 후, 정확히 lookback_days개만 남기기
            df_raw = pd.DataFrame(rows, columns=["stock_id", "date", "high", "low", "close", "volume"])
            df_raw["date"] = pd.to_datetime(df_raw["date"])
            df_raw = df_raw.sort_values("date", ascending=False).head(lookback_days).reset_index(drop=True)

            stock_id = int(df_raw.loc[0, "stock_id"])

            # 2) bin edges 계산 (평균 종가 × 0.5% or 고정 단위 등)
            min_price = df_raw["low"].min()
            max_price = df_raw["high"].max()
            avg_close = df_raw["close"].mean()
            num_bins = 10
            bin_size = (max_price - min_price) / num_bins  # 고정 빈갯수 10개

            # edges: floor(min/bs)*bs ~ ceil(max/bs)*bs 간격
            low_edge = np.floor(min_price / bin_size) * bin_size
            high_edge = np.ceil(max_price / bin_size) * bin_size
            edges = np.arange(low_edge, high_edge + bin_size, bin_size)
            bin_centers = edges[:-1] + (bin_size / 2) # 넘파이 배열 벡터 연산
            if bin_size == 0: # 휴면주 방어 로직
                bin_size = max(avg_close * 0.005, 0.1)

            # 3) 각 bin별 누적 거래량 초기화
            vp_buckets = pd.DataFrame({
                "price_bin": bin_centers,
                "volume_sum": 0.0
            }).set_index("price_bin")

            # 4) 각 일봉의 day_vol을 high~low 구간에 걸친 bin들에 균등 분배
            for row in df_raw.itertuples():
                day_low  = row.low
                day_high = row.high
                day_vol  = row.volume
                if day_vol == 0:
                    continue

                mask = (vp_buckets.index + bin_size / 2 >= day_low) & \
                       (vp_buckets.index - bin_size / 2 <= day_high)
                overlapping = vp_buckets.index[mask]

                # 겹치는 bin마다 균등 분배
                per_vol = day_vol / len(overlapping)
                vp_buckets.loc[overlapping, "volume_sum"] += per_vol

            vp_buckets["volume_sum"] = vp_buckets["volume_sum"].astype(int)
            
            all_bins = vp_buckets.reset_index()   # index(=price_bin)를 컬럼으로 만듦
            total_volume = all_bins["volume_sum"].sum()

            method_id = self.get_method_id("vp_daily")
            all_bins["method_id"]  = method_id
            all_bins["is_support"] = all_bins["price_bin"] < avg_close
            all_bins["strength"]   = all_bins["volume_sum"] / total_volume
            all_bins["method_id"]  = self.get_method_id("vp_daily")
            all_bins["date"]       = datetime.now().date()
            all_bins["stock_id"]   = stock_id
            all_bins["last_updated"] = datetime.now()

            # 7) support_resistance에 Upsert (stock_id, date, price_level, is_support, method 단일성)
            # price_bin → price_level 로 컬럼명 변경
            to_upsert = all_bins[[
                "stock_id", "date", "price_bin", "is_support", "strength", "method_id", "last_updated"
            ]].rename(columns={"price_bin": "price_level"})


            records = to_upsert.to_records(index=False)
            sql = """
            INSERT INTO support_resistance (
                stock_id, date, price_level, is_support, strength, method_id, last_updated
            ) VALUES %s
            ON CONFLICT (stock_id, date, price_level, is_support, method_id) DO UPDATE SET
                strength     = EXCLUDED.strength,
                last_updated = EXCLUDED.last_updated;
            """
            execute_values(cur, sql, records)
            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"[{symbol}] vp_daily 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    # ────────────────────────────────────────────────────────────────────────────
    # 3) 장기 매물대(vp_longterm) 계산 및 저장
    def save_vp_longterm(self, symbol: str, timeframe_years: int = 5) -> bool:
        """
        과거 timeframe_years년치 일봉을 모아서 장기 매물대 상위 top_n개를 계산하여
        1) volume_profile_archives (모든 bin) 저장
        2) support_resistance(vp_longterm) 상위 top_n개 저장
        """
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            # 1) 과거 timeframe_years년치 데이터 로드
            today = datetime.now().date()
            past_date = today - timedelta(days=int(timeframe_years * 365))

            cur.execute("""
                SELECT s.id, p.date, p.high, p.low, p.close, p.volume
                  FROM stock_prices p
                  JOIN stocks s ON s.id = p.stock_id
                 WHERE s.symbol = %s
                   AND p.date BETWEEN %s AND %s
                 ORDER BY p.date ASC;
            """, (symbol, past_date, today))
            rows = cur.fetchall()
            if not rows:
                logger.warning(f"[{symbol}] vp_longterm용 가격 데이터 없음")
                return False

            df_hist = pd.DataFrame(rows, columns=["stock_id", "date", "high", "low", "close", "volume"])
            df_hist["date"] = pd.to_datetime(df_hist["date"])
            stock_id = int(df_hist.loc[0, "stock_id"])

            # 2) bin edges 계산 (평균 종가 × 0.5% 등)
            min_price = df_hist["low"].min()
            max_price = df_hist["high"].max()
            avg_close = df_hist["close"].mean()
            bin_size = (max_price - min_price) / 10

            low_edge = np.floor(min_price / bin_size) * bin_size
            high_edge = np.ceil(max_price / bin_size) * bin_size
            edges = np.arange(low_edge, high_edge + bin_size, bin_size)
            bin_centers = edges[:-1] + (bin_size / 2)

            # 3) 각 bin별 누적 거래량 계산
            vp_buckets = pd.DataFrame({
                "price_bin": bin_centers,
                "volume_sum": 0.0
            }).set_index("price_bin")

            for row in df_hist.itertuples():
                day_low  = row.low
                day_high = row.high
                day_vol  = row.volume
                if day_vol == 0:
                    continue

                mask = (vp_buckets.index + bin_size / 2 >= day_low) & \
                    (vp_buckets.index - bin_size / 2 <= day_high)
                overlapping = vp_buckets.index[mask]

                per_vol = day_vol / len(overlapping)
                vp_buckets.loc[overlapping, "volume_sum"] += per_vol

            vp_buckets["volume_sum"] = vp_buckets["volume_sum"].astype(int)
            all_bins = vp_buckets.reset_index().rename(columns={"price_bin": "price_level"})

            # 아래부터 새로 추가! (전체 bin을 support_resistance에 strength 비율로 저장)
            total_volume = all_bins["volume_sum"].sum()
            all_bins["is_support"] = all_bins["price_level"] < avg_close
            all_bins["strength"] = (all_bins["volume_sum"] / total_volume * 100).round(1)
            all_bins["method_id"] = self.get_method_id("vp_longterm")
            all_bins["date"] = today
            all_bins["stock_id"] = stock_id
            all_bins["last_updated"] = datetime.now()

            to_upsert = all_bins[[
                "stock_id", "date", "price_level", "is_support", "strength", "method_id", "last_updated"
            ]]

            recs_res = to_upsert.to_records(index=False)
            sql_res = """
            INSERT INTO support_resistance (
                stock_id, date, price_level, is_support, strength, method_id, last_updated
            ) VALUES %s
            ON CONFLICT (stock_id, date, price_level, is_support, method_id) DO UPDATE SET
                strength     = EXCLUDED.strength,
                last_updated = EXCLUDED.last_updated;
            """
            execute_values(cur, sql_res, recs_res)

            conn.commit()
            return True

        except Exception as e:
            conn.rollback()
            logger.error(f"[{symbol}] vp_longterm 저장 실패: {e}")
            return False
        finally:
            cur.close()
            self._put_conn(conn)

    def process_symbol_all(self, sym):
        result = {
            "pivot_daily": False,
            "pivot_weekly": False,
            "vp_daily": False,
            "vp_longterm": False
        }
        try:
            result["pivot_daily"] = self.save_pivot_daily(sym)
        except Exception as e:
            logger.error(f"[{sym}] save_pivot_daily 실패: {e}")
        try:
            result["pivot_weekly"] = self.save_pivot_weekly(sym)
        except Exception as e:
            logger.error(f"[{sym}] save_pivot_weekly 실패: {e}")
        try:
            result["vp_daily"] = self.save_vp_daily(sym)
        except Exception as e:
            logger.error(f"[{sym}] save_vp_daily 실패: {e}")
        try:
            result["vp_longterm"] = self.save_vp_longterm(sym, timeframe_years=5)
        except Exception as e:
            logger.error(f"[{sym}] save_vp_longterm 실패: {e}")
        return result


    # ────────────────────────────────────────────────────────────────────────────
    # 4) 여러 심볼을 병렬로 처리하는 메서드
    def update_all(self, symbols: list[str], max_workers: int = 8):
        results = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.process_symbol_all, sym): sym
                for sym in symbols
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                sym = future_to_symbol[future]
                try:
                    results[sym] = future.result()
                except Exception as e:
                    logger.error(f"[{sym}] SupportResistance 처리 도중 예외: {e}")
                    results[sym] = {
                        'pivot_daily': False,
                        'pivot_weekly': False,
                        'vp_daily': False,
                        'vp_longterm': False
                    }
        return results

    def close_pool(self):
        super().close_pool()

# 사용 예시:
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute("SELECT symbol FROM stocks;")
    symbols = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()

    sr_collector = SupportResistanceCollector()
    results = sr_collector.update_all(symbols, max_workers=8)
    for sym, outcome in results.items():
        logger.info(
            f"{sym} → pivot_daily: {outcome['pivot_daily']}, "
            f"pivot_weekly: {outcome['pivot_weekly']}, "
            f"vp_daily: {outcome['vp_daily']}, "
            f"vp_longterm: {outcome['vp_longterm']}"
        )
    sr_collector.close_pool()
