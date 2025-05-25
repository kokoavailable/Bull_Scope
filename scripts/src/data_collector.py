import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timedelta
from helper.common import logger, DB_PARAMS
import concurrent.futures
import time
import numpy as np


class DataCollector:

    # 클래스 레벨 풀

    connection_pool = None

    def __init__(self, minconn=4, maxconn=16):
        if DataCollector.connection_pool is None:
            DataCollector.connection_pool = ThreadedConnectionPool(
                minconn, maxconn, **DB_PARAMS
            )
        self._ensure_tables()

    def _get_conn(self):
        # 풀에서 커넥션 할당
        return DataCollector.connection_pool.getconn()
    
    def _put_conn(self, conn):
        # 커넥션 반환
        DataCollector.connection_pool.putconn(conn)

    def _ensure_tables(self):
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_price (
                symbol VARCHAR(16) NOT NULL,
                date DATE NOT NULL,
                open NUMERIC,
                high NUMERIC,
                low NUMERIC,
                close NUMERIC,
                adj_close NUMERIC,
                volume BIGINT,
                PRIMARY KEY(symbol, date)
            );
            """)
            conn.commit()
            
        finally:
            cur.close()
            self._put_conn(conn)

    def fetch_stock_symbols(self, market: str = "US", filter_options: dict = None):
        if market.upper() == "US":
            url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
            df = pd.read_csv(url, sep="|")
            df = df.dropna(subset=['Symbol'])

           # 기본 필터링 (더 엄격하게)
            default_filters = {
                'exclude_test_issues': True,
                'exclude_special_symbols': True,
                'exclude_name_keywords': ['Acquisition', 'SPAC', 'Warrant', 'Unit', 'Right'],
                'normal_financial_status_only': True,
                'max_symbol_length': 5
            }

            if filter_options:
                default_filters.update(filter_options)
            filter_options = default_filters


            if filter_options:
                # ETF 제외 (가장 일반적인 필터)
                if filter_options.get('exclude_etf', False):
                    df = df[df['ETF'] != 'Y']
                
                # 테스트 이슈 제외
                if filter_options.get('exclude_test_issues', True):
                    df = df[df['Test Issue'] != 'Y']
                
                # 특수문자 포함된 심볼 제외 (워런트, 유닛 등)
                if filter_options.get('exclude_special_symbols', True):
                    df = df[~df['Symbol'].str.contains(r'[\.\-\+\=\$]', na=False)]
                
                # 금융상태가 정상인 것만 (Delisted 등 제외)
                if filter_options.get('normal_financial_status_only', False):
                    df = df[df['Financial Status'].isna() | (df['Financial Status'] == '')]
                
                # 특정 거래소만 선택
                if 'exchanges' in filter_options:
                    # N=NYSE, Q=NASDAQ, P=NYSE Arca, Z=BATS 등
                    df = df[df['Listing Exchange'].isin(filter_options['exchanges'])]
                
                # 특정 마켓 카테고리만 (Q=NASDAQ Global Select, G=NASDAQ Global, S=NASDAQ Capital 등)
                if 'market_categories' in filter_options:
                    df = df[df['Market Category'].isin(filter_options['market_categories'])]
                
                # 특정 키워드가 포함된 회사명 제외 (SPAC, Acquisition 등)
                if 'exclude_name_keywords' in filter_options:
                    pattern = '|'.join(filter_options['exclude_name_keywords'])
                    df = df[~df['Security Name'].str.contains(pattern, case=False, na=False)]
                
                # NextShares 제외
                if filter_options.get('exclude_nextshares', False):
                    df = df[df['NextShares'] != 'Y']
            
            return df['Symbol'].tolist()
        else:
            raise NotImplementedError("Only US supported for now")

    def fetch_stock_data(self, symbol: str, period: str = "1y"):
        try:
            logger.debug(f"Fetching {symbol} with period={period}")
            
            ticker = yf.Ticker(symbol)
            
            # max period 처리 개선 - 핵심 수정 부분!
            if period.lower() == "max":
                try:
                    df = ticker.history(period="max")
                except Exception as e:
                    logger.warning(f"{symbol}: max period 실패, 10y로 시도 - {str(e)}")
                    try:
                        df = ticker.history(period="10y")
                    except Exception as e2:
                        logger.warning(f"{symbol}: 10y도 실패, 5y로 시도 - {str(e2)}")
                        try:
                            df = ticker.history(period="5y")
                        except Exception as e3:
                            logger.warning(f"{symbol}: 5y도 실패, 2y로 최종 시도 - {str(e3)}")
                            df = ticker.history(period="2y")
            else:
                df = ticker.history(period=period)
            
            if df.empty:
                logger.warning(f"{symbol}: 데이터가 없습니다 (period={period})")
                return pd.DataFrame(), f"No data available for {symbol}"
            
            df.reset_index(inplace=True)
            logger.debug(f"{symbol}: {len(df)} rows fetched")
            return df, None
            
        except Exception as e:
            error_msg = f"{symbol} 데이터 가져오기 실패 (period={period}): {str(e)}"
            logger.error(error_msg)
            return pd.DataFrame(), error_msg

    def save_price_data_bulk(self, df: pd.DataFrame, symbol: str):
        if df.empty:
            return False, "Empty dataframe"
        
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            df["symbol"] = symbol
            df["date"] = pd.to_datetime(df["Date"]).dt.date
            df["adj_close"] = df.get("Adj Close", df["Close"])
            df = df.replace({np.nan: None})
            records = df[["symbol", "date", "Open", "High", "Low", "Close", "adj_close", "Volume"]].values.tolist()

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
            logger.debug(f"{symbol}: {len(records)} records saved")
            return True, None
        except Exception as e:
            conn.rollback()
            error_msg = f"{symbol} 데이터 저장 실패: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
        finally:
            cur.close()
            self._put_conn(conn)

    def fetch_and_save(self, symbol: str, period: str = "1y", delay: float = 0.2):
        try:
            logger.debug(f"🔄 Processing {symbol} (period={period})...")
            
            df, fetch_error = self.fetch_stock_data(symbol, period)
            if fetch_error:
                logger.warning(f"❌ {symbol}: {fetch_error}")
                return symbol, False, fetch_error
            
            success, save_error = self.save_price_data_bulk(df, symbol)
            if not success:
                logger.error(f"💾 {symbol}: {save_error}")
                return symbol, False, save_error
            
            time.sleep(delay)
            logger.info(f"✅ {symbol}: SUCCESS ({len(df)} records)")
            return symbol, True, None
        except Exception as e:
            error_msg = f"{symbol} 처리 중 예상치 못한 에러: {str(e)}"
            logger.error(f"💥 {error_msg}")
            return symbol, False, error_msg


    def update_all_stocks_parallel(self, market="US", period="1y", delay=0.2, max_workers=8):
        logger.debug(f"시작: {market} 마켓, period={period}, delay={delay}, workers={max_workers}")
        symbols = self.fetch_stock_symbols(market)
        logger.debug(f"총 {len(symbols)}개 심볼 처리 예정")

        results = {}
        errors = {}
        success_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_symbol = {
                executor.submit(self.fetch_and_save, symbol, period, delay): symbol
                for symbol in symbols
            }
            for i, future in enumerate(concurrent.futures.as_completed(future_to_symbol)):
                symbol, success, error = future.result()
                results[symbol] = success

                if success:
                    success_count += 1
                else:
                    errors[symbol] = error
                
                                # 진행상황 로깅 (매 100개마다)
                if (i + 1) % 100 == 0 or (i + 1) == len(symbols):
                    logger.debug(f"진행: {i+1}/{len(symbols)} ({success_count} 성공)")
        
        logger.debug(f"=== 최종 결과 ===")
        logger.debug(f"총 처리: {len(symbols)}")
        logger.debug(f"성공: {success_count}")
        logger.debug(f"실패: {len(symbols) - success_count}")

        if errors:
            logger.info(f"\n=== 실패 상세 ===")
            for symbol, error in list(errors.items())[:10]:  # 첫 10개만
                logger.error(f"{symbol}: {error}")
            if len(errors) > 10:
                logger.info(f"... 그 외 {len(errors) - 10}개 더 실패")
        
        return results


    def close(self):
        if DataCollector.connection_pool:
            DataCollector.connection_pool.closeall()

if __name__ == "__main__":

    start_time = time.time()
    collector = DataCollector()

    result = collector.update_all_stocks_parallel(
        market="US", 
        period="max", 
        delay=0.5, 
        max_workers=4
    )

    end_time = time.time()
    elapsed_time = end_time - start_time
    minutes, seconds = divmod(elapsed_time, 60)

    logger.info(f"전체 처리 시간: {int(minutes)}분 {int(seconds)}초")
    collector.close()
