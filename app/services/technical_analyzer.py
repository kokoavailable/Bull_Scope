import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
import logging
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta

from app.models.database import Stock, StockPrice, StockTechnical, SupportResistance

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TechnicalAnalyzer:
    def __init__(self, db: Session):
        self.db = db
        
    def calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """
        상대강도지수(RSI) 계산
        
        Args:
            prices: 가격 데이터 시리즈 (종가)
            period: RSI 기간 (기본값: 14일)
            
        Returns:
            RSI 값 시리즈
        """
        # 가격 변화량 계산
        delta = prices.diff()
        
        # 상승/하락 구분
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 평균 상승/하락 계산
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # 첫 번째 값 계산
        first_avg_gain = gain.iloc[:period].mean()
        first_avg_loss = loss.iloc[:period].mean()
        
        # avg_gain 및 avg_loss를 첫 번째 값부터 다시 계산
        avg_gain.iloc[period] = first_avg_gain
        avg_loss.iloc[period] = first_avg_loss
        
        # 나머지 값을 Wilder 스무딩 방식으로 계산
        for i in range(period + 1, len(prices)):
            avg_gain.iloc[i] = (avg_gain.iloc[i-1] * (period-1) + gain.iloc[i]) / period
            avg_loss.iloc[i] = (avg_loss.iloc[i-1] * (period-1) + loss.iloc[i]) / period
            
        # RS 및 RSI 계산
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
        
    def calculate_macd(self, prices: pd.Series, 
                      fast_period: int = 12, 
                      slow_period: int = 26, 
                      signal_period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        MACD(Moving Average Convergence Divergence) 계산
        
        Args:
            prices: 가격 데이터 시리즈 (종가)
            fast_period: 빠른 EMA 기간 (기본값: 12일)
            slow_period: 느린 EMA 기간 (기본값: 26일)
            signal_period: 시그널 EMA 기간 (기본값: 9일)
            
        Returns:
            MACD 라인, 시그널 라인, 히스토그램 시리즈의 튜플
        """
        # EMA 계산
        ema_fast = prices.ewm(span=fast_period, adjust=False).mean()
        ema_slow = prices.ewm(span=slow_period, adjust=False).mean()
        
        # MACD 라인 계산
        macd_line = ema_fast - ema_slow
        
        # 시그널 라인 계산
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        
        # 히스토그램 계산
        histogram = macd_line - signal_line
        
        return macd_line, signal_line, histogram
    
    def calculate_ppo(self, prices: pd.Series, 
                     fast_period: int = 12, 
                     slow_period: int = 26, 
                     signal_period: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        PPO(Percentage Price Oscillator) 계산 - MACD의 백분율 버전
        
        Args:
            prices: 가격 데이터 시리즈 (종가)
            fast_period: 빠른 EMA 기간 (기본값: 12일)
            slow_period: 느린 EMA 기간 (기본값: 26일)
            signal_period: 시그널 EMA 기간 (기본값: 9일)
            
        Returns:
            PPO 라인, 시그널 라인, 히스토그램 시리즈의 튜플
        """
        # EMA 계산
        ema_fast = prices.ewm(span=fast_period, adjust=False).mean()
        ema_slow = prices.ewm(span=slow_period, adjust=False).mean()
        
        # PPO 라인 계산 (MACD를 느린 EMA로 나누어 백분율로 표현)
        ppo_line = ((ema_fast - ema_slow) / ema_slow) * 100
        
        # 시그널 라인 계산
        signal_line = ppo_line.ewm(span=signal_period, adjust=False).mean()
        
        # 히스토그램 계산
        histogram = ppo_line - signal_line
        
        return ppo_line, signal_line, histogram
    
    def calculate_moving_averages(self, prices: pd.Series) -> Dict[str, pd.Series]:
        """
        다양한 이동평균 계산
        
        Args:
            prices: 가격 데이터 시리즈 (종가)
            
        Returns:
            이동평균 시리즈의 딕셔너리 {'MA20': ma20, 'MA50': ma50, 'MA200': ma200}
        """
        ma20 = prices.rolling(window=20).mean()
        ma50 = prices.rolling(window=50).mean()
        ma200 = prices.rolling(window=200).mean()
        
        return {
            'MA20': ma20,
            'MA50': ma50,
            'MA200': ma200
        }
    
    def calculate_bollinger_bands(self, prices: pd.Series, window: int = 20, num_std: float = 2.0) -> Tuple[pd.Series, pd.Series, pd.Series]:
        """
        볼린저 밴드 계산
        
        Args:
            prices: 가격 데이터 시리즈 (종가)
            window: 이동평균 기간 (기본값: 20일)
            num_std: 표준편차 승수 (기본값: 2.0)
            
        Returns:
            상단 밴드, 중간 밴드(SMA), 하단 밴드 시리즈의 튜플
        """
        # 중간 밴드 (단순이동평균)
        middle_band = prices.rolling(window=window).mean()
        
        # 표준편차 계산
        std = prices.rolling(window=window).std()
        
        # 상단 및 하단 밴드
        upper_band = middle_band + (std * num_std)
        lower_band = middle_band - (std * num_std)
        
        return upper_band, middle_band, lower_band
    
    def identify_support_resistance(self, prices: pd.DataFrame, window: int = 20, threshold: float = 0.03) -> List[Dict[str, Any]]:
        """
        지지선과 저항선 식별
        
        Args:
            prices: 가격 데이터프레임 (날짜, 고가, 저가, 종가 포함)
            window: 분석 윈도우 크기 (기본값: 20일)
            threshold: 가격 수준 근접 임계값 (기본값: 3%)
            
        Returns:
            지지선 및 저항선 정보 목록
        """
        # 피벗 포인트 찾기
        pivot_high = []
        pivot_low = []
        
        for i in range(window, len(prices) - window):
            # 고점 확인 (전후 window 기간 동안의 최고가)
            if prices.iloc[i]['high'] == max(prices.iloc[i-window:i+window+1]['high']):
                pivot_high.append({
                    'date': prices.iloc[i]['date'],
                    'price': prices.iloc[i]['high'],
                    'type': 'resistance'
                })
                
            # 저점 확인 (전후 window 기간 동안의 최저가)
            if prices.iloc[i]['low'] == min(prices.iloc[i-window:i+window+1]['low']):
                pivot_low.append({
                    'date': prices.iloc[i]['date'],
                    'price': prices.iloc[i]['low'],
                    'type': 'support'
                })
                
        # 가격 수준 군집화
        price_levels = []
        
        # 저항선 군집화
        for ph in pivot_high:
            found = False
            for level in price_levels:
                # 기존 가격 수준과의 차이가 임계값 이내인지 확인
                if abs(ph['price'] - level['price']) / level['price'] < threshold:
                    level['count'] += 1
                    level['dates'].append(ph['date'])
                    # 가중 평균으로 가격 수준 업데이트
                    level['price'] = (level['price'] * (level['count'] - 1) + ph['price']) / level['count']
                    found = True
                    break
                    
            if not found:
                price_levels.append({
                    'price': ph['price'],
                    'type': 'resistance',
                    'count': 1,
                    'dates': [ph['date']]
                })
                
        # 지지선 군집화
        for pl in pivot_low:
            found = False
            for level in price_levels:
                # 기존 가격 수준과의 차이가 임계값 이내인지 확인
                if abs(pl['price'] - level['price']) / level['price'] < threshold:
                    level['count'] += 1
                    level['dates'].append(pl['date'])
                    # 가중 평균으로 가격 수준 업데이트
                    level['price'] = (level['price'] * (level['count'] - 1) + pl['price']) / level['count']
                    found = True
                    break
                    
            if not found:
                price_levels.append({
                    'price': pl['price'],
                    'type': 'support',
                    'count': 1,
                    'dates': [pl['date']]
                })
                
        # 강도에 따라 정렬 (발생 횟수가 많을수록 강한 수준)
        price_levels.sort(key=lambda x: x['count'], reverse=True)
        
        # 상위 레벨만 반환
        return price_levels[:10]
    
    def check_golden_cross(self, macd: pd.Series, signal: pd.Series) -> List[Dict[str, Any]]:
        """
        MACD 골든 크로스 및 데드 크로스 확인
        
        Args:
            macd: MACD 라인 시리즈
            signal: 시그널 라인 시리즈
            
        Returns:
            크로스 이벤트 목록 (날짜, 유형)
        """
        cross_events = []
        
        # 전일 MACD와 시그널 라인 차이
        prev_diff = macd.shift(1) - signal.shift(1)
        # 당일 MACD와 시그널 라인 차이
        curr_diff = macd - signal
        
        # 골든 크로스 확인 (MACD가 시그널 라인을 상향 돌파)
        golden_cross = (prev_diff < 0) & (curr_diff > 0)
        
        # 데드 크로스 확인 (MACD가 시그널 라인을 하향 돌파)
        dead_cross = (prev_diff > 0) & (curr_diff < 0)
        
        # 이벤트 목록 생성
        for date in golden_cross[golden_cross].index:
            cross_events.append({
                'date': date,
                'type': 'golden_cross'
            })
            
        for date in dead_cross[dead_cross].index:
            cross_events.append({
                'date': date,
                'type': 'dead_cross'
            })
            
        # 날짜순 정렬
        cross_events.sort(key=lambda x: x['date'])
        
        return cross_events
    
    def analyze_stock(self, symbol: str) -> Dict[str, Any]:
        """
        종목의 기술적 분석 수행
        
        Args:
            symbol: 종목 심볼
            
        Returns:
            기술적 분석 결과 딕셔너리
        """
        try:
            # 주식 정보 조회
            stock = self.db.query(Stock).filter(Stock.symbol == symbol).first()
            if not stock:
                logger.error(f"{symbol} 종목 정보를 찾을 수 없습니다.")
                return {}
                
            # 가격 데이터 조회 (최근 200일)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=365)
            
            price_data = self.db.query(StockPrice).filter(
                StockPrice.stock_id == stock.id,
                StockPrice.date >= start_date,
                StockPrice.date <= end_date
            ).order_by(StockPrice.date.asc()).all()
            
            if not price_data:
                logger.error(f"{symbol} 가격 데이터를 찾을 수 없습니다.")
                return {}
                
            # DataFrame 생성
            df = pd.DataFrame([{
                'date': p.date,
                'open': p.open,
                'high': p.high,
                'low': p.low,
                'close': p.close,
                'volume': p.volume
            } for p in price_data])
            
            # 기술적 지표 계산
            close_prices = df['close']
            
            # RSI 계산
            rsi = self.calculate_rsi(close_prices)
            
            # MACD 계산
            macd_line, signal_line, histogram = self.calculate_macd(close_prices)
            
            # PPO 계산
            ppo_line, ppo_signal, ppo_histogram = self.calculate_ppo(close_prices)
            
            # 이동평균 계산
            mas = self.calculate_moving_averages(close_prices)
            
            # 볼린저 밴드 계산
            upper_band, middle_band, lower_band = self.calculate_bollinger_bands(close_prices)
            
            # 지지선/저항선 식별
            support_resistance_levels = self.identify_support_resistance(df)
            
            # MACD 골든 크로스 확인
            cross_events = self.check_golden_cross(macd_line, signal_line)
            
            # 최근 골든 크로스/데드 크로스 식별
            recent_golden_cross = None
            recent_dead_cross = None
            
            for event in reversed(cross_events):
                if event['type'] == 'golden_cross' and not recent_golden_cross:
                    recent_golden_cross = event['date']
                if event['type'] == 'dead_cross' and not recent_dead_cross:
                    recent_dead_cross = event['date']
                    
            # 최근 데이터 DB에 저장
            for i in range(len(df)):
                date = df.iloc[i]['date']
                
                # 이미 존재하는 기술 지표 확인
                existing = self.db.query(StockTechnical).filter(
                    StockTechnical.stock_id == stock.id,
                    StockTechnical.date == date
                ).first()
                
                # 골든 크로스 여부 확인
                is_golden_cross = False
                if i > 0 and macd_line.iloc[i-1] < signal_line.iloc[i-1] and macd_line.iloc[i] > signal_line.iloc[i]:
                    is_golden_cross = True
                
                if existing:
                    # 기존 데이터 업데이트
                    existing.rsi_14 = rsi.iloc[i] if i < len(rsi) else None
                    existing.macd = macd_line.iloc[i] if i < len(macd_line) else None
                    existing.macd_signal = signal_line.iloc[i] if i < len(signal_line) else None
                    existing.macd_histogram = histogram.iloc[i] if i < len(histogram) else None
                    existing.ma_20 = mas['MA20'].iloc[i] if i < len(mas['MA20']) else None
                    existing.ma_50 = mas['MA50'].iloc[i] if i < len(mas['MA50']) else None
                    existing.ma_200 = mas['MA200'].iloc[i] if i < len(mas['MA200']) else None
                    existing.bolinger_upper = upper_band.iloc[i] if i < len(upper_band) else None
                    existing.bolinger_middle = middle_band.iloc[i] if i < len(middle_band) else None
                    existing.bolinger_lower = lower_band.iloc[i] if i < len(lower_band) else None
                    existing.ppo = ppo_line.iloc[i] if i < len(ppo_line) else None
                    existing.golden_cross = is_golden_cross
                else:
                    # 새 데이터 추가
                    tech = StockTechnical(
                        stock_id=stock.id,
                        date=date,
                        rsi_14=rsi.iloc[i] if i < len(rsi) and not pd.isna(rsi.iloc[i]) else None,
                        macd=macd_line.iloc[i] if i < len(macd_line) and not pd.isna(macd_line.iloc[i]) else None,
                        macd_signal=signal_line.iloc[i] if i < len(signal_line) and not pd.isna(signal_line.iloc[i]) else None,
                        macd_histogram=histogram.iloc[i] if i < len(histogram) and not pd.isna(histogram.iloc[i]) else None,
                        ma_20=mas['MA20'].iloc[i] if i < len(mas['MA20']) and not pd.isna(mas['MA20'].iloc[i]) else None,
                        ma_50=mas['MA50'].iloc[i] if i < len(mas['MA50']) and not pd.isna(mas['MA50'].iloc[i]) else None,
                        ma_200=mas['MA200'].iloc[i] if i < len(mas['MA200']) and not pd.isna(mas['MA200'].iloc[i]) else None,
                        bolinger_upper=upper_band.iloc[i] if i < len(upper_band) and not pd.isna(upper_band.iloc[i]) else None,
                        bolinger_middle=middle_band.iloc[i] if i < len(middle_band) and not pd.isna(middle_band.iloc[i]) else None,
                        bolinger_lower=lower_band.iloc[i] if i < len(lower_band) and not pd.isna(lower_band.iloc[i]) else None,
                        ppo=ppo_line.iloc[i] if i < len(ppo_line) and not pd.isna(ppo_line.iloc[i]) else None,
                        golden_cross=is_golden_cross
                    )
                    self.db.add(tech)
                    
            # 지지선/저항선 저장
            # 기존 지지선/저항선 삭제
            self.db.query(SupportResistance).filter(
                SupportResistance.stock_id == stock.id
            ).delete()
            
            # 새 지지선/저항선 추가
            for level in support_resistance_levels:
                sr = SupportResistance(
                    stock_id=stock.id,
                    date=datetime.now().date(),
                    price_level=level['price'],
                    is_support=level['type'] == 'support',
                    strength=level['count']
                )
                self.db.add(sr)
                
            self.db.commit()
            
            # 최근 데이터 가져오기
            latest_price = df.iloc[-1] if not df.empty else None
            latest_rsi = rsi.iloc[-1] if not rsi.empty else None
            latest_macd = macd_line.iloc[-1] if not macd_line.empty else None
            latest_signal = signal_line.iloc[-1] if not signal_line.empty else None
            latest_histogram = histogram.iloc[-1] if not histogram.empty else None
            latest_ppo = ppo_line.iloc[-1] if not ppo_line.empty else None
            
            # 분석 결과 반환
            result = {
                "symbol": symbol,
                "company_name": stock.company_name,
                "latest_price": latest_price['close'] if latest_price is not None else None,
                "latest_date": latest_price['date'] if latest_price is not None else None,
                "rsi": latest_rsi,
                "macd": {
                    "line": latest_macd,
                    "signal": latest_signal,
                    "histogram": latest_histogram
                },
                "ppo": latest_ppo,
                "moving_averages": {
                    "ma20": mas['MA20'].iloc[-1] if not mas['MA20'].empty else None,
                    "ma50": mas['MA50'].iloc[-1] if not mas['MA50'].empty else None,
                    "ma200": mas['MA200'].iloc[-1] if not mas['MA200'].empty else None
                },
                "support_resistance": support_resistance_levels,
                "recent_golden_cross": recent_golden_cross,
                "recent_dead_cross": recent_dead_cross,
                "is_oversold": latest_rsi < 30 if latest_rsi is not None else False,
                "is_overbought": latest_rsi > 70 if latest_rsi is not None else False,
                "is_uptrend": latest_price['close'] > mas['MA50'].iloc[-1] if latest_price is not None and not mas['MA50'].empty else False,
                "is_momentum_increasing": latest_histogram > 0 if latest_histogram is not None else False
            }
            
            return result
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"{symbol} 기술적 분석 실패: {str(e)}")
            return {}
            
    def scan_for_opportunities(self, criteria: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        기회 스캔 - 설정된 기준에 따라 기회 포착
        
        Args:
            criteria: 스캔 기준 (기본값: RSI < 35)
            
        Returns:
            기회 목록
        """
        if criteria is None:
            criteria = {
                "rsi_max": 35,               # RSI 35 미만
                "debt_to_equity_max": 3.0,   # D/E 3 이하
                "current_ratio_min": 0.5     # 유동비율 0.5 이상
            }
            
        try:
            # 모든 주식 가져오기
            stocks = self.db.query(Stock).all()
            opportunities = []
            
            for stock in stocks:
                # 각 종목에 대한 최신 기술적 지표 조회
                latest_technical = self.db.query(StockTechnical).filter(
                    StockTechnical.stock_id == stock.id
                ).order_by(StockTechnical.date.desc()).first()
                
                # 각 종목에 대한 최신 재무 지표 조회
                latest_fundamental = self.db.query(StockFundamental).filter(
                    StockFundamental.stock_id == stock.id
                ).order_by(StockFundamental.date.desc()).first()
                
                if not latest_technical or not latest_fundamental:
                    continue
                    
                # 기준 충족 여부 확인
                meets_criteria = True
                
                # RSI 기준 확인
                if criteria.get("rsi_max") and latest_technical.rsi_14 and latest_technical.rsi_14 > criteria["rsi_max"]:
                    meets_criteria = False
                    
                # D/E 비율 기준 확인
                if criteria.get("debt_to_equity_max") and latest_fundamental.debt_to_equity and latest_fundamental.debt_to_equity > criteria["debt_to_equity_max"]:
                    meets_criteria = False
                    
                # 유동비율 기준 확인
                if criteria.get("current_ratio_min") and latest_fundamental.current_ratio and latest_fundamental.current_ratio < criteria["current_ratio_min"]:
                    meets_criteria = False
                    
                # 골든 크로스 확인 (선택적)
                if criteria.get("check_golden_cross") and not latest_technical.golden_cross:
                    meets_criteria = False
                    
                # 기준 충족 시 기회 목록에 추가
                if meets_criteria:
                    # 최신 가격 데이터 조회
                    latest_price = self.db.query(StockPrice).filter(
                        StockPrice.stock_id == stock.id
                    ).order_by(StockPrice.date.desc()).first()
                    
                    # 지지선/저항선 조회
                    support_resistance = self.db.query(SupportResistance).filter(
                        SupportResistance.stock_id == stock.id
                    ).order_by(SupportResistance.strength.desc()).all()
                    
                    # 가장 가까운 지지선 찾기
                    nearest_support = None
                    if latest_price and support_resistance:
                        for sr in support_resistance:
                            if sr.is_support and sr.price_level < latest_price.close:
                                nearest_support = sr.price_level
                                break
                                
                    # 가장 가까운 저항선 찾기
                    nearest_resistance = None
                    if latest_price and support_resistance:
                        for sr in support_resistance:
                            if not sr.is_support and sr.price_level > latest_price.close:
                                nearest_resistance = sr.price_level
                                break
                    
                    opportunities.append({
                        "symbol": stock.symbol,
                        "company_name": stock.company_name,
                        "sector": stock.sector,
                        "latest_price": latest_price.close if latest_price else None,
                        "latest_date": latest_price.date if latest_price else None,
                        "rsi": latest_technical.rsi_14,
                        "debt_to_equity": latest_fundamental.debt_to_equity,
                        "current_ratio": latest_fundamental.current_ratio,
                        "nearest_support": nearest_support,
                        "nearest_resistance": nearest_resistance,
                        "potential_gain": ((nearest_resistance - latest_price.close) / latest_price.close * 100) if nearest_resistance and latest_price else None,
                        "potential_loss": ((latest_price.close - nearest_support) / latest_price.close * 100) if nearest_support and latest_price else None,
                        "risk_reward_ratio": ((nearest_resistance - latest_price.close) / (latest_price.close - nearest_support)) if nearest_resistance and nearest_support and latest_price else None
                    })
                    
            # 리스크/리워드 비율로 정렬 (높은 순)
            opportunities.sort(key=lambda x: x.get("risk_reward_ratio", 0) if x.get("risk_reward_ratio") is not None else 0, reverse=True)
            
            return opportunities
            
        except Exception as e:
            logger.error(f"기회 스캔 실패: {str(e)}")
            return []