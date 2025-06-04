INSERT INTO market_indicator_types (code, name, description) VALUES
  ('vix',               'VIX',               'CBOE Volatility Index'),
  ('fear_greed_index',  'Fear & Greed Index','CNN 시장 공포·탐욕 지수')
ON CONFLICT (code) DO NOTHING;

INSERT INTO support_resistance_methods (code, name, description) VALUES
  ('pivot_daily',   'Daily Pivot Point',      '데일리 피벗 포인트 (전일 고가/저가/시가/종가 기반)'),
  ('pivot_weekly',  'Weekly Pivot Point',     '주간 피벗 포인트 (전주 OHLC 기반)'),
  ('vp_daily',      'Volume Profile Daily',   '최근 N일 기준 매물대 (볼륨 프로파일, 단기)'),
  ('vp_longterm',   'Volume Profile Longterm','장기 매물대 (볼륨 프로파일, 5년 기준)')
ON CONFLICT (code) DO NOTHING;

INSERT INTO technical_indicator_types (code, name, description) VALUES
  ('rsi_14', 'RSI(14)', 'Relative Strength Index (14일)'),
  ('macd', 'MACD', 'Moving Average Convergence Divergence'),
  ('macd_signal', 'MACD Signal', 'MACD Signal Line'),
  ('macd_histogram', 'MACD Histogram', 'MACD Histogram (Oscillator)'),
  ('ma_20', 'MA20', '20일 이동평균선'),
  ('ma_50', 'MA50', '50일 이동평균선'),
  ('ma_200', 'MA200', '200일 이동평균선'),
  ('bolinger_upper', 'Bollinger Upper', '볼린저밴드 상단 (20, 2σ)'),
  ('bolinger_middle', 'Bollinger Middle', '볼린저밴드 중심선 (20일 단순이동평균)'),
  ('bolinger_lower', 'Bollinger Lower', '볼린저밴드 하단 (20, 2σ)'),
  ('ppo', 'PPO', 'Percentage Price Oscillator'),
  ('st_ma_gap', 'Short-Term MA Gap', '단기 이동평균선 간 차이 (MA5-MA10 등)'),
  ('lt_ma_gap', 'Long-Term MA Gap', '장기 이동평균선 간 차이 (MA50-MA200 등)'),
  ('macd_gap', 'MACD Gap', 'MACD와 Signal 간 차이'),
  ('st_golden_cross', 'Short-Term Golden Cross', '단기 골든크로스 (예: MA5>MA10)'),
  ('lt_golden_cross', 'Long-Term Golden Cross', '장기 골든크로스 (예: MA50>MA200)'),
  ('macd_golden_cross', 'MACD Golden Cross', 'MACD 골든크로스 (MACD > Signal)')
ON CONFLICT (code) DO NOTHING;

INSERT INTO fundamental_indicator_types (code, name, info_key, description) VALUES
  ('market_cap',     'Market Cap',         'marketCap',          '시가총액'),
  ('pe_ratio',       'PER',                'trailingPE',         '주가수익비율'),
  ('pb_ratio',       'PBR',                'priceToBook',        '주가순자산비율'),
  ('debt_to_equity', 'Debt to Equity',     'debtToEquity',       '부채비율'),
  ('current_ratio',  'Current Ratio',      'currentRatio',       '유동비율'),
  ('quick_ratio',    'Quick Ratio',        'quickRatio',         '당좌비율'),
  ('roe',            'ROE',                'returnOnEquity',     '자기자본이익률'),
  ('roa',            'ROA',                'returnOnAssets',     '총자산이익률'),
  ('eps',            'EPS',                'trailingEps',        '주당순이익'),
  ('revenue',        'Revenue',            'totalRevenue',       '매출액'),
  ('net_income',     'Net Income',         'netIncomeToCommon',  '당기순이익')
ON CONFLICT (code) DO NOTHING;
