-- 주식 정보 테이블
CREATE TABLE IF NOT EXISTS stocks (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR UNIQUE NOT NULL,
    company_name VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    country VARCHAR,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 주가 정보 테이블
CREATE TABLE IF NOT EXISTS stock_prices (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    adj_close FLOAT,
    volume INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

-- 재무 지표 종류
CREATE TABLE IF NOT EXISTS fundamental_indicator_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR UNIQUE NOT NULL,      -- ex: 'market_cap', 'pe_ratio', ...
    name VARCHAR NOT NULL,             -- ex: 'Market Cap', 'PER', ...
    description TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 재무 지표 값

CREATE TABLE IF NOT EXISTS stock_fundamental_indicators (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    indicator_type_id INT NOT NULL REFERENCES fundamental_indicator_types(id),
    value FLOAT,                         -- 재무 지표 값 (숫자형)
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date, indicator_type_id)
);


-- 기술적 지표 종류 테이블
CREATE TABLE IF NOT EXISTS technical_indicator_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR UNIQUE NOT NULL,        -- ex: 'rsi_14', 'macd', 'ma_5'
    name VARCHAR NOT NULL,
    description TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 기술적 지표 값 테이블
CREATE TABLE IF NOT EXISTS stock_technical_indicators (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT NOT NULL REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    indicator_type_id INT NOT NULL REFERENCES technical_indicator_types(id),
    value FLOAT,                         -- 지표 값 (숫자형)
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date, indicator_type_id)
);

-- 기술적 지표 테이블
CREATE TABLE IF NOT EXISTS stock_technicals (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    rsi_14 FLOAT,
    macd FLOAT,
    macd_signal FLOAT,
    macd_histogram FLOAT,
    ma_5 FLOAT,
    ma_10 FLOAT
    ma_20 FLOAT,
    ma_50 FLOAT,
    ma_200 FLOAT,
    bolinger_upper FLOAT,
    bolinger_middle FLOAT,
    bolinger_lower FLOAT,
    ppo FLOAT,
    st_ma_golden_cross BOOLEAN,
    lt_ma_golden_cross BOOLEAN,
    macd_golden_cross BOOLEAN,
    st_ma_golden_gap FLOAT,
    lt_ma_golden_gap FLOAT,
    macd_golden_gap FLOAT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(stock_id, date)
);

-- 저항선/지지선 정보 테이블
CREATE TABLE IF NOT EXISTS support_resistance (
    id BIGSERIAL PRIMARY KEY,
    stock_id BIGINT REFERENCES stocks(id) ON DELETE CASCADE,
    date DATE NOT NULL,
    price_level FLOAT NOT NULL,
    is_support BOOLEAN DEFAULT TRUE,
    strength INTEGER DEFAULT 1,
    method VARCHAR NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    unique (stock_id, date, price_level, is_support, method)
);

-- 시장 지표 종류 테이블
CREATE TABLE IF NOT EXISTS indicator_types (
    id SERIAL PRIMARY KEY,
    code VARCHAR UNIQUE NOT NULL,       -- ex: 'vix', 'fear_greed_index', 'market_trend'
    name VARCHAR NOT NULL,              -- ex: 'VIX', 'Fear & Greed Index', 'Market Trend'
    description TEXT,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 시장 지표 값 테이블

CREATE TABLE IF NOT EXISTS market_indicators (
    id BIGSERIAL PRIMARY KEY,
    date DATE NOT NULL,
    indicator_type_id INT NOT NULL REFERENCES indicator_types(id),
    value VARCHAR,                      -- 다양한 타입을 저장하고 싶으면 VARCHAR가 유연함(숫자도 문자로 저장)
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, indicator_type_id)
);
