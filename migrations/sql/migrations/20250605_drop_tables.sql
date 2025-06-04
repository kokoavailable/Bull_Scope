-- 값 테이블(데이터)부터 삭제
DROP TABLE IF EXISTS market_indicators;
DROP TABLE IF EXISTS support_resistance;
DROP TABLE IF EXISTS technical_indicators;
DROP TABLE IF EXISTS fundamental_indicators;
DROP TABLE IF EXISTS stock_prices;

-- 메타/타입 테이블
DROP TABLE IF EXISTS market_indicator_types;
DROP TABLE IF EXISTS support_resistance_methods;
DROP TABLE IF EXISTS technical_indicator_types;
DROP TABLE IF EXISTS fundamental_indicator_types;

-- 메인 참조 테이블
DROP TABLE IF EXISTS stocks;