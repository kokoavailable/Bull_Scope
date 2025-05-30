![ChatGPT Image May 28, 2025, 10_45_28 PM](https://github.com/user-attachments/assets/0bde5d2b-866f-449b-82f4-fd1b5b658deb)


# BullScope - 단기 급등주 검색 시스템

BullScope는 기술적 지표와 재무 지표를 활용하여 단기 급등 가능성이 있는 종목을 찾아주는 시스템입니다.

## 주요 기능

- **기술적 저평가 종목 필터링**: 기술적 저평가 종목 필터링
- **재무 건전성 분석**: Solvency, Liqudity, Propitability 등 필터링
- **추세 전환 모멘텀 분석**: MACD 골든 크로스를 통한 중단기 모멘텀 전환 기회 포착
- **매물대 분석**: 지지선/저항선 분석을 통한 매매 타점 제공
- **시장 심리 지표**: VIX, 공포/탐욕 지수 등 시장 심리 지표 제공
- **주요 매크로 뉴스**: 시장에 영향을 미치는 주요 뉴스 스크랩

## 기술 스택

- **Backend**: Python, FastAPI
- **Database**: PostgreSQL
- **Data Processing**: Pandas, NumPy
- **Data Source**: yfinance, Web Scraping (BeautifulSoup, Selenium)
- **Deployment**: Docker, Docker Compose

## 설치 및 실행 방법

### 필요 조건

- Docker 및 Docker Compose 설치
- Python 3.12+

### 설치 방법

1. 저장소 클론
   ```bash
   git clone https://github.com/yourusername/bullscope.git
   cd bullscope
   ```

2. Docker Compose를 사용하여 실행
   ```bash
   docker-compose up -d
   ```

3. API 접속
   ```
   http://localhost:8000/docs
   ```

### 수동 설치 (Docker 없이)

1. 필요한 패키지 설치
   ```bash
   pip install -r requirements.txt
   ```

2. PostgreSQL 데이터베이스 설정
   ```
   데이터베이스 이름: bullscope
   사용자: postgres
   비밀번호: password
   ```

3. 애플리케이션 실행
   ```bash
   uvicorn app.main:app --reload
   ```

## API 엔드포인트

### 종목 관련

- `GET /api/v1/stocks` - 종목 목록 조회
- `GET /api/v1/stocks/{symbol}/prices` - 특정 종목 가격 데이터 조회
- `GET /api/v1/stocks/{symbol}/analysis` - 특정 종목 분석
- `POST /api/v1/stocks/opportunities` - 투자 기회 찾기

### 시장 관련

- `GET /api/v1/market/analysis` - 시장 분석 정보 조회

### 관리자 기능

- `POST /api/v1/admin/update-stock/{symbol}` - 특정 종목 데이터 업데이트
- `POST /api/v1/admin/update-all-stocks` - 모든 종목 데이터 업데이트
- `POST /api/v1/admin/update-market` - 시장 데이터 업데이트

## 프로젝트 구조

```
bullscope/
├── app/
│   ├── __init__.py
│   ├── main.py                  # FastAPI main 애플리케이션
│   ├── models/
│   │   ├── __init__.py
│   │   ├── database.py          # 데이터베이스 연결 설정
│   │   └── schemas.py           # Pydantic 모델 및 스키마
│   ├── routers/
│   │   ├── __init__.py
│   │   └── stocks.py            # API 엔드포인트
│   └── services/
│       ├── __init__.py
│       ├── data_collector.py    # yfinance로 데이터 수집
│       ├── technical_analyzer.py # 기술적 분석 도구 (RSI, MACD 등)
│       ├── fundamental_analyzer.py # 재무 분석 도구 (D/E, 유동비율 등)
│       └── market_analyzer.py   # 시장 심리 분석 (VIX, 공포탐욕지수 등)
├── migrations/                  # 데이터베이스 마이그레이션 파일
├── tests/                       # 테스트 코드
├── .gitignore
├── requirements.txt
├── docker-compose.yml
└── README.md
```

## 향후 개발 계획

- 웹 프론트엔드 개발 (React + TypeScript)
- 알고리즘 트레이딩 기능 추가
- 머신러닝 모델을 통한 가격 예측
- 실시간 알림 시스템 구현
- 백테스팅 기능 구현

## 라이센스

이 프로젝트는 MIT 라이센스 하에 배포됩니다.

## 에어플로우 환경설정 및 설치

### 로컬

가상환경 설정
python3 -m venv airflow-venv
source airflow-venv/bin/activate

가상환경의 activate 스크립트 수정
echo 'export AIRFLOW_HOME=/Users/koko/bull_Scope/airflow' >> ~/venvs/bull_scope/bin/activate

에어플로우 설치및 포스트그레스 드라이버 (메타데이터용, Local Executer)
pip install "apache-airflow[postgres]"


환경 변수 설정

메타 데이터 디비 미그레이트 
export AIRFLOW_HOME=~/airflow

airflow db migrate

airflow.cfg, airflow.db 파일 생성

load_examples = False
default_task_execution_timeout = 7200
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://postgres:비밀번호@localhost:5432/


옵션 수정. rm -rf airflow.db 제거후 다시 재 migrate. 

에어 플로우 스케줄러 및 api 서버 실행.

pip install flask_appbuilder 

nohup airflow standalone > airflow.log 2>&1 &       

airflow dags reserialize

-- stop 옵션은 디프리케이티드 됨.
pkill -f airflow

웹 ui 접속 http://localhost:8080/

로그 찍기전에 워커 프로세스가 죽는 경우에는 로그 자체가 안찍히는 경우가 많음.
파이썬 프롬프트로 정적 실행해보기.

프롬프트 루트폴더에서

You said:
>>> from src.data_collector import DataCollector
>>> collector = DataCollector()
>>> result = collector.update_all_stocks_parallel(market="US", period="1d", delay=0.1) 
>>> collector.close()

수행 -> 디비 유저 계정 참조 오류 잡아냄.

에어플로우 홈, 파이썬 패스 확인 
echo $AIRFLOW_HOME
echo $PYTHONPATH

echo 'export AIRFLOW_HOME=/Users/koko/bull_scope/airflow' >> ~/.zshrc
echo 'export PYTHONPATH=/Users/koko/bull_scope/airflow' >> ~/.zshrc



## 의존성 설치

포트리 설치
curl -sSL https://install.python-poetry.org | python3 -

포트리 명령어가 설치 안되는경우. 설치확인
ls ~/.local/bin/poetry

설치확인시, PATH 추가
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc

poetry add numpy pandas    # 런타임 패키지 추가
poetry add --dev pytest    # 개발 패키지 추가

poetry install   # 모든 의존성 설치