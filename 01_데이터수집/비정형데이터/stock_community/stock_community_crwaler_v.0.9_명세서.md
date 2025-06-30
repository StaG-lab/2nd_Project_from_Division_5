## 코드 명세서: stock_community_crwaler_v.0.9.py

### 1. 개요

- **프로그램명**: `stock_community_crwaler_v.0.9.py`
- **버전**: 0.9
- **목적**: 지정된 CSV 파일에 명시된 주식 종목들에 대해, 정의된 기간 내의 금융 토론방 게시글과 댓글을 병렬로 크롤링하여 CSV 파일로 저장하는 자동화 스크립트.
- **주요 기능**:
  - CSV 파일을 이용한 크롤링 대상 목록 관리 (종목, 기간, 테마 정보)
  - 명령어 인자(CLI)를 통한 대상 필터링 (`or`, `and` 논리) 및 동시 작업자 수 조절
  - 날짜 기반의 효율적인 대상 페이지 탐색 (유추 탐색 + 정밀 탐색)
  - Selenium과 ThreadPoolExecutor를 이용한 병렬 크롤링
  - 게시글 상세 정보(제목, 내용, 작성자, 조회수, 공감/비공감 수, 댓글 등) 수집
  - 봇 탐지 회피를 위한 랜덤 지연 시간 적용
  - 결과를 종목별 또는 전체 통합 CSV 파일로 저장
- **사용 기술**: Python, Selenium, Pandas, `concurrent.futures`, `argparse`

### 2. 실행 방법

- **명령어 형식**: `python stock_community_crwaler_v.0.9.py [OPTIONS]`
- **인자 (Arguments)**:
  - `-f, --file`: 크롤링 대상 종목 목록이 포함된 CSV 파일 경로. (기본값: `data/stock_list.csv`)
  - `-w, --workers`: 동시에 실행할 스레드(작업자) 수. (기본값: `3`)
  - `-o, --option`: 크롤링 대상을 필터링할 키워드. 공백으로 구분. (예: "20대 이재명")
  - `-l, --logic`: 필터링 키워드 간 논리 연산자. (`or` 또는 `and`, 기본값: `or`)

### 3. 주요 구성 요소

#### 3.1. 전역 설정

- `MAX_PRECISION_SEARCH_PAGES`: 목표 날짜를 찾기 위한 정밀 탐색 시 최대 시도 페이지 수 (무한 루프 방지).
- `RANDOM_DELAY_MIN`, `RANDOM_DELAY_MAX`: 요청 간 랜덤 지연 시간 범위 (초).
- `OUTPUT_DIR`: 결과 CSV 파일이 저장될 디렉토리.

#### 3.2. 유틸리티 함수

- `initialize_driver()`: Headless 모드의 Chrome WebDriver 인스턴스를 생성하고 초기화. 사용자 에이전트 설정 및 프록시 지정을 지원.
- `apply_random_delay()`: 설정된 범위 내에서 랜덤한 시간 동안 대기하여 서버의 봇 탐지를 우회.
- `parse_article_date()`: 'YYYY.MM.DD HH:MM' 형식의 문자열에서 날짜 부분만 파싱하여 `datetime.date` 객체로 변환.
- `get_total_pages_from_driver()`: 게시판의 '맨뒤' 버튼 링크에서 전체 페이지 수를 추출.
- `get_current_page_number()`: 현재 WebDriver가 보고 있는 페이지의 URL에서 페이지 번호를 추출.
- `click_element_by_selector()`: CSS 선택자를 이용해 웹 요소를 찾아 클릭.
- `scrape_article_details()`: 개별 게시글 URL에 접속하여 제목, 본문, 댓글 등 상세 정보를 스크랩.
- `page_move_by_list_button()`: **(핵심 제약사항 함수)** URL 직접 조작이 아닌, 게시글 상세 페이지를 경유하여 목표 페이지로 이동하는 우회 로직을 수행.
  1. 현재 페이지의 게시글 목록에서 랜덤한 게시글의 상세 페이지로 이동.
  2. 해당 URL의 `page` 파라미터를 목표 페이지 번호로 수정한 뒤, 해당 URL로 재접속.
  3. 이동된 게시글 상세 페이지 내의 '목록' 버튼을 클릭하여 목표하던 페이지의 게시판 목록으로 돌아옴.

#### 3.3. 핵심 크롤링 로직

- `scrape_stock_articles_by_date_range(stock_data, proxy)`: **[작업자 함수]** 개별 스레드에서 단일 종목의 크롤링 작업을 수행.
  1. **초기화**: WebDriver를 생성하고 목표 종목의 정보(코드, 날짜 등)를 설정.
  2. **전체 범위 파악**: 게시판의 1페이지와 마지막 페이지에 접근하여 가장 최신/오래된 게시글의 날짜와 총 페이지 수를 파악.
  3. **목표 페이지 탐색 (2단계)**:
     - **1단계 (유추 탐색)**: 전체 게시판의 날짜 범위와 목표 시작 날짜의 상대적 위치를 계산하여 크롤링을 시작할 페이지를 **추론**하고 이동. 거리가 멀수록 큰 폭으로, 가까울수록 작은 폭으로 이동하며 빠르게 목표 지점에 근접.
     - **2단계 (정밀 탐색)**: 유추 탐색으로 목표 날짜 근처에 도달하면, 한 페이지씩 순차적으로 이동하며 목표 `end_date`가 포함된 정확한 페이지를 찾음.
  4. **게시글 순차 크롤링**:
     - 찾아낸 시작 페이지부터 1페이지씩 증가하며 크롤링 진행.
     - 각 페이지의 게시글 목록을 순회하며 게시글 날짜를 확인.
     - 날짜가 `start_date`와 `end_date` 사이에 있을 경우에만 `scrape_article_details`를 호출하여 상세 정보를 수집.
     - 게시글 날짜가 `start_date`보다 오래되면 해당 종목의 크롤링을 종료.
  5. **데이터 저장 및 종료**: 수집된 데이터는 주기적으로 종목별 CSV 파일에 추가 저장되며, 작업 완료 후 WebDriver 리소스를 정리.

#### 3.4. 데이터 처리 및 관리

- `load_theme_stock_list()`: 입력받은 CSV 파일을 Pandas DataFrame으로 로드하고, 날짜 컬럼을 `datetime` 형식으로 변환.
- `filter_stock_list_or()`, `filter_stock_list_and()`: `load_theme_stock_list`에서 로드한 DataFrame을 사용자가 입력한 필터링 옵션과 논리에 따라 필터링.
- `save_to_csv()`: 수집된 데이터를 리스트 형태로 받아 DataFrame으로 변환 후, 지정된 경로에 CSV 파일로 저장. 파일이 이미 존재할 경우 데이터를 이어 붙임(append).

#### 3.5. 메인 실행부

- `main()`:
  1. `argparse`를 통해 커맨드 라인 인자를 파싱.
  2. `load_theme_stock_list`를 호출하여 크롤링 대상 목록을 준비하고 필터링.
  3. `ThreadPoolExecutor`를 생성하여 지정된 `workers` 수만큼의 스레드 풀을 구성.
  4. 필터링된 각 종목에 대해 `scrape_stock_articles_by_date_range` 함수를 작업으로 제출(submit).
  5. 모든 스레드의 작업이 완료될 때까지 대기하고, 최종 결과를 취합하여 요약 정보를 출력.
