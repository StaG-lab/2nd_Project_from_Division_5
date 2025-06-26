import pandas as pd
import time
import re
import os
import datetime
import requests
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException, UnexpectedAlertPresentException
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import random 

# --- 상수 및 설정 ---
# 이 예시에서는 유료 프록시 사용을 가정합니다. 실제 프록시 정보로 대체해야 합니다.
# PROXY_LIST = ["http://user:password@ip1:port1", "http://user:password@ip2:port2"] 
PROXY_LIST = [] # 여기에 실제 프록시 주소를 추가하세요. 없으면 프록시 사용 안함.

HEADERS = {"user-agent": "Mozilla/5.0"} 
MAX_WORKERS = 3 
THEME_STOCK_LIST_PATH = 'data/theme_stock_list_with_dates.csv'
CRAWLED_LOG_PATH = 'data/crawled_log.csv' 
RANDOM_DELAY_MIN = 2.0 
RANDOM_DELAY_MAX = 5.0 
MAX_PRECISION_SEARCH_PAGES = 50 # 1페이지 단위 정밀 탐색 시 최대 탐색 페이지 수

# 다양한 User-Agent 목록 (실제 브라우저 User-Agent를 주기적으로 업데이트하는 것이 좋습니다)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/119.0"
]

def initialize_driver(proxy=None):
    """개별 스레드를 위한 새로운 Selenium WebDriver를 초기화하고 반환합니다."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(f'user-agent={random.choice(USER_AGENTS)}') 
    options.add_argument('lang=ko_KR')
    
    # 봇 감지 회피를 위한 옵션 추가
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    if proxy:
        options.add_argument(f'--proxy-server={proxy}')
        print(f"정보: 드라이버에 프록시 설정: {proxy}")
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(40) 
    return driver

def load_theme_stock_list(file_path):
    """테마주 목록 CSV 파일을 로드하고 필요한 컬럼을 반환합니다."""
    if not os.path.exists(file_path):
        print(f"오류: 테마주 목록 파일이 없습니다. '{file_path}' 경로를 확인해주세요.")
        return pd.DataFrame()
    
    df = pd.read_csv(file_path)
    df = df[['election', 'candidate', 'stock_name', 'stock_code', 'category', 'start_date', 'end_date']]
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    print(f"--- 총 {len(df)}개의 테마주 정보를 로드했습니다. ---")
    return df

def load_crawled_log(log_path):
    """이전에 크롤링 완료된 목록을 로드합니다."""
    if os.path.exists(log_path):
        df = pd.read_csv(log_path)
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['end_date'] = pd.to_datetime(df['end_date'])
        crawled_set = set(tuple(row) for row in df[['stock_code', 'start_date', 'end_date']].values)
        print(f"--- {len(crawled_set)}개의 이전 크롤링 기록을 로드했습니다. ---")
        return crawled_set
    return set()

def update_crawled_log(log_path, stock_info):
    """크롤링 완료된 종목 정보를 로그 파일에 추가합니다."""
    new_log_entry = pd.DataFrame([stock_info])
    if not os.path.exists(log_path):
        new_log_entry.to_csv(log_path, index=False, encoding='utf-8-sig')
    else:
        new_log_entry.to_csv(log_path, mode='a', header=False, index=False, encoding='utf-8-sig')

def parse_article_date(date_str_full):
    """게시글 날짜 문자열에서 날짜만 파싱하여 datetime.date 객체로 반환합니다."""
    date_only_str = date_str_full[:10] 
    try:
        return datetime.datetime.strptime(date_only_str, '%Y.%m.%d').date()
    except ValueError:
        return None

def apply_random_delay():
    """랜덤한 시간 동안 대기합니다."""
    delay = random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX)
    time.sleep(delay)

def scrape_article_details(driver, article_url):
    """
    하나의 게시글 URL에 접속하여 본문과 댓글을 스크랩합니다.
    이 함수는 이미 생성된 드라이버를 인자로 받습니다.
    """
    content = ""
    comments = []
    try:
        driver.get(article_url)
        apply_random_delay()
        
        wait = WebDriverWait(driver, 10)
        content_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div#body')))
        content = content_element.text.strip()
        
        comment_elements = driver.find_elements(By.CSS_SELECTOR, 'span.u_cbox_contents')
        comments = [elem.text.strip() for elem in comment_elements]

    except TimeoutException:
        print(f"경고: 게시글 상세 페이지 ({article_url}) 로드 타임아웃.")
    except UnexpectedAlertPresentException as e:
        print(f"오류: 상세 페이지에서 예기치 않은 Alert 발생: {e.alert_text}. 스킵합니다.")
        try:
            driver.switch_to.alert.accept() 
        except:
            pass 
    except Exception as e:
        print(f"오류: 게시글 상세 페이지 ({article_url}) 스크랩 중 예상치 못한 오류 발생: {e}")
    
    return content, comments

def get_current_page_number(driver):
    """현재 드라이버의 URL에서 페이지 번호를 추출합니다."""
    match = re.search(r'page=(\d+)', driver.current_url)
    return int(match.group(1)) if match else 1

def get_total_pages_from_driver(driver):
    """현재 드라이버가 보고 있는 페이지에서 총 페이지 수를 추출합니다."""
    try:
        # 맨뒤 링크가 있다면 맨뒤 페이지 번호를 가져옴
        last_page_link_tag = driver.find_element(By.CSS_SELECTOR, 'td.pgRR a')
        return int(re.search(r'page=(\d+)', last_page_link_tag.get_attribute('href')).group(1))
    except NoSuchElementException:
        # 맨뒤 버튼이 없으면 현재 페이지가 유일한 페이지일 가능성이 높음 (게시글이 적을 때)
        # 이 경우 1페이지로 간주
        return 1
    except Exception as e:
        print(f"경고: 총 페이지 수 가져오기 실패: {e}")
        return 1 # 실패 시 기본값 반환

def click_element_by_selector(driver, element_selector, stock_code, context=""):
    """
    주어진 셀렉터로 요소를 찾아 클릭하고, 페이지가 로드될 때까지 기다립니다.
    성공하면 True, 실패하면 False를 반환합니다. navigate_page_by_click에서 내부적으로 활용
    """
    try:
        wait = WebDriverWait(driver, 10)
        element_to_click = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, element_selector)))
        element_to_click.click()
        apply_random_delay()
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
        return True
    except TimeoutException:
        print(f"경고: 종목 {stock_code}, {context} 클릭 후 로드 타임아웃.")
        return False
    except UnexpectedAlertPresentException as e:
        print(f"오류: {context} 클릭 중 Alert 발생 ({stock_code}): {e.alert_text}. Alert 닫고 실패 처리.")
        try:
            driver.switch_to.alert.accept() 
        except:
            pass 
        return False
    except NoSuchElementException:
        print(f"정보: 종목 {stock_code}, {context} 링크({element_selector})를 찾을 수 없습니다.")
        return False
    except WebDriverException as e:
        print(f"오류: 웹드라이버 문제 발생 ({stock_code}, {context}): {e}. 실패 처리.")
        return False
    except Exception as e:
        print(f"오류: 종목 {stock_code}, {context} 클릭 중 예상치 못한 오류 발생: {e}. 실패 처리.")
        return False

def navigate_page_by_click(driver, target_page_num, stock_code, max_attempts=5):
    """
    지정된 페이지 번호로 이동하기 위해 'table.Nnavi'의 링크를 클릭합니다.
    필요시 '이전'/'다음' 링크를 클릭하여 목표 페이지가 보이도록 합니다.
    """
    current_page_in_url = get_current_page_number(driver)
    if current_page_in_url == target_page_num:
        print(f"정보: 종목 {stock_code}, 이미 목표 페이지 {target_page_num}에 있습니다.")
        apply_random_delay() # 페이지가 이미 로드되어 있을 수 있으므로 대기
        return True

    for attempt in range(max_attempts):
        try:
            # 먼저 정확한 페이지 번호 링크를 찾으려 시도
            # on 클래스가 붙은 현재 페이지 링크는 클릭 대상에서 제외
            page_link_selector = f'table.Nnavi a[href*="page={target_page_num}"]:not(td.on a)'
            wait = WebDriverWait(driver, 5) 
            page_link_element = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, page_link_selector)))
            
            print(f"정보: 종목 {stock_code}, 페이지 {target_page_num} 링크 클릭 시도.")
            page_link_element.click()
            apply_random_delay()
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
            
            if get_current_page_number(driver) == target_page_num:
                print(f"정보: 종목 {stock_code}, 페이지 {target_page_num}으로 성공적으로 이동.")
                return True
            else:
                print(f"경고: 종목 {stock_code}, 페이지 {target_page_num} 클릭 후 예상 페이지 불일치. 재시도 {attempt+1}/{max_attempts}.")
                continue 
        
        except (TimeoutException, NoSuchElementException):
            # 직접 링크를 찾지 못했다면, '이전' 또는 '다음'을 클릭하여 목표 페이지를 보이게 해야 함
            print(f"정보: 종목 {stock_code}, 페이지 {target_page_num} 직접 링크 없음. '이전'/'다음' 페이지 그룹으로 이동 시도.")
            
            current_displayed_pages_text = [a.text.strip() for a in driver.find_elements(By.CSS_SELECTOR, "table.Nnavi td:not(.pgLL):not(.pgL):not(.pgR):not(.pgRR) a")]
            current_displayed_pages = [int(p) for p in current_displayed_pages_text if p.isdigit()]
            
            if not current_displayed_pages: # 페이지네이션이 아예 없을 때
                # 1페이지인 경우 (게시글이 매우 적거나 1페이지가 전부일 때)
                if target_page_num == 1:
                    print(f"정보: 종목 {stock_code}, 페이지네이션이 없으며 목표가 1페이지입니다. 이미 1페이지에 있을 수 있습니다.")
                    driver.get(f"https://finance.naver.com/item/board.naver?code={stock_code}&page=1") # 혹시 모를 상황 대비 1페이지로 강제 이동
                    apply_random_delay()
                    try:
                        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                        if get_current_page_number(driver) == 1:
                            return True
                    except:
                        pass
                print(f"경고: 종목 {stock_code}, 현재 페이지 그룹에서 클릭 가능한 페이지 번호 없음. 크롤링 종료.")
                return False

            min_page_in_group = min(current_displayed_pages)
            max_page_in_group = max(current_displayed_pages)

            if target_page_num < min_page_in_group:
                # 목표 페이지가 현재 보이는 페이지 그룹보다 이전이라면 '이전' 클릭
                if click_element_by_selector(driver, 'td.pgL a', stock_code, "'이전' 페이지 그룹"):
                    continue 
                else:
                    print(f"오류: 종목 {stock_code}, '이전' 페이지 그룹 이동 실패. 크롤링 종료.")
                    return False
            elif target_page_num > max_page_in_group:
                # 목표 페이지가 현재 보이는 페이지 그룹보다 다음이라면 '다음' 클릭
                if click_element_by_selector(driver, 'td.pgR a', stock_code, "'다음' 페이지 그룹"):
                    continue 
                else:
                    print(f"오류: 종목 {stock_code}, '다음' 페이지 그룹 이동 실패. 크롤링 종료.")
                    return False
            else:
                # 목표 페이지가 현재 보이는 페이지 그룹 내에 있으나 클릭 못함 (on 클래스 등으로 인해 클릭 불가)
                print(f"경고: 종목 {stock_code}, 목표 페이지 {target_page_num}가 현재 페이지 그룹 내에 있으나 클릭 불가 (on 클래스 등).")
                # 이 경우는 재시도해도 무의미하므로 바로 실패
                return False 
                
        except UnexpectedAlertPresentException as e:
            print(f"오류: 페이지 이동 중 Alert 발생 ({stock_code}, 페이지 {target_page_num}, 시도 {attempt+1}/{max_attempts}): {e.alert_text}. Alert 닫고 재시도.")
            try:
                driver.switch_to.alert.accept()
            except:
                pass
            if attempt == max_attempts - 1: return False
            apply_random_delay()
        except WebDriverException as e:
            print(f"오류: 웹드라이버 문제 발생 ({stock_code}, 페이지 {target_page_num}, 시도 {attempt+1}/{max_attempts}): {e}. 재시도.")
            if attempt == max_attempts - 1: return False
            apply_random_delay()
        except Exception as e:
            print(f"오류: 종목 {stock_code}, 페이지 이동 중 예상치 못한 오류 발생 ({target_page_num}, 시도 {attempt+1}/{max_attempts}): {e}. 재시도.")
            if attempt == max_attempts - 1: return False
            apply_random_delay()

    print(f"오류: 종목 {stock_code}, 페이지 {target_page_num}으로 이동하는 데 최대 시도 횟수 초과. 크롤링 중단.")
    return False

def find_page_by_date_range_precision(driver, stock_code, start_search_page, start_date, end_date, search_forward=True, max_precision_search_pages=MAX_PRECISION_SEARCH_PAGES):
    """
    주어진 시작 페이지부터 1페이지 단위로 정밀 탐색하여
    실제 크롤링을 시작할 페이지를 찾아 반환합니다.
    search_forward=True: 순방향 (페이지 증가) 탐색 (오래된 글 방향)
    search_forward=False: 역방향 (페이지 감소) 탐색 (최신 글 방향)
    """
    print(f"정보: 종목 {stock_code}, 페이지 {start_search_page}부터 1페이지 단위 정밀 탐색 시작 (방향: {'순방향' if search_forward else '역방향'}).")

    current_page = start_search_page
    total_pages_board = get_total_pages_from_driver(driver)

    for attempt in range(max_precision_search_pages):
        # 유효 페이지 범위 확인
        if current_page < 1:
            print(f"정보: 종목 {stock_code}, 정밀 탐색 중 1페이지 이하로 내려갔습니다. 탐색 종료.")
            return -1
        if current_page > total_pages_board:
            print(f"정보: 종목 {stock_code}, 정밀 탐색 중 총 페이지 수를 초과했습니다. 탐색 종료.")
            return -1

        # 페이지 이동
        if not navigate_page_by_click(driver, current_page, stock_code, max_attempts=3):
            print(f"경고: 종목 {stock_code}, 정밀 탐색 중 페이지 {current_page} 이동 실패. 탐색 종료.")
            return -1

        soup = bs(driver.page_source, 'html.parser')
        posts = soup.select("table.type2 tr[onmouseover]")
        
        if not posts:
            print(f"정보: 종목 {stock_code}, 페이지 {current_page}에 게시글 없음. 다음 페이지로 이동.")
            current_page += (1 if search_forward else -1)
            continue
        
        first_article_date = parse_article_date(posts[0].find_all('td')[0].find('span', class_='tah').text.strip())
        last_article_date = parse_article_date(posts[-1].find_all('td')[0].find('span', class_='tah').text.strip())

        if not first_article_date or not last_article_date:
            print(f"경고: 종목 {stock_code}, 페이지 {current_page}의 날짜 파싱 실패. 다음 페이지로 이동.")
            current_page += (1 if search_forward else -1)
            continue

        # --- 정밀 탐색 종료 조건 및 방향 전환 로직 (핵심 수정 부분) ---
        
        # 1. 현재 페이지가 목표 날짜 범위에 정확히 걸쳐 있는 경우
        # (페이지의 가장 오래된 글이 end_date보다 같거나 이전이고, 가장 최신 글이 start_date보다 같거나 이후인 경우)
        if last_article_date <= end_date and first_article_date >= start_date:
            print(f"정보: 종목 {stock_code}, 페이지 {current_page}에서 목표 날짜 범위({start_date}~{end_date})를 포함하는 페이지 발견. 크롤링 시작 지점 확정.")
            return current_page # 이 페이지부터 크롤링 시작

        # 2. 순방향 탐색 중 (페이지 증가, 즉 점점 오래된 글을 찾아가는 중)
        if search_forward:
            # 현재 페이지의 가장 오래된 글이 목표 시작 날짜보다 **오래된 경우**
            # (즉, 목표 시작점을 지나쳐버렸으므로, 이제 되돌아가야 함)
            if last_article_date < start_date:
                print(f"정보: 종목 {stock_code}, 순방향 정밀 탐색 중 페이지 {current_page}에서 시작 날짜({start_date})보다 오래된 글({last_article_date}) 발견. 역방향 정밀 탐색으로 전환하여 시작 지점 재탐색.")
                # 현재 페이지에서 한 페이지 앞으로 돌아가서 역방향으로 재탐색
                return find_page_by_date_range_precision(driver, stock_code, current_page - 1, start_date, end_date, search_forward=False)
            
            # 아직 목표 시작점까지 도달하지 못했고, 현재 페이지의 글들이 너무 최신인 경우
            # (last_article_date >= start_date 이면서 first_article_date > end_date)
            # 계속 순방향(페이지 증가)으로 더 오래된 글을 찾아 나감
            else:
                print(f"정보: 종목 {stock_code}, 순방향 정밀 탐색 계속. 페이지 {current_page} (날짜: {first_article_date}~{last_article_date}).")
                current_page += 1

        # 3. 역방향 탐색 중 (페이지 감소, 즉 점점 최신 글을 찾아가는 중)
        else: # not search_forward
            # 현재 페이지의 가장 최신 글이 목표 종료 날짜보다 **최신인 경우**
            # (즉, 목표 종료점을 지나쳐버렸으므로, 이제 되돌아가야 함)
            if first_article_date > end_date:
                print(f"정보: 종목 {stock_code}, 역방향 정밀 탐색 중 페이지 {current_page}에서 종료 날짜({end_date})보다 최신 글({first_article_date}) 발견. 순방향 정밀 탐색으로 전환하여 시작 지점 재탐색.")
                # 현재 페이지에서 한 페이지 뒤로 돌아가서 순방향으로 재탐색
                return find_page_by_date_range_precision(driver, stock_code, current_page + 1, start_date, end_date, search_forward=True)
            
            # 아직 목표 종료점까지 도달하지 못했고, 현재 페이지의 글들이 너무 오래된 경우
            # (first_article_date <= end_date 이면서 last_article_date < start_date)
            # 계속 역방향(페이지 감소)으로 더 최신 글을 찾아 나감
            else:
                print(f"정보: 종목 {stock_code}, 역방향 정밀 탐색 계속. 페이지 {current_page} (날짜: {first_article_date}~{last_article_date}).")
                current_page -= 1

    print(f"경고: 종목 {stock_code}, 정밀 탐색 중 최대 페이지 탐색 횟수({max_precision_search_pages}) 초과. 실제 시작 페이지를 찾지 못했습니다.")
    return -1 # 실제 시작 페이지를 찾지 못한 경우


def find_actual_crawl_start_page(driver, stock_code, total_pages, estimated_target_page, start_date, end_date):
    """
    예측 페이지를 기반으로 실제 크롤링을 시작할 페이지를 찾아 반환합니다.
    이 함수는 페이지 그룹 단위 이동과 1페이지 단위 정밀 탐색을 결합합니다.
    그룹 이동 시에도 날짜 범위를 확인하여 적절한 시점에 정밀 탐색으로 전환합니다.
    """
    print(f"정보: 종목 {stock_code}, 목표 날짜 ({start_date} ~ {end_date})를 위한 실제 크롤링 시작 페이지 탐색 시작.")

    current_search_page = get_current_page_number(driver)
    
    # 맨뒤 페이지의 게시글 날짜로 탐색 방향을 미리 결정
    # (이미 total_pages로 이동했으므로 driver는 맨뒤 페이지에 있다고 가정)
    soup = bs(driver.page_source, 'html.parser')
    posts = soup.select("table.type2 tr[onmouseover]")
    
    if posts:
        last_article_on_board_date = parse_article_date(posts[-1].find_all('td')[0].find('span', class_='tah').text.strip())
        first_article_on_board_date = parse_article_date(posts[0].find_all('td')[0].find('span', class_='tah').text.strip())

        if last_article_on_board_date and first_article_on_board_date:
            # 1. 맨뒤 페이지 자체가 목표 날짜 범위에 걸쳐있는 경우
            if last_article_on_board_date <= end_date and first_article_on_board_date >= start_date:
                print(f"정보: 종목 {stock_code}, 맨뒤 페이지({current_search_page})가 목표 날짜 범위({start_date}~{end_date})를 포함합니다. 여기서부터 정밀 탐색 시작.")
                return find_page_by_date_range_precision(driver, stock_code, current_search_page, start_date, end_date, search_forward=False)
            
            # 2. 맨뒤 페이지의 글이 목표 end_date보다 훨씬 최신인 경우 (역방향 탐색)
            elif last_article_on_board_date > end_date:
                print(f"정보: 종목 {stock_code}, 맨뒤 페이지({current_search_page})의 가장 오래된 글({last_article_on_board_date})이 종료 날짜({end_date})보다 최신입니다. 역방향 그룹 탐색 시작.")
                # 현재 맨뒤 페이지부터 역방향 탐색 시작.
                # 아래 로직으로 통합되도록 continue
            
            # 3. 맨뒤 페이지의 글이 목표 start_date보다 훨씬 오래된 경우 (순방향 탐색)
            elif first_article_on_board_date < start_date:
                print(f"정보: 종목 {stock_code}, 맨뒤 페이지({current_search_page})의 가장 최신 글({first_article_on_board_date})이 시작 날짜({start_date})보다 오래되었습니다. 순방향 그룹 탐색 시작.")
                # 1페이지로 이동하여 순방향 탐색 시작.
                if not click_element_by_selector(driver, 'td.pgLL a', stock_code, "맨앞 페이지로 이동"):
                    print(f"오류: 종목 {stock_code}, 맨앞 페이지 이동 실패. 크롤링 종료.")
                    return -1
                current_search_page = get_current_page_number(driver) # 1페이지로 이동했음을 확인
                return find_page_by_date_range_precision(driver, stock_code, current_search_page, start_date, end_date, search_forward=True)
    else:
        print(f"경고: 종목 {stock_code}, 맨뒤 페이지에서 게시글을 찾을 수 없습니다. 크롤링 종료.")
        return -1


    # 그룹 탐색 로직 (순방향 또는 역방향)
    # 현재 드라이버는 맨뒤 페이지에 있거나 (위의 로직에서 continue 된 경우),
    # 또는 1페이지로 이동했을 수 있음.

    # 현재 드라이버가 1페이지에 있는지, 맨뒤 페이지에 있는지 다시 확인
    current_page_at_start_of_group_search = get_current_page_number(driver)
    
    # 1페이지와 맨뒤 중 어디에 가까운지 (혹은 현재 드라이버가 있는 위치에 따라) 탐색 방향 결정
    # 만약 현재 페이지가 맨뒤 근처에 있다면 역방향 탐색 (총 페이지 수에서 빼는 방향)
    if current_page_at_start_of_group_search > total_pages / 2 : # 대략 맨뒤 쪽에 가까움
        print(f"정보: 종목 {stock_code}, 현재 페이지({current_page_at_start_of_group_search})가 맨뒤에 더 가깝습니다. 역방향 그룹 탐색 시작.")
        search_forward = False
    else: # 대략 맨앞 쪽에 가까움
        print(f"정보: 종목 {stock_code}, 현재 페이지({current_page_at_start_of_group_search})가 맨앞에 더 가깝습니다. 순방향 그룹 탐색 시작.")
        search_forward = True
        # 현재 1페이지가 아니라면 1페이지로 이동 (확실한 순방향 시작점)
        if current_page_at_start_of_group_search != 1:
            if not click_element_by_selector(driver, 'td.pgLL a', stock_code, "맨앞 페이지로 이동"):
                print(f"오류: 종목 {stock_code}, 맨앞 페이지 이동 실패. 크롤링 종료.")
                return -1
            current_search_page = get_current_page_number(driver)

    current_search_page = get_current_page_number(driver) # 현재 드라이버가 있는 페이지 번호
    
    # 그룹 이동 루프
    while True:
        soup = bs(driver.page_source, 'html.parser')
        posts = soup.select("table.type2 tr[onmouseover]")
        
        # 현재 페이지에 게시글이 없으면 건너뛰고 다음 그룹으로 이동 시도
        if not posts:
            print(f"정보: 종목 {stock_code}, 페이지 {current_search_page}에 게시글 없음. {'다음' if search_forward else '이전'} 그룹으로 이동 시도.")
            if search_forward:
                if not click_element_by_selector(driver, 'td.pgR a', stock_code, "다음 페이지 그룹으로 이동"):
                    print(f"경고: 종목 {stock_code}, '다음 페이지 그룹' 이동 실패. 현재 페이지부터 정밀 탐색으로 전환.")
                    break
            else:
                if not click_element_by_selector(driver, 'td.pgL a', stock_code, "이전 페이지 그룹으로 이동"):
                    print(f"경고: 종목 {stock_code}, '이전 페이지 그룹' 이동 실패. 현재 페이지부터 정밀 탐색으로 전환.")
                    break
            
            new_page = get_current_page_number(driver)
            if new_page == current_search_page: # 페이지가 이동하지 않았다면 (끝에 도달)
                print(f"정보: 종목 {stock_code}, 그룹 이동 후 페이지 변화 없음. 그룹 이동 종료. 현재 페이지부터 정밀 탐색.")
                break
            current_search_page = new_page
            continue

        first_article_date = parse_article_date(posts[0].find_all('td')[0].find('span', class_='tah').text.strip())
        last_article_date = parse_article_date(posts[-1].find_all('td')[0].find('span', class_='tah').text.strip())

        if not first_article_date or not last_article_date:
            print(f"경고: 종목 {stock_code}, 페이지 {current_search_page}의 날짜 파싱 실패. {'다음' if search_forward else '이전'} 그룹으로 이동 시도.")
            if search_forward:
                if not click_element_by_selector(driver, 'td.pgR a', stock_code, "다음 페이지 그룹으로 이동"):
                    print(f"경고: 종목 {stock_code}, '다음 페이지 그룹' 이동 실패. 현재 페이지부터 정밀 탐색으로 전환.")
                    break
            else:
                if not click_element_by_selector(driver, 'td.pgL a', stock_code, "이전 페이지 그룹으로 이동"):
                    print(f"경고: 종목 {stock_code}, '이전 페이지 그룹' 이동 실패. 현재 페이지부터 정밀 탐색으로 전환.")
                    break
            
            new_page = get_current_page_number(driver)
            if new_page == current_search_page:
                print(f"정보: 종목 {stock_code}, 그룹 이동 후 페이지 변화 없음. 그룹 이동 종료. 현재 페이지부터 정밀 탐색.")
                break
            current_search_page = new_page
            continue

        # --- 그룹 탐색 종료 및 정밀 탐색 전환 조건 (핵심 수정 부분) ---
        # 목표 날짜 범위 (start_date ~ end_date)를 포함하거나 지나친 경우
        # 순방향 탐색 (오래된 날짜 방향으로)
        if search_forward:
            # 현재 페이지의 가장 오래된 글이 목표 start_date보다 이전이거나 (지나침)
            # 현재 페이지의 가장 최신 글이 목표 end_date를 포함하는 경우 (시작 지점 발견)
            if last_article_date < start_date or (last_article_date <= end_date and first_article_date >= start_date):
                print(f"정보: 종목 {stock_code}, 순방향 그룹 이동 중 페이지 {current_search_page}에서 목표 날짜 범위({start_date}~{end_date})에 근접 또는 진입. 정밀 탐색 시작.")
                return find_page_by_date_range_precision(driver, stock_code, current_search_page, start_date, end_date, search_forward=True) # 순방향 정밀 탐색
            # 아직 목표 범위를 지나치지 않고, 더 오래된 글을 찾아 계속 순방향으로 가야 하는 경우
            else:
                print(f"정보: 종목 {stock_code}, 순방향 그룹 이동: 현재 페이지 {current_search_page} (날짜: {first_article_date}~{last_article_date}), 목표 날짜({start_date}~{end_date})보다 최신. 계속 순방향 탐색.")
                if not click_element_by_selector(driver, 'td.pgR a', stock_code, "다음 페이지 그룹으로 이동"):
                    print(f"경고: 종목 {stock_code}, '다음 페이지 그룹' 이동 실패. 현재 페이지부터 정밀 탐색으로 전환.")
                    break
        # 역방향 탐색 (최신 날짜 방향으로)
        else:
            # 현재 페이지의 가장 최신 글이 목표 end_date보다 최신이거나 (지나침)
            # 현재 페이지의 가장 오래된 글이 목표 start_date를 포함하는 경우 (시작 지점 발견)
            if first_article_date > end_date or (last_article_date <= end_date and first_article_date >= start_date):
                print(f"정보: 종목 {stock_code}, 역방향 그룹 이동 중 페이지 {current_search_page}에서 목표 날짜 범위({start_date}~{end_date})에 근접 또는 진입. 정밀 탐색 시작.")
                return find_page_by_date_range_precision(driver, stock_code, current_search_page, start_date, end_date, search_forward=False) # 역방향 정밀 탐색
            # 아직 목표 범위를 지나치지 않고, 더 최신 글을 찾아 계속 역방향으로 가야 하는 경우
            else:
                print(f"정보: 종목 {stock_code}, 역방향 그룹 이동: 현재 페이지 {current_search_page} (날짜: {first_article_date}~{last_article_date}), 목표 날짜({start_date}~{end_date})보다 오래됨. 계속 역방향 탐색.")
                if not click_element_by_selector(driver, 'td.pgL a', stock_code, "이전 페이지 그룹으로 이동"):
                    print(f"경고: 종목 {stock_code}, '이전 페이지 그룹' 이동 실패. 현재 페이지부터 정밀 탐색으로 전환.")
                    break
        
        new_page = get_current_page_number(driver)
        if new_page == current_search_page: # 페이지가 이동하지 않았다면 (끝에 도달)
            print(f"정보: 종목 {stock_code}, 그룹 이동 후 페이지 변화 없음. 그룹 이동 종료. 현재 페이지부터 정밀 탐색.")
            break
        current_search_page = new_page
        print(f"정보: 종목 {stock_code}, 그룹 이동: 현재 페이지 {current_search_page}")

    # 그룹 탐색 루프를 빠져나왔으나 아직 정확한 시작점을 찾지 못한 경우 (예: 페이지 그룹이 없음)
    # 현재 페이지부터 정밀 탐색 시도
    print(f"정보: 종목 {stock_code}, 그룹 탐색 완료 또는 중단. 현재 페이지 {current_search_page}부터 정밀 탐색 시도.")
    return find_page_by_date_range_precision(driver, stock_code, current_search_page, start_date, end_date, search_forward=search_forward)



def scrape_stock_articles_by_date_range(stock_data, proxy=None): 
    """
    [작업자 함수] 특정 종목에 대해 지정된 날짜 범위 내의 게시글을 크롤링합니다.
    각 스레드에서 독립적으로 실행됩니다.
    """
    driver = initialize_driver(proxy) 
    all_articles_data = []

    stock_code = stock_data['stock_code']
    stock_name = stock_data['stock_name']
    start_date = stock_data['start_date'].date() 
    end_date = stock_data['end_date'].date()   
    election = stock_data['election']
    candidate = stock_data['candidate']
    category = stock_data['category']

    print(f"정보: 종목 {stock_code} 크롤링 시작. 목표 날짜: {start_date} ~ {end_date}.")
    
    try:
        # --- 1단계: 초기 페이지 로드 후 '맨뒤' 페이지로 이동 시도 및 전체 페이지 수 획득 ---
        initial_list_url = f"https://finance.naver.com/item/board.naver?code={stock_code}&page=1"
        
        retries = 3 
        total_pages = 1 
        
        for i in range(retries):
            try:
                driver.get(initial_list_url)
                apply_random_delay()
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                
                # 총 페이지 수 획득
                total_pages = get_total_pages_from_driver(driver)
                print(f"정보: 종목 {stock_code} 총 페이지 수: {total_pages}")

                # 맨뒤 페이지로 이동 시도 (가장 오래된 게시글이 있는 곳)
                if total_pages > 1:
                    print(f"정보: 종목 {stock_code}, '맨뒤' 페이지({total_pages})로 이동 시도.")
                    if not navigate_page_by_click(driver, total_pages, stock_code, max_attempts=5):
                        print(f"오류: 종목 {stock_code}, '맨뒤' 페이지 클릭 이동 실패. 크롤링 종료.")
                        return all_articles_data
                else:
                    print(f"정보: 종목 {stock_code}, 게시글이 1페이지 밖에 없거나 없음. 1페이지에서 시작합니다.")
                
                # 현재 페이지 (맨뒤 페이지 또는 1페이지)의 게시글이 목표 날짜보다 최신인지 확인
                soup_current = bs(driver.page_source, 'html.parser')
                posts_current = soup_current.select("table.type2 tr[onmouseover]")

                if not posts_current:
                    print(f"정보: 종목 {stock_code}, 현재 페이지에 게시글이 없습니다. 크롤링 종료.")
                    return all_articles_data

                last_article_on_board_date = parse_article_date(posts_current[-1].find_all('td')[0].find('span', class_='tah').text.strip())
                if not last_article_on_board_date:
                    print(f"경고: 종목 {stock_code}, 현재 페이지의 날짜 파싱 실패. 크롤링 종료.")
                    return all_articles_data

                if last_article_on_board_date > end_date:
                    print(f"정보: 종목 {stock_code}, 맨뒤 페이지의 가장 오래된 글({last_article_on_board_date})이 지정된 종료 날짜({end_date})보다 최신입니다. 해당 기간의 글이 없어 크롤링을 중단합니다.")
                    return all_articles_data
                
                break # 초기 페이지 정보 가져오기 성공 시 루프 탈출

            except UnexpectedAlertPresentException as e:
                print(f"오류: 초기 페이지 로드 중 Alert 발생 ({stock_code}, 시도 {i+1}/{retries}): {e.alert_text}. Alert 닫고 재시도.")
                try: driver.switch_to.alert.accept() 
                except: pass 
                if i < retries - 1 and PROXY_LIST: driver.quit(); driver = initialize_driver(proxy) 
                else: print(f"오류: 종목 {stock_code}, 초기 페이지 로드 재시도 실패. 크롤링 종료."); return all_articles_data
            except TimeoutException:
                print(f"경고: 종목 {stock_code}, 초기 페이지 로드 타임아웃 (시도 {i+1}/{retries}). 재시도.")
                if i < retries - 1 and PROXY_LIST: driver.quit(); driver = initialize_driver(proxy)
                else: print(f"오류: 종목 {stock_code}, 초기 페이지 로드 타임아웃 실패. 크롤링 종료."); return all_articles_data
            except WebDriverException as e:
                print(f"오류: 웹드라이버 문제 발생 ({stock_code}, 초기 페이지, 시도 {i+1}/{retries}): {e}. 재시도.")
                if i < retries - 1 and PROXY_LIST: driver.quit(); driver = initialize_driver(proxy)
                else: print(f"오류: 종목 {stock_code}, 웹드라이버 문제 해결 실패. 크롤링 종료."); return all_articles_data
            except Exception as e:
                print(f"오류: 종목 {stock_code}, 초기 페이지 로드 중 예상치 못한 오류 발생 (시도 {i+1}/{retries}): {e}. 재시도.")
                if i < retries - 1 and PROXY_LIST: driver.quit(); driver = initialize_driver(proxy)
                else: print(f"오류: 종목 {stock_code}, 예상치 못한 오류 해결 실패. 크롤링 종료."); return all_articles_data
        
        # 맨뒤 페이지까지 확인 후에도 목표 날짜 범위를 벗어난다면, 크롤링할 데이터가 없다고 판단
        # 이 부분은 find_actual_crawl_start_page 내에서 더 자세히 처리되므로 중복될 수 있음.
        # find_actual_crawl_start_page가 -1을 반환하면 여기서 처리되도록
        
        # --- 2. 실제 크롤링 시작 지점 탐색 및 이동 ---
        # estimated_target_page는 이제 find_actual_crawl_start_page 내부에서만 사용되는 개념에 더 가까움
        # 여기서는 단순히 총 페이지 수와 목표 날짜를 넘겨주어 최적의 시작 페이지를 찾도록 함
        actual_start_crawl_page = find_actual_crawl_start_page(driver, stock_code, total_pages, get_current_page_number(driver), start_date, end_date) # 현재 드라이버 위치 전달
        
        if actual_start_crawl_page == -1:
            print(f"정보: 종목 {stock_code}, 실제 크롤링 시작 페이지를 찾을 수 없습니다. 크롤링 종료.")
            return all_articles_data
        
        # 실제 데이터 크롤링은 항상 찾은 페이지부터 역방향 (페이지 번호 감소)으로 진행
        current_page = actual_start_crawl_page 

        # --- 3단계: 실제 데이터 크롤링 (역방향으로 1페이지씩) ---
        print(f"정보: 종목 {stock_code} 실제 데이터 크롤링 시작 (시작 페이지: {current_page}, 목표 시작 날짜: {start_date}).")
        
        while current_page > 0: 
            if not navigate_page_by_click(driver, current_page, stock_code, max_attempts=5):
                print(f"오류: 종목 {stock_code}, 페이지 {current_page} 이동 실패 (데이터 크롤링). 크롤링 종료.")
                break # 페이지 이동 실패 시 크롤링 중단
            
            soup = bs(driver.page_source, 'html.parser')
            posts = soup.select("table.type2 tr[onmouseover]")

            if not posts:
                print(f"정보: 종목 {stock_code}, 페이지 {current_page}에 게시글이 없습니다. 이전 페이지로 이동.")
                current_page -= 1
                continue 

            page_finished_crawling = False
            for post in posts:
                tds = post.find_all('td')
                if len(tds) < 6:
                    continue

                article_date_str_full = tds[0].find('span', class_='tah').text.strip()
                article_date = parse_article_date(article_date_str_full)
                
                if not article_date: 
                    continue

                if article_date < start_date:
                    print(f"정보: 종목 {stock_code}, 페이지 {current_page}에서 {start_date} 이전 날짜({article_date}) 게시글 발견. 해당 종목 크롤링 종료.")
                    page_finished_crawling = True
                    break 

                if start_date <= article_date <= end_date:
                    title_tag = tds[1].find('a')
                    if not title_tag or 'title' not in title_tag.attrs:
                        continue
                    
                    title = title_tag['title']
                    article_url = "https://finance.naver.com" + title_tag['href']
                    
                    nickname = tds[2].text.strip()
                    views = int(tds[3].text.strip().replace(',', ''))
                    likes = int(tds[4].text.strip().replace(',', ''))
                    dislikes = int(tds[5].text.strip().replace(',', ''))
                    
                    content, comments = scrape_article_details(driver, article_url)

                    all_articles_data.append({
                        'stock_name': stock_name,
                        'stock_code': stock_code,
                        'article_date': article_date_str_full[:10], 
                        'article_title': title,
                        'article_nickname': nickname,
                        'article_content': content,
                        'article_comments': " | ".join(comments), 
                        'article_viewers': views,
                        'article_up': likes,
                        'article_down': dislikes,
                        'article_URL': article_url,
                        'vote_election': election,
                        'vote_candidate': candidate,
                        'vote_category': category,
                        'vote_start_date': stock_data['start_date'], 
                        'vote_end_date': stock_data['end_date']     
                    })
                    # 상세 내용 본 후 현재 목록 페이지로 복귀
                    driver.get(driver.current_url) 
                    apply_random_delay() 
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
            
            if page_finished_crawling: 
                break
            
            current_page -= 1 

    except Exception as e:
        print(f"오류: 종목 {stock_code} 처리 중 예상치 못한 오류 발생: {e}")
    finally:
        driver.quit() 
        
    return all_articles_data

if __name__ == "__main__":
    start_time = time.time()
    
    theme_stocks_df = load_theme_stock_list(THEME_STOCK_LIST_PATH)
    theme_stocks_df = theme_stocks_df[75:78]
    if theme_stocks_df.empty:
        print("크롤링을 시작할 테마주 목록이 없습니다. 프로그램을 종료합니다.")
        exit()

    crawled_log = load_crawled_log(CRAWLED_LOG_PATH)

    tasks_to_crawl = []
    print("크롤링할 작업 목록을 생성 중입니다 (중복 제외)...")
    for _, row in theme_stocks_df.iterrows():
        stock_code = row['stock_code']
        start_date = row['start_date']
        end_date = row['end_date']
        
        if (stock_code, start_date, end_date) in crawled_log:
            print(f"정보: 종목 {stock_code} (시작: {start_date.strftime('%Y-%m-%d')}, 종료: {end_date.strftime('%Y-%m-%d')})은(는) 이미 크롤링되었습니다. 스킵합니다.")
            continue
        tasks_to_crawl.append(row.to_dict()) 
            
    print(f"총 {len(tasks_to_crawl)}개의 종목-날짜 범위에 대해 크롤링을 시작합니다.")

    all_results = []
    
    num_proxies = len(PROXY_LIST)
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_stock = {}
        for i, stock_data in enumerate(tasks_to_crawl):
            proxy = PROXY_LIST[i % num_proxies] if num_proxies > 0 else None
            future = executor.submit(scrape_stock_articles_by_date_range, stock_data, proxy)
            future_to_stock[future] = stock_data
        
        progress = tqdm(as_completed(future_to_stock), total=len(tasks_to_crawl), desc="종목 크롤링 진행률")
        
        for future in progress:
            stock_data_info = future_to_stock[future]
            try:
                result_list = future.result()
                if result_list:
                    all_results.extend(result_list)
                    update_crawled_log(CRAWLED_LOG_PATH, {
                        'stock_code': stock_data_info['stock_code'],
                        'start_date': stock_data_info['start_date'].strftime('%Y-%m-%d'),
                        'end_date': stock_data_info['end_date'].strftime('%Y-%m-%d')
                    })
            except Exception as exc:
                print(f'{stock_data_info["stock_code"]} 작업 중 오류 발생: {exc}')

    if not all_results:
        print("수집된 데이터가 없습니다.")
    else:
        final_df = pd.DataFrame(all_results)
        
        data_dir = 'data'
        os.makedirs(data_dir, exist_ok=True)
        
        current_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = os.path.join(data_dir, f'naver_stock_community_themed_{current_date}.csv')
        
        final_df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        print("\n--- 최종 크롤링 결과 ---")
        print(f"총 {len(final_df)}개의 게시글을 수집했습니다.")
        print(f"최종 데이터 저장 완료: {filepath}")
        print(final_df.head())

    end_time = time.time()
    print(f"\n총 실행 시간: {end_time - start_time:.2f}초")