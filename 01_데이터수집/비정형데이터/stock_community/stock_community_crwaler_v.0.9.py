import datetime
import re
import random
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, UnexpectedAlertPresentException
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor
import argparse


# 전역 설정 (필요에 따라 config 파일로 분리 가능)
MAX_PRECISION_SEARCH_PAGES = 100 # 정밀 탐색 최대 시도 페이지 수 (무한 루프 방지)
RANDOM_DELAY_MIN = 0.3 # 최소 랜덤 지연 시간 (초)
RANDOM_DELAY_MAX = 1.9 # 최대 랜덤 지연 시간 (초)
OUTPUT_DIR = 'output'


# --- 유틸리티 함수 ---
def initialize_driver(proxy=None):
    """WebDriver를 초기화하고 반환합니다."""
    options = webdriver.ChromeOptions()
    options.add_argument("headless") # GUI 없이 실행
    options.add_argument("window-size=1920x1080")
    options.add_argument("disable-gpu")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    options.add_argument("lang=ko_KR")
    options.add_argument("no-sandbox")
    options.add_argument("disable-dev-shm-usage")
    if proxy:
        options.add_argument(f'--proxy-server={proxy}')

    # 드라이버 설치 관리자를 사용하여 ChromeDriver를 자동으로 다운로드 및 설치
    driver = webdriver.Chrome(options=options)
    print(f"정보: 드라이버 초기화 완료. (프록시: {proxy if proxy else '없음'})")
    return driver

def apply_random_delay():
    """랜덤한 시간 동안 지연을 적용하여 봇 감지를 회피합니다."""
    delay = random.uniform(RANDOM_DELAY_MIN, RANDOM_DELAY_MAX)
    time.sleep(delay)

def parse_article_date(date_str_full):
    """게시글 날짜 문자열에서 날짜만 파싱하여 datetime.date 객체로 반환합니다."""
    date_only_str = date_str_full[:10] 
    try:
        return datetime.datetime.strptime(date_only_str, '%Y.%m.%d').date()
    except ValueError:
        return None

def get_total_pages_from_driver(driver):
    """
    현재 페이지에서 총 페이지 수를 추출합니다.
    """
    try:
        # 맨 뒤 페이지로 가는 링크의 href 속성에서 'page=' 파라미터 값을 추출
        last_page_link = driver.find_element(By.CSS_SELECTOR, 'td.pgRR a')
        href = last_page_link.get_attribute('href')
        match = re.search(r'page=(\d+)', href)
        if match:
            return int(match.group(1))
    except NoSuchElementException:
        # '맨뒤' 버튼이 없는 경우 (예: 페이지가 하나뿐이거나)
        try:
            # 현재 페이지 번호를 확인 (가장 마지막 페이지 번호가 곧 총 페이지 수)
            current_page_elem = driver.find_element(By.CSS_SELECTOR, 'td.pgON strong')
            return int(current_page_elem.text.strip())
        except NoSuchElementException:
            return 1 # 페이지 번호를 찾을 수 없으면 1페이지로 간주
    except Exception as e:
        print(f"오류: 총 페이지 수 추출 중 오류 발생: {e}")
    return 1 # 기본값

def get_current_page_number(driver):
    """
    현재 드라이버가 위치한 게시판 페이지 번호를 URL에서 추출합니다.
    """
    current_url = driver.current_url
    match = re.search(r'page=(\d+)', current_url)
    if match:
        return int(match.group(1))
    return 1 # 기본값

def click_element_by_selector(driver, css_selector, stock_code, action_desc):
    """
    주어진 CSS 선택자를 사용하여 요소를 클릭하고, 클릭 후 잠시 대기합니다.
    """
    try:
        element = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector))
        )
        element.click()
        apply_random_delay()
        print(f"정보: 종목 {stock_code} - '{action_desc}' 성공.")
        return True
    except TimeoutException:
        print(f"경고: 종목 {stock_code} - '{action_desc}' 버튼 클릭 타임아웃.")
    except NoSuchElementException:
        print(f"경고: 종목 {stock_code} - '{action_desc}' 버튼을 찾을 수 없습니다.")
    except Exception as e:
        print(f"오류: 종목 {stock_code} - '{action_desc}' 클릭 중 오류 발생: {e}")
    return False

def scrape_article_details(driver, article_url):
    """
    주어진 게시글 URL에서 본문과 댓글을 크롤링합니다.
    이 함수는 별도의 드라이버를 초기화하지 않고 기존 드라이버를 사용합니다.
    """
    
    datas = {
        "article_title" : "",
        "article_date" : "",
        "article_nickname" : "",
        "article_viewers" : "",
        "article_likes" : "",
        "article_dislikes" : "",
        "article_content" : "",
        "article_comments" : "",
        "article_url" : article_url,
    }

    # 게시글 상세 페이지로 이동
    try:
        driver.get(article_url)
        apply_random_delay()
        
        wait = WebDriverWait(driver, 10)
        content_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div#body')))
        datas["article_content"] = content_element.text.strip()
        article_title = driver.find_element(By.CSS_SELECTOR, 'strong.c.p15')
        datas["article_title"] = article_title.text.strip()
        article_date = driver.find_element(By.CSS_SELECTOR, 'table.view tbody tr th.tah')
        datas["article_date"] = article_date.text.strip()
        article_viewers = driver.find_element(By.CSS_SELECTOR, 'span.tah.p11')
        datas["article_viewers"] = article_viewers.text.strip()
        article_likes = driver.find_element(By.CSS_SELECTOR, 'table.view tbody tr:nth-child(1) th:nth-child(2) strong._goodCnt')
        datas["article_likes"] = article_likes.text.strip()
        article_dislikes = driver.find_element(By.CSS_SELECTOR, 'table.view tbody tr:nth-child(1) th:nth-child(2) strong._badCnt')
        datas["article_dislikes"] = article_dislikes.text.strip()
        article_nickname = driver.find_element(By.CSS_SELECTOR, 'table.view tbody tr:nth-child(2) th.info span strong')
        datas["article_nickname"] = article_nickname.text.strip()
        comment_elements = driver.find_elements(By.CSS_SELECTOR, 'span.u_cbox_contents')
        comments = [elem.text.strip() for elem in comment_elements]
        datas["article_comments"] = " || ".join(comments)
        
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
    return datas


def page_move_by_list_button(driver, wait, stock_code, page_number, board_mode=False):
    if not board_mode :
        article_links_on_page = driver.find_elements(By.CSS_SELECTOR, "table.type2 tbody tr td.title a")
        if not article_links_on_page:
            print(f"경고: 종목 {stock_code} - 현재 페이지에 게시글이 없습니다. (CSS 선택자 오류 또는 페이지 구조 변경)")
            return []
            
        random_article_link = random.choice(article_links_on_page)
        article_detail_url = random_article_link.get_attribute('href')
        target_board_list_url = re.sub(r'page=\d+', f'page={page_number}', article_detail_url)
        print(f"정보: 종목 {stock_code} - 랜덤 게시글({target_board_list_url})로 이동 시도.")
        driver.get(target_board_list_url)
        apply_random_delay()
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.title_discuss ul li a")))
    
    board_links = driver.find_elements(By.CSS_SELECTOR, "div.title_discuss ul li a")
    if len(board_links) < 2:
        print(f"경고: 종목 {stock_code} - 게시글 상세 페이지에서 게시판 링크를 찾을 수 없습니다. (게시판 링크 CSS 선택자 오류 또는 페이지 구조 변경)")
        return []

    board_list_link_element = board_links[1]
    board_list_link_element.click()
    apply_random_delay()

# --- 메인 크롤링 함수 ---
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
    init_page = 1

    print(f"정보: 종목 {stock_code} 크롤링 시작. 목표 날짜: {start_date} ~ {end_date}.")
    
    initial_list_url = f"https://finance..com/item/board.?code={stock_code}&page={init_page}"

    try:
        driver.get(initial_list_url)
        apply_random_delay()

        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
        try:
            first_article_date_element = driver.find_element(By.CSS_SELECTOR, "span.tah")
            article_latest_date_str = first_article_date_element.text.strip()
            article_latest_date = parse_article_date(article_latest_date_str)
            print(f"정보: 종목 {stock_code} - 1페이지 최신 게시글 날짜: {article_latest_date}")
        except NoSuchElementException:
            print(f"경고: 종목 {stock_code} - 1페이지에서 게시글을 찾을 수 없습니다. URL - {initial_list_url}")
            return []

        last_page = get_total_pages_from_driver(driver)
        print(f"정보: 종목 {stock_code} - 총 페이지 수: {last_page}")

        if last_page > 1:
            if not click_element_by_selector(driver, 'td.pgRR a', stock_code, "맨 뒤 페이지 이동"):
                print(f"오류: 종목 {stock_code} - 맨 뒤 페이지로 이동 실패. 크롤링 중단.")
                return []
            
            current_url_page = get_current_page_number(driver)
            if current_url_page != last_page:
                print(f"경고: 종목 {stock_code} - 맨 뒤 페이지 이동 후 URL 페이지({current_url_page})와 last_page({last_page}) 불일치.")
            
            try:
                last_article_date_element = driver.find_element(By.CSS_SELECTOR, "td span.tah")
                article_oldest_date_str = last_article_date_element.text.strip()
                article_oldest_date = parse_article_date(article_oldest_date_str)
                print(f"정보: 종목 {stock_code} - 마지막 페이지 가장 오래된 게시글 날짜: {article_oldest_date}")
            except NoSuchElementException:
                print(f"경고: 종목 {stock_code} - 마지막 페이지에서 게시글을 찾을 수 없습니다.")
                return []
        else:
            article_oldest_date = article_latest_date
            print(f"정보: 종목 {stock_code} - 총 페이지가 1이므로 최신/오래된 날짜 동일: {article_oldest_date}")

        if end_date > article_latest_date:
            print(f"정보: 종목 {stock_code} - 목표 종료 날짜({end_date})가 최신 게시글 날짜({article_latest_date})보다 미래입니다. 크롤링할 내용이 없습니다.")
            return []
        if end_date < article_oldest_date and last_page > 1:
            print(f"정보: 종목 {stock_code} - 목표 종료 날짜({end_date})가 가장 오래된 게시글 날짜({article_oldest_date})보다 과거입니다. 크롤링할 내용이 없습니다.")
            return []
        
        total_date_range_days = (article_latest_date - article_oldest_date).days
        target_date_from_latest_days = (article_latest_date - end_date).days

        inferred_page_number = init_page 

        if total_date_range_days > 0 and target_date_from_latest_days >= 0:
            inferred_page_number = round(init_page + (last_page - init_page) * (target_date_from_latest_days / total_date_range_days))
            inferred_page_number = max(init_page, min(last_page, inferred_page_number))
            print(f"정보: 종목 {stock_code} - 유추된 시작 페이지: {inferred_page_number}")
        else:
            inferred_page_number = init_page 
        
        try:
            page_move_by_list_button(driver, wait, stock_code, inferred_page_number)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))

            actual_current_page = get_current_page_number(driver)
            if actual_current_page != inferred_page_number:
                print(f"경고: 종목 {stock_code} - 유추 페이지 이동 불일치. 목표: {inferred_page_number}, 실제: {actual_current_page}")
                inferred_page_number = actual_current_page
            
            print(f"정보: 종목 {stock_code} - 현재 탐색 페이지: {actual_current_page}")

        except NoSuchElementException:
            print(f"오류: 종목 {stock_code} - 게시글 링크 또는 게시판 링크를 찾을 수 없습니다. HTML 구조 변경 가능성.")
            return []
        except Exception as e:
            print(f"오류: 종목 {stock_code} - 게시글 상세 페이지 이동/게시판 이동 중 오류 발생: {e}")
            return []

        def get_current_page_date_range(driver_instance):
            try:
                latest_elem = driver_instance.find_element(By.CSS_SELECTOR, "table.type2 tbody tr:nth-child(3) td span.tah")
                oldest_elem = driver_instance.find_element(By.CSS_SELECTOR, "table.type2 tbody tr:nth-last-child(3) td span.tah")
                
                current_page_latest_date = parse_article_date(latest_elem.text.strip())
                current_page_oldest_date = parse_article_date(oldest_elem.text.strip())
                return current_page_latest_date, current_page_oldest_date
            except NoSuchElementException:
                all_dates_on_page_elements = driver_instance.find_elements(By.CSS_SELECTOR, "table.type2 tbody tr td span.tah")
                all_dates_on_page = [parse_article_date(elem.text.strip()) for elem in all_dates_on_page_elements if parse_article_date(elem.text.strip()) is not None]
                if all_dates_on_page:
                    return max(all_dates_on_page), min(all_dates_on_page)
                return None, None

        current_page_latest_date, current_page_oldest_date = get_current_page_date_range(driver)
        if not current_page_latest_date or not current_page_oldest_date:
            print(f"오류: 종목 {stock_code} - 현재 페이지의 날짜 정보를 가져올 수 없습니다. 크롤링 중단.")
            return []

        print(f"정보: 종목 {stock_code} - 현재 페이지({actual_current_page}) 날짜 범위: {current_page_latest_date} ~ {current_page_oldest_date}")

        low_page = 1
        high_page = last_page
        current_page = actual_current_page
        
        search_attempts = 0
            
        distance_history = {            
            stock_code:[abs(0),0]
        }
        while not (current_page_oldest_date <= end_date <= current_page_latest_date):
            search_attempts += 1
            if search_attempts > MAX_PRECISION_SEARCH_PAGES / 2: # 초기 유추 단계에서도 무한루프 방지
                print(f"경고: 종목 {stock_code} - 유추 단계 최대 탐색 시도 횟수({MAX_PRECISION_SEARCH_PAGES / 2}) 초과. 정밀 탐색으로 전환하거나 크롤링 중단.")
                break
            

            distance_to_end = (current_page_latest_date - end_date).days
            if distance_history[stock_code][0] == 0:
                distance_history[stock_code][0] = abs(distance_to_end)
            
            if abs(distance_to_end) > distance_history[stock_code][0] :
                print(f"경고: 종목 {stock_code} - 현재 유추 값의 거리({abs(distance_to_end)})가 이전 거리 값({distance_history[stock_code][0]})보다 멀어짐으로 정밀 탐색으로 전환합니다.")
                current_page = distance_history[stock_code][1]
                break
            else :
                distance_history[stock_code][0] = abs(distance_to_end)
            
            distance_history[stock_code][1] = current_page
            
            if abs(distance_to_end) > 365 * 2:
                weight = 5
            elif abs(distance_to_end) > 365:
                weight = 4
            elif abs(distance_to_end) > 90:
                weight = 3
            elif abs(distance_to_end) > 30:
                weight = 1
            else:
                weight = 0.5

            step_size = max(1, round( (last_page / 100) * weight))

            if end_date < current_page_latest_date:
                next_page = current_page + max(1, round(step_size))
                print(f"정보: 종목 {stock_code} - 순방향 탐색. 현재: {current_page}, 목표: {next_page}, 거리: {distance_to_end}일")
            elif end_date > current_page_oldest_date:
                next_page = current_page - max(1, round(step_size))
                print(f"정보: 종목 {stock_code} - 역방향 탐색. 현재: {current_page}, 목표: {next_page}, 거리: {distance_to_end}일")
            else:
                break
            
            next_page = max(1, min(last_page, next_page))
            if next_page == current_page :
                print(f"정보: 종목 {stock_code} - 더 이상 이동할 페이지가 없습니다. 정밀 탐색 시작.")
                break
            if current_page + next_page < 1 :
                print(f"정보: 종목 {stock_code} - 더 이상 이동할 페이지가 없습니다. 정밀 탐색 시작.")
                current_page = 1
                break
            
            try:
                page_move_by_list_button(driver, wait, stock_code, next_page)
                
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                
                current_page = get_current_page_number(driver)
                current_page_latest_date, current_page_oldest_date = get_current_page_date_range(driver)
                if not current_page_latest_date or not current_page_oldest_date:
                    print(f"경고: 종목 {stock_code} - 이동한 페이지의 날짜 정보를 가져올 수 없습니다. 탐색 중단.")
                    break

                print(f"정보: 종목 {stock_code} - 이동 후 페이지({current_page}) 날짜 범위: {current_page_latest_date} ~ {current_page_oldest_date}")

                if abs((current_page_latest_date - end_date).days) <= 7:
                    print(f"정보: 종목 {stock_code} - 목표 날짜에 7일 이내로 근접. 정밀 탐색 시작.")
                    break
                
            except Exception as e:
                print(f"오류: 종목 {stock_code} - 페이지 탐색 중 오류 발생: {e}. 탐색 중단.")
                break
        
        print(f"정보: 종목 {stock_code} - 정밀 탐색 시작 (현재 페이지: {current_page}). 목표: {end_date}")
        crwaling_start_page = -1
        precision_search_attempts = 0
        
        while not (current_page_oldest_date <= end_date <= current_page_latest_date) and precision_search_attempts < MAX_PRECISION_SEARCH_PAGES:
            precision_search_attempts += 1
            
            if end_date < current_page_oldest_date:
                next_page_for_precision = current_page + 1
            elif end_date > current_page_latest_date:
                 next_page_for_precision = current_page - 1
            else:
                break 

            next_page_for_precision = max(1, min(last_page, next_page_for_precision))
            
            if next_page_for_precision == current_page:
                print(f"정보: 종목 {stock_code} - 정밀 탐색 중 더 이상 이동할 페이지가 없습니다. 현재 페이지({current_page})부터 크롤링 시도.")
                break

            print(f"정보: 종목 {stock_code} - 정밀 탐색 ({precision_search_attempts}/{MAX_PRECISION_SEARCH_PAGES}): 페이지 {current_page} -> {next_page_for_precision}")

            try:
                page_move_by_list_button(driver, wait, stock_code, next_page_for_precision)
                wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                
                current_page = get_current_page_number(driver)
                current_page_latest_date, current_page_oldest_date = get_current_page_date_range(driver)
                if not current_page_latest_date or not current_page_oldest_date:
                    print(f"경고: 종목 {stock_code} - 정밀 탐색 중 이동한 페이지의 날짜 정보를 가져올 수 없습니다. 탐색 중단.")
                    break
                
                print(f"정보: 종목 {stock_code} - 정밀 탐색 후 페이지({current_page}) 날짜 범위: {current_page_latest_date} ~ {current_page_oldest_date}")

            except Exception as e:
                print(f"오류: 종목 {stock_code} - 정밀 페이지 탐색 중 오류 발생: {e}. 탐색 중단.")
                break

        if current_page_oldest_date <= end_date <= current_page_latest_date:
            crwaling_start_page = current_page
            print(f"정보: 종목 {stock_code} - 크롤링 시작 페이지 확정: {crwaling_start_page} (end_date: {end_date} 포함)")
        else:
            crwaling_start_page = current_page
            print(f"경고: 종목 {stock_code} - end_date({end_date})가 최종 페이지({current_page}) 범위({current_page_latest_date} ~ {current_page_oldest_date})에 포함되지 않음. 가장 가까운 페이지에서 크롤링 시작: {crwaling_start_page}")

        # ----------------------------------------------------------------------
        # 10. 게시글 및 댓글 실제 크롤링 시작
        # ----------------------------------------------------------------------
        
        stop_crawling = False
        current_crawling_page = crwaling_start_page

        while not stop_crawling and current_crawling_page <= last_page: # 마지막 페이지까지 크롤링
            print(f"\n정보: 종목 {stock_code} - 현재 크롤링 페이지: {current_crawling_page} / 총 {last_page} 페이지")
            
            # 페이지 이동 (만약 현재 드라이버가 다른 페이지에 있다면 이동)
            if get_current_page_number(driver) != current_crawling_page:
                try:
                    page_move_by_list_button(driver, wait, stock_code, current_crawling_page)
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                    
                    if get_current_page_number(driver) != current_crawling_page:
                        print(f"오류: 종목 {stock_code} - 페이지 이동 실패. 목표: {current_crawling_page}, 실제: {get_current_page_number(driver)}. 크롤링 중단.")
                        break
                except Exception as e:
                    print(f"오류: 종목 {stock_code} - 크롤링 페이지 ({current_crawling_page}) 이동 중 오류 발생: {e}. 크롤링 중단.")
                    break
            
            # 페이지의 모든 게시글을 크롤링
            articles_on_current_page_elements = driver.find_elements(By.CSS_SELECTOR, "table.type2 tbody tr")
            article_info_list = []
            for row in articles_on_current_page_elements:
                try:
                    title_link_elem = row.find_element(By.CSS_SELECTOR, "td.title a")
                    article_url = title_link_elem.get_attribute('href')
                    
                    date_elem = row.find_element(By.CSS_SELECTOR, "td span.tah")
                    article_date_str = date_elem.text.strip()
                    article_date = parse_article_date(article_date_str)
                    # 유효한 날짜와 URL만 추가
                    if article_url and article_date:
                        article_info_list.append({'url': article_url, 'date': article_date})
                except NoSuchElementException:
                    # 광고 행 등 게시글이 아닌 경우 스킵
                    continue
                except Exception as e:
                    print(f"경고: 종목 {stock_code} - 게시글 목록에서 정보 추출 중 오류 발생. 해당 행 스킵.")
                    continue

            # article_info_list를 순회하며 크롤링
            for article_item in article_info_list:
                article_url = article_item['url']
                article_date_on_list = article_item['date']

                try:
                    if article_date_on_list < start_date:
                        print(f"정보: 종목 {stock_code} - 게시글 날짜({article_date_on_list})가 시작 날짜({start_date})보다 과거입니다. 크롤링 종료.")
                        stop_crawling = True
                        break
                    
                    if article_date_on_list > end_date:
                        print(f"정보: 종목 {stock_code} - 게시글 날짜({article_date_on_list})가 종료 날짜({end_date})보다 미래입니다. 해당 게시글 스킵.")
                        continue

                    print(f"정보: 종목 {stock_code} - 게시글 크롤링 시작: {article_url} ({article_date_on_list})")
                    
                    datas = scrape_article_details(driver, article_url)

                    article_data = {
                        'stock_name': stock_name,
                        'stock_code': str(stock_code),
                        'article_date': datetime.datetime.strptime(datas["article_date"].split(' ')[0], '%Y.%m.%d').date(), # '년.월.일' 부분만 사용
                        'article_title': datas["article_title"],
                        'article_nickname': datas["article_nickname"],
                        'article_content': datas["article_content"],
                        'article_comments': datas["article_comments"],
                        'article_viewers': datas["article_viewers"],
                        'article_likes': datas["article_likes"],
                        'article_dislikes': datas["article_dislikes"],
                        'article_url': datas["article_url"],
                        'vote_election': election,
                        'vote_candidate': candidate,
                        'vote_category': category,
                        'vote_start_date': stock_data['start_date'].date(),
                        'vote_end_date': stock_data['end_date'].date()
                    }
                    all_articles_data.append(article_data)
                    print(f"정보: 종목 {stock_code} - 게시글 '{article_data['article_title'][:20]}...' ({article_data['article_date']}) 크롤링 완료. (누적: {len(all_articles_data)}건)")
                    
                    page_move_by_list_button(driver, wait, stock_code, current_crawling_page, True)
                    
                except TimeoutException:
                    print(f"경고: 종목 {stock_code} - 게시글 또는 요소 로드 타임아웃. 다음 게시글로.")
                    # 타임아웃 발생 시 현재 페이지의 게시판 목록으로 강제 이동 시도
                    try:
                        driver.get(article_url)
                        apply_random_delay()
                        page_move_by_list_button(driver, wait, stock_code, current_crawling_page, True)
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                    except Exception as retry_e:
                        print(f"오류: 타임아웃 후 게시판 복귀 실패")
                    continue

                except NoSuchElementException as e:
                    print(f"경고: 종목 {stock_code} - 게시글 내 필요한 요소를 찾을 수 없습니다. 해당 게시글 스킵.")
                    # 요소 없음 발생 시 현재 페이지의 게시판 목록으로 강제 이동 시도
                    try:
                        driver.get(article_url)
                        apply_random_delay()
                        page_move_by_list_button(driver, wait, stock_code, current_crawling_page, True)
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                    except Exception as retry_e:
                        print(f"오류: 요소 없음 후 게시판 복귀 실패")
                    continue
                except Exception as e:
                    print(f"오류: 종목 {stock_code} - 게시글 크롤링 중 예상치 못한 오류 발생. 다음 게시글로.")
                    # 일반 예외 발생 시 현재 페이지의 게시판 목록으로 강제 이동 시도
                    try:
                        driver.get(article_url)
                        apply_random_delay()
                        page_move_by_list_button(driver, wait, stock_code, current_crawling_page, True)
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
                    except Exception as retry_e:
                        print(f"오류: 일반 예외 후 게시판 복귀 실패: {retry_e}")
                    continue

            if stop_crawling:
                break
            save_to_csv(all_articles_data, output_dir="output", filename=f"stock_articles_{election}_{candidate}_{stock_code}.csv")
            current_crawling_page += 1 # 다음 페이지로 이동
    except Exception as e:
        print(f"치명적 오류: 종목 {stock_code} 크롤링 중 예상치 못한 오류 발생:{e}")
    finally:
        if driver:
            driver.quit() # 드라이버 종료 (매우 중요)
    return []

def save_to_csv(data_list, output_dir="output", filename="crawled_articles.csv"):
    """
    크롤링된 기사 데이터를 Pandas DataFrame으로 변환하여 CSV 파일로 저장합니다.
    """
    if not data_list:
        print("정보: 저장할 데이터가 없습니다.")
        return

    # Pandas DataFrame 생성
    df = pd.DataFrame(data_list)

    # 출력 디렉토리가 없으면 생성
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    file_path = os.path.join(output_dir, filename)

    # 파일이 이미 존재하면 기존 파일에 이어서 작성, 없으면 새로 생성
    # header=False는 기존 파일에 덮어쓸 때 헤더를 다시 쓰지 않도록 함
    # index=False는 DataFrame의 인덱스를 CSV에 포함하지 않도록 함
    #mode = 'a' if os.path.exists(file_path) else 'w'
    mode = "w"
    header = not os.path.exists(file_path) # 파일이 없으면 헤더 포함

    try:
        df.to_csv(file_path, mode=mode, header=header, index=False, encoding='utf-8-sig')
        print(f"정보: {len(data_list)}개의 기사 데이터가 '{file_path}'에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"오류: CSV 파일 저장 중 오류 발생: {e}")


# --- 필터링 로직 ---
def filter_stock_list_or(df, filter_option_str):
    """
    DataFrame을 주어진 필터링 문자열에 따라 'election', 'candidate', 'category' 컬럼에 대한 부분 문자열 일치 또는
    'stock_code' 컬럼에 대한 정확한 일치로 필터링합니다.
    예: "20대 이재명 정책주" -> '20대'는 election, '이재명'은 candidate, '정책주'는 category와 일치하는지 확인
    예: "039240" -> '039240'은 stock_code와 일치하는지 확인
    """
    if not filter_option_str:
        return df

    keywords = filter_option_str.split()
    if not keywords:
        return df

    # 필터링할 컬럼 지정 (기존 키워드 검색용)
    text_search_columns = ['election', 'candidate', 'category']
    stock_code_column = 'stock_code' # stock_code는 별도로 정확히 일치하는지 확인

    condition = pd.Series([False] * len(df), index=df.index) # 초기값은 모두 False

    for keyword in keywords:
        # 1. 텍스트 검색 컬럼 (election, candidate, category)에 대한 조건
        for col in text_search_columns:
            if col in df.columns:
                # 대소문자 구분 없이, 부분 일치 (contains)로 검색
                condition = condition | df[col].astype(str).str.contains(keyword, case=False, na=False)
            else:
                print(f"경고: 필터링 컬럼 '{col}'이 CSV 파일에 없습니다. 해당 컬럼은 필터링에서 제외됩니다.")
        
        # 2. stock_code 컬럼에 대한 정확한 일치 조건
        if stock_code_column in df.columns:
            # stock_code는 정확히 일치하는 경우를 찾습니다.
            condition = condition | (df[stock_code_column].astype(str).str.strip().str.lower() == keyword.strip().lower())
        else:
            print(f"경고: 필터링 컬럼 '{stock_code_column}'이 CSV 파일에 없습니다. 해당 컬럼은 필터링에서 제외됩니다.")

    filtered_df = df[condition]
    print(f"정보: 필터링 옵션 '{filter_option_str}'에 따라 {len(filtered_df)}개의 종목이 필터링되었습니다.")
    if filtered_df.empty:
        print("경고: 필터링 조건에 해당하는 종목이 없습니다. 모든 항목이 제외됩니다.")
    return filtered_df


def filter_stock_list_and(df, filter_option_str):
    """
    DataFrame을 주어진 필터링 문자열에 따라 'election', 'candidate', 'category' 컬럼에 대한 부분 문자열 일치 또는
    'stock_code' 컬럼에 대한 정확한 일치로 필터링합니다.
    입력된 모든 키워드에 대해 AND 연산으로 필터링합니다.

    예: "20대 이재명 정책주" -> '20대', '이재명', '정책주' 키워드를 모두 만족하는 행을 찾습니다.
        (한 행 안에서 '20대'가 발견되고, '이재명'이 발견되고, '정책주'가 발견되어야 함)
    예: "039240" -> '039240'은 stock_code와 일치하는지 확인합니다.
    """
    if not filter_option_str:
        return df

    keywords = filter_option_str.split()
    if not keywords:
        return df

    # 필터링할 컬럼 지정
    text_search_columns = ['election', 'candidate', 'category']
    stock_code_column = 'stock_code'

    # 최종 AND 조건을 저장할 Series, 초기값은 모두 True
    # True로 시작해야 각 키워드 조건을 & (AND) 연산으로 누적할 수 있습니다.
    final_condition = pd.Series([True] * len(df), index=df.index)

    for keyword in keywords:
        # 현재 키워드에 대한 조건을 저장할 Series (OR 연산용), 초기값은 모두 False
        # False로 시작해야 각 컬럼의 조건을 | (OR) 연산으로 누적할 수 있습니다.
        keyword_condition = pd.Series([False] * len(df), index=df.index)

        # 1. 텍스트 검색 컬럼 (election, candidate, category)에 대한 OR 조건
        for col in text_search_columns:
            if col in df.columns:
                # 현재 키워드가 텍스트 컬럼 중 하나라도 포함되면 True
                keyword_condition = keyword_condition | df[col].astype(str).str.contains(keyword, case=False, na=False)
            else:
                print(f"경고: 필터링 컬럼 '{col}'이 CSV 파일에 없습니다. 해당 컬럼은 필터링에서 제외됩니다.")

        # 2. stock_code 컬럼에 대한 OR 조건
        if stock_code_column in df.columns:
            # 현재 키워드가 stock_code와 정확히 일치해도 True
            keyword_condition = keyword_condition | (df[stock_code_column].astype(str).str.strip().str.lower() == keyword.strip().lower())
        else:
            print(f"경고: 필터링 컬럼 '{stock_code_column}'이 CSV 파일에 없습니다. 해당 컬럼은 필터링에서 제외됩니다.")

        # 3. 현재 키워드에 대한 통합 조건(keyword_condition)을
        #    최종 조건(final_condition)에 AND 연산으로 누적합니다.
        final_condition = final_condition & keyword_condition

    filtered_df = df[final_condition]
    print(f"정보: 필터링 옵션 '{filter_option_str}'에 따라 {len(filtered_df)}개의 종목이 필터링되었습니다.")
    if filtered_df.empty:
        print("경고: 필터링 조건에 해당하는 종목이 없습니다.")

    return filtered_df

def load_theme_stock_list(file_path, filter_option=None, filter_logic='or'):
    """테마주 목록 CSV 파일을 로드하고 필요한 컬럼을 반환합니다."""
    if not os.path.exists(file_path):
        print(f"오류: 테마주 목록 파일이 없습니다. '{file_path}' 경로를 확인해주세요.")
        return pd.DataFrame()
    
    df = pd.read_csv(file_path, dtype=str)
    # 필요한 컬럼이 모두 있는지 확인
    required_columns = ['election', 'candidate', 'stock_name', 'stock_code', 'category', 'start_date', 'end_date']
    if not all(col in df.columns for col in required_columns):
        missing_cols = [col for col in required_columns if col not in df.columns]
        print(f"오류: CSV 파일에 필수 컬럼이 누락되었습니다: {', '.join(missing_cols)}. 파일을 확인해주세요.")
        return pd.DataFrame()

    df = df[required_columns].copy()
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    print(f"--- 총 {len(df)}개의 테마주 정보를 로드했습니다. ---")

    # 필터링 적용
    if filter_option:
        # logic 인자에 따라 적절한 필터링 함수 호출
        print(f"정보: '{filter_logic.upper()}' 논리로 필터링을 적용합니다.")
        if filter_logic.lower() == 'and':
            df = filter_stock_list_and(df, filter_option)
        else: # 기본값 또는 'or'일 경우
            df = filter_stock_list_or(df, filter_option)
    return df


def main():
    parser = argparse.ArgumentParser(description="네이버 종목 토론방 게시글 크롤러")
    parser.add_argument('-f', '--file', type=str, default='data/stock_list.csv',
                        help="크롤링할 종목 목록이 담긴 CSV 파일 경로 (기본값: data/stock_list.csv)")
    parser.add_argument('-w', '--workers', type=int, default=3,
                        help="동시에 실행할 쓰레드(작업자) 수 (기본값: 3)")
    parser.add_argument('-o', '--option', type=str, default=None,
                        help="필터링할 문자열 (예: '20대 이재명 정책주'). 'election', 'candidate', 'category', 'stock_code' 컬럼에서 검색합니다.")
    parser.add_argument('-l', '--logic', type=str, default='or', choices=['or', 'and'],
                        help="필터링 키워드 간의 검색 조건 ('or' 또는 'and', 기본값: or)")
    args = parser.parse_args()
   
    stock_list_to_crawl = load_theme_stock_list(args.file, args.option, args.logic)
    stock_list_to_crawl = stock_list_to_crawl.to_dict('records')
    
    if not stock_list_to_crawl:
        print("경고: 크롤링할 종목 데이터가 없습니다. 프로그램을 종료합니다.")
        return

    # 프록시 리스트 (필요하다면 사용)
    # proxy_list = ["http://your.proxy.com:8080", "http://another.proxy.com:8080"]
    proxy_list = [None] # 프록시를 사용하지 않을 경우

    all_results = []
    
    # ThreadPoolExecutor를 사용하여 여러 종목을 동시에 크롤링 (병렬 처리)
    # max_workers는 동시에 실행될 스레드(작업자)의 수
    # 주의: 웹사이트에 과도한 요청을 보내지 않도록 적절한 max_workers와 지연 시간 설정이 중요합니다.
    with ThreadPoolExecutor(max_workers=args.workers) as executor: # --- 수정된 부분: args.workers 사용 ---
        futures = []
        for i, stock_data in enumerate(stock_list_to_crawl):
            # 각 종목에 대해 랜덤으로 프록시 할당 (또는 None 할당)
            proxy_to_use = random.choice(proxy_list) 
            futures.append(executor.submit(scrape_stock_articles_by_date_range, stock_data, proxy_to_use))

        for future in futures:
            result = future.result()
            if result:
                all_results.extend(result)

    print("\n--- 모든 종목 크롤링 완료 ---")
    if all_results:
        # 모든 종목의 데이터를 하나의 CSV로 저장할 수도 있습니다.
        #save_to_csv(all_results, filename="all_stock_articles.csv")
        print(f"총 {len(all_results)}개의 게시글이 성공적으로 수집되었습니다.")
    else:
        print("수집된 게시글이 없습니다.")

if __name__ == "__main__":
    main()