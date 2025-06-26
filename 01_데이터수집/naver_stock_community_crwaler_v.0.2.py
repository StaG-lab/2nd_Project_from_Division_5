# pip install -r requirements.txt
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
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- 상수 및 설정 ---
HEADERS = {"user-agent": "Mozilla/5.0"}
LAST_PAGE_LIMIT = 5  # 디버깅 및 테스트를 위한 기본 페이지 제한
MAX_WORKERS = 5 # 스레드의 최대 개수, 적정값은 시스템 사양에 따라 다를 수 있음

def initialize_driver():
    """개별 스레드를 위한 새로운 Selenium WebDriver를 초기화하고 반환합니다."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('lang=ko_KR')
    
    # 각 스레드는 자신만의 드라이버 인스턴스를 갖도록 함
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(20) # 페이지 로드 타임아웃 줄임
    return driver

def get_item_code_list():
    """네이버 금융 인기 검색 종목 코드를 가져옵니다."""
    url = "https://finance.naver.com/sise/lastsearch2.naver"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status() # HTTP 오류 발생 시 예외 발생
        soup = bs(response.text, 'html.parser')
        
        table = soup.select_one('div.box_type_l table.type_5')
        if not table:
            print("종목 코드 테이블을 찾을 수 없습니다.")
            return []

        item_codes = []
        code_pattern = r'code=(\d+)'
        for a_tag in table.select('a'):
            href = a_tag.get('href', '')
            match = re.search(code_pattern, href)
            if match:
                item_codes.append(match.group(1))
        
        print(f"--- 총 {len(item_codes)}개의 종목 코드를 추출했습니다. ---")
        return item_codes
    except requests.RequestException as e:
        print(f"URL 요청 중 오류가 발생했습니다: {e}")
        return []
    except Exception as e:
        print(f"종목 코드 추출 중 예상치 못한 오류 발생: {e}")
        return []

def get_last_page(item_code):
    """특정 종목의 마지막 페이지 번호를 requests를 사용하여 빠르게 가져옵니다."""
    url = f"https://finance.naver.com/item/board.naver?code={item_code}"
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        soup = bs(response.text, 'html.parser')
        
        last_page_tag = soup.select_one('td.pgRR a')
        if not last_page_tag or 'href' not in last_page_tag.attrs:
            # 게시글이 10페이지 미만일 경우 맨뒤 버튼이 없을 수 있음
            # 이 경우 모든 페이지 버튼을 찾아 가장 큰 번호를 반환
            page_tags = soup.select('td.pgN a')
            if not page_tags:
                return 1 # 페이지 버튼이 아예 없으면 1페이지
            
            pages = [int(re.search(r'page=(\d+)', tag['href']).group(1)) for tag in page_tags if re.search(r'page=(\d+)', tag['href'])]
            return max(pages) if pages else 1

        href = last_page_tag['href']
        match = re.search(r'page=(\d+)', href)
        if match:
            return int(match.group(1))
        return LAST_PAGE_LIMIT
    except Exception as e:
        # print(f"경고: 종목 {item_code}의 마지막 페이지 번호를 가져오는 중 오류 발생: {e}. 기본값을 사용합니다.")
        return LAST_PAGE_LIMIT

def scrape_article_details(driver, article_url):
    """
    하나의 게시글 URL에 접속하여 본문과 댓글을 스크랩합니다.
    이 함수는 이미 생성된 드라이버를 인자로 받습니다.
    """
    content = ""
    comments_str = ""
    try:
        driver.get(article_url)
        # 내용이 로드될 때까지 명시적으로 대기
        wait = WebDriverWait(driver, 10)
        content_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div#body')))
        content = content_element.text.strip()
        
        # 댓글은 동적으로 로드될 수 있으므로 잠시 대기 후 수집
        time.sleep(0.3) 
        comment_elements = driver.find_elements(By.CSS_SELECTOR, 'span.u_cbox_contents')
        comments = [elem.text.strip() for elem in comment_elements]
        comments_str = " | ".join(comments)

    except TimeoutException:
        #print(f"타임아웃: {article_url} 내용을 가져올 수 없습니다.")
        pass # 오류 발생 시 빈 문자열 반환
    except Exception:
        #print(f"오류 발생: {article_url} 처리 중 문제 발생.")
        pass
    
    return content, comments_str

def scrape_page_worker(item_code, page_no):
    """
    [작업자 함수] 한 페이지에 있는 모든 게시글의 정보와 내용을 수집합니다.
    각 스레드에서 독립적으로 실행됩니다.
    """
    driver = initialize_driver()
    articles_data = []
    list_url = f"https://finance.naver.com/item/board.naver?code={item_code}&page={page_no}"

    try:
        driver.get(list_url)
        # 페이지 목록이 로드될 때까지 대기
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2")))
        
        soup = bs(driver.page_source, 'html.parser')
        posts = soup.select("table.type2 tr[onmouseover]") # 유효한 게시글 행만 선택

        for post in posts:
            tds = post.find_all('td')
            if len(tds) < 6:
                continue

            # 1. 목록 정보 우선 수집
            date = tds[0].find('span', class_='tah').text.strip()
            title_tag = tds[1].find('a')
            if not title_tag or 'title' not in title_tag.attrs:
                continue
            
            title = title_tag['title']
            article_url = "https://finance.naver.com" + title_tag['href']
            
            nickname = tds[2].text.strip()
            views = tds[3].text.strip()
            likes = tds[4].text.strip()
            dislikes = tds[5].text.strip()
            
            # 2. 수집된 링크로 이동하여 상세 내용 수집
            content, comments = scrape_article_details(driver, article_url)

            # 3. 모든 정보를 딕셔너리로 저장
            articles_data.append({
                '날짜': date,
                '제목': title,
                '닉네임': nickname,
                '종목코드': item_code,
                '게시글내용': content,
                '댓글': comments,
                '조회수': int(views.replace(',', '')),
                '공감': int(likes.replace(',', '')),
                '비공감': int(dislikes.replace(',', '')),
                'URL': article_url
            })
            # 목록 페이지로 다시 돌아올 필요 없이, 다음 게시글 수집은 for문으로 계속 진행됩니다.
            # 하지만 상세 내용을 본 후에는 목록으로 돌아와야 다음 링크를 얻을 수 있습니다.
            # scrape_article_details에서 새 탭을 열고 닫는 전략이 더 효율적일 수 있으나,
            # 현재는 안정성을 위해 같은 탭에서 이동 후 다시 목록 페이지로 돌아갑니다.
            driver.get(list_url) # 다음 게시글 수집을 위해 목록 페이지로 복귀
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.type2"))) # 복귀 후 로딩 대기

    except TimeoutException:
        #print(f"타임아웃: 종목 {item_code}, 페이지 {page_no} 목록을 가져올 수 없습니다.")
        pass
    except Exception as e:
        #print(f"오류 발생: 종목 {item_code}, 페이지 {page_no} 처리 중 문제 발생: {e}")
        pass
    finally:
        driver.quit() # 작업자(스레드)의 임무가 끝나면 반드시 드라이버 종료
    
    return articles_data


if __name__ == "__main__":
    start_time = time.time()
    
    item_codes = get_item_code_list()
    # 테스트를 위해 일부 종목만 사용
    # item_codes = item_codes[:3] 
    
    # --- 전체 작업 목록 생성 ---
    tasks = []
    print("수집할 페이지 목록을 생성 중입니다...")
    for code in tqdm(item_codes, desc="종목별 마지막 페이지 확인"):
        # 실제 크롤링 시에는 get_last_page(code)를 사용하세요.
        # last_page = get_last_page(code) 
        # 테스트를 위해 LAST_PAGE_LIMIT로 제한
        last_page = LAST_PAGE_LIMIT 
        for page in range(1, last_page + 1):
            tasks.append((code, page))
            
    print(f"총 {len(tasks)}개의 페이지를 {MAX_WORKERS}개의 스레드로 수집합니다.")

    all_results = []
    # --- ThreadPoolExecutor를 사용한 병렬 처리 ---
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # 각 작업을 executor에 제출하고 future 객체를 받음
        future_to_task = {executor.submit(scrape_page_worker, code, page): (code, page) for code, page in tasks}
        
        # tqdm을 사용하여 진행 상황 시각화
        progress = tqdm(as_completed(future_to_task), total=len(tasks), desc="페이지 크롤링 진행률")
        
        for future in progress:
            task_info = future_to_task[future]
            try:
                # 작업 결과(한 페이지의 게시글 데이터 리스트)를 가져옴
                result_list = future.result()
                if result_list:
                    all_results.extend(result_list)
            except Exception as exc:
                print(f'{task_info} 작업 중 오류 발생: {exc}')

    # --- 최종 결과 처리 ---
    if not all_results:
        print("수집된 데이터가 없습니다.")
    else:
        # 최종 데이터프레임 생성
        final_df = pd.DataFrame(all_results)
        
        # 데이터 저장
        data_dir = 'data'
        os.makedirs(data_dir, exist_ok=True)
        
        current_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filepath = os.path.join(data_dir, f'naver_stock_community_refactored_{current_date}.csv')
        
        final_df.to_csv(filepath, index=False, encoding='utf-8-sig')
        
        print("\n--- 최종 크롤링 결과 ---")
        print(f"총 {len(final_df)}개의 게시글을 수집했습니다.")
        print(f"최종 데이터 저장 완료: {filepath}")
        print(final_df.head())

    end_time = time.time()
    print(f"\n총 실행 시간: {end_time - start_time:.2f}초")