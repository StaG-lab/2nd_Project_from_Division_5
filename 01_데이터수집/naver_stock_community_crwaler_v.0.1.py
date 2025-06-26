# pip install -r requirements.txt
import pandas as pd
import time
import re
import os
import datetime 
from bs4 import BeautifulSoup as bs
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from tqdm import trange, tqdm # 진행률 표시줄
import requests

# Selenium WebDriver 인스턴스를 전역적으로 관리하거나, 각 함수에서 인자로 전달하도록 변경
# driver = None # 초기화 시점에 None으로 설정
# headers는 BeautifulSoup와 requests에 사용되는 HTTP 헤더
headers = {"user-agent": "Mozilla/5.0"}
current_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
filepath = f'data/articleList_{current_date}.csv'

last_page_default = 5 # 기본값 설정, 실제로는 get_last_page 함수에서 동적으로 결정됨

def initialize_driver():
    """Selenium WebDriver를 초기화하고 반환합니다."""
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # 브라우저 창을 띄우지 않고 실행 (백그라운드 실행)
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    options.add_argument('lang=ko_KR') # 한국어 설정
    
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30) # 페이지 로딩 최대 30초 대기
    return driver

def get_item_code_list():
    """네이버 금융 인기 검색 종목 코드를 가져옵니다."""
    url = "https://finance.naver.com/sise/lastsearch2.naver"
    response = requests.get(url, headers=headers)
    soup = bs(response.text, 'html.parser') 
    item_codes = []
    
    # 'div.box_type_l' 안에 있는 'table.type_5'를 찾습니다.
    # select_one은 찾지 못하면 None을 반환하므로 예외 처리
    list_table_container = soup.select_one('div.box_type_l')
    if not list_table_container:
        print("종목 코드 테이블 컨테이너를 찾을 수 없습니다.")
        return []

    list_table = list_table_container.select_one('table.type_5')
    if not list_table:
        print("종목 코드 테이블을 찾을 수 없습니다.")
        return []

    code_pattern = r'code=(\d+)'

    print("--- 추출된 종목 코드 ---")
    for a_tag in list_table.find_all('a'):
        href_value = a_tag.get('href')
        
        if href_value:
            match = re.search(code_pattern, href_value)
            if match:
                stock_code = match.group(1)
                stock_name = a_tag.text.strip()
                print(f"종목명: {stock_name}, 코드: {stock_code}")
                item_codes.append(stock_code)
    return item_codes

def get_item_url(item_code, page_no=1):
    """주어진 종목 코드와 페이지 번호에 대한 게시판 URL을 생성합니다."""
    return f"https://finance.naver.com/item/board.naver?code={item_code}&page={page_no}"

def get_last_page(driver, item_code):
    """
    Selenium을 사용하여 특정 종목의 마지막 페이지 번호를 가져옵니다.
    """
    url = get_item_url(item_code)
    driver.get(url)

    try:
        # 'td.pgRR' 클래스를 가진 요소를 찾을 때까지 최대 10초 대기
        last_page_element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'td.pgRR a'))
        )
        
        href_value = last_page_element.get_attribute('href')
        
        # href 값에서 'page=' 다음의 숫자를 추출
        match = re.search(r'page=(\d+)', href_value)
        if match:
            return int(match.group(1))
        else:
            print(f"경고: 종목 {item_code}의 마지막 페이지 번호를 추출할 수 없습니다. 기본값 사용.")
            return last_page_default # 추출 실패 시 적절한 기본값
    except TimeoutException:
        print(f"경고: 종목 {item_code}의 마지막 페이지 요소를 찾지 못했습니다 (타임아웃). 기본값 사용.")
        return last_page_default # 타임아웃 발생 시 기본값
    except NoSuchElementException:
        print(f"경고: 종목 {item_code}의 마지막 페이지 요소를 찾을 수 없습니다. 기본값 사용.")
        return last_page_default # 요소가 없을 시 기본값
    except Exception as e:
        print(f"오류: 종목 {item_code}의 마지막 페이지를 가져오는 중 예상치 못한 오류 발생: {e}. 기본값 사용.")
        return last_page_default

def get_one_page(driver, item_code, page_no):
    """
    Selenium을 사용하여 특정 종목의 한 페이지 게시글 정보를 가져옵니다.
    동적으로 로드되는 내용을 처리합니다.
    """
    page_url = get_item_url(item_code, page_no)
    #print(f"크롤링 중: 종목 {item_code}, 페이지 {page_no} ({page_url})")
    driver.get(page_url)

    dataframe = pd.DataFrame(columns=['날짜', '제목', '닉네임', '종목', '게시글', '댓글수', '조회수', '공감', '비공감'])
    
    # 게시글 목록이 로드될 때까지 대기
    # 여기서는 게시글의 첫 번째 컬럼(날짜)이 나타날 때까지 기다립니다.
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'span.tah.p10.gray03'))
        )
    except TimeoutException:
        print(f"경고: 종목 {item_code}, {page_no} 페이지의 게시글 목록 로딩 타임아웃.")
        return dataframe # 데이터프레임 반환 후 다음 페이지로 이동
    except Exception as e:
        print(f"오류: 종목 {item_code}, {page_no} 페이지 로딩 중 예상치 못한 오류 발생: {e}")
        return dataframe

    # BeautifulSoup으로 페이지 소스 파싱 (Selenium이 로드한 최종 HTML)
    soup =bs(driver.page_source, 'html.parser')

    posts = soup.find_all('tr')
    date_pattern = r'\d{4}\.\d{2}\.\d{2} \d{2}:\d{2}'
    comment_count_pattern = r'\[(\d+)\]'
    article_number_pattern = r'nid=(\d+)'
    cleanbot_str = '클린봇이 이용자 보호를 위해 숨긴 게시글입니다.'

    for post in posts:
        tds = post.find_all('td')
        # 유효한 게시글 행인지 확인 (td 개수, 날짜 형식 등)
        if not tds or len(tds) < 6:
            continue
        
        date_span = post.find('span', class_='tah p10 gray03')
        # 날짜가 유효한 형식인지 확인
        if date_span:
            date_text = date_span.text.strip()
            if not re.fullmatch(date_pattern, date_text): # 정확히 날짜 패턴에 맞는지 확인
                continue # 날짜 형식이 아니면 게시글 아님
            date = date_text
        else:
            continue # 날짜 스팬이 없으면 게시글 아님

        # 제목, 링크, 닉네임, 조회수, 공감, 비공감 추출
        title_td = tds[1]
        link_tag = title_td.find('a')
        
        if not link_tag: # 링크가 없으면 게시글 아님
            continue

        link = link_tag['href'] if 'href' in link_tag.attrs else ''
        if not link: # href 속성 값이 없으면 건너뛰기
            continue

        article_number_match = re.search(article_number_pattern, link)
        article_number = article_number_match.group(1) if article_number_match else ''
        if not article_number: # 게시글 번호가 없으면 건너뛰기
            continue

        full_title_text = title_td.text.strip()
        comment_count_match = re.search(comment_count_pattern, full_title_text)
        comment_count = comment_count_match.group(1) if comment_count_match else 0
        
        # 댓글 수를 제외한 제목 추출 (정규표현식으로 더 정확하게)
        title = re.sub(comment_count_pattern, '', full_title_text).strip()
        title = re.sub(r'\s+', ' ', title).strip() # 여러 공백 하나로 줄이기

        # '클린봇' 게시글 필터링
        if cleanbot_str in title:
            continue
        
        # 나머지 정보 추출 (존재하지 않을 수 있으므로 인덱스 체크)
        nickname = tds[2].text.strip() if len(tds) > 2 else ''
        views = tds[3].text.strip() if len(tds) > 3 else ''
        likes = tds[4].text.strip() if len(tds) > 4 else ''
        dislikes = tds[5].text.strip() if len(tds) > 5 else ''

        # DataFrame에 추가
        new_row = pd.DataFrame([{
            '날짜': date,
            '제목': title,
            '닉네임': nickname,
            '종목': item_code,
            '게시글': article_number,
            '댓글수': comment_count,
            '조회수': views,
            '공감': likes,
            '비공감': dislikes
        }])
        dataframe = pd.concat([dataframe, new_row], ignore_index=True)
    return dataframe


def get_all_pages(driver, item_code):
    """
    특정 종목의 모든 게시판 페이지를 크롤링합니다.
    """
    last_page = get_last_page(driver, item_code)
    
    # 디버깅을 위해 last_page 제한 
    if last_page > last_page_default: last_page = last_page_default
    
    # print(f"\n종목 코드: {item_code}, 총 {last_page} 페이지를 크롤링합니다.")
    
    page_list = []
    # 1페이지부터 마지막 페이지까지 반복
    for page_num in range(1, last_page + 1):
        df_one_page = get_one_page(driver, item_code, page_num)
        if not df_one_page.empty: # 빈 데이터프레임이 아니면 추가
            page_list.append(df_one_page)
        # 과도한 요청 방지를 위해 페이지당 딜레이 추가
        time.sleep(0.5) 
    
    if not page_list:
        print(f"경고: 종목 {item_code}에서 어떤 게시글도 가져오지 못했습니다.")
        return pd.DataFrame(columns=['날짜', '제목', '닉네임', '종목', '게시글', '댓글수', '조회수', '공감', '비공감'])

    df_all_page = pd.concat(page_list, ignore_index=True)
    
    # 데이터 타입 변환
    for col in ['댓글수', '조회수', '공감', '비공감']:
        # 숫자만 포함하는지 확인 후 int로 변환, 실패 시 0으로 설정
        df_all_page[col] = pd.to_numeric(df_all_page[col], errors='coerce').fillna(0).astype(int)
    
    return df_all_page

def get_article_content(article_list):
    # 결과를 저장할 DataFrame 초기화
    content_df = pd.DataFrame(columns=['게시글', '게시글내용', '댓글']) 
    
    driver = initialize_driver() # 드라이버 초기화
    driver.set_page_load_timeout(30) # 페이지 로딩 최대 30초 대기

    error_count = 0
    for article in tqdm(article_list, desc="게시글 내용 및 댓글 크롤링 중"):
        stock_code, article_id = article[0], article[1]
        
        # 게시글 URL 생성
        url = f'https://finance.naver.com/item/board_read.naver?code={stock_code}&nid={article_id}'
        
        try:
            # WebDriver를 통해 URL 접속
            driver.get(url)

            # div#body가 나타날 때까지 페이지 로딩을 기다림 (최대 10초)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div#body'))
            )

            # 게시글 내용 추출
            content_element = driver.find_element(By.CSS_SELECTOR, 'div#body')
            content = content_element.text.strip() if content_element else ''
            if not content:
                print(f"경고: 게시글 {article_id}의 내용을 찾을 수 없습니다.")
            
            # 댓글 추출
            comments = []
            # 댓글 요소가 로드될 때까지 잠시 대기 (optional, 하지만 필요할 수 있음)
            time.sleep(0.5) 
            comment_elements = driver.find_elements(By.CSS_SELECTOR, 'span.u_cbox_contents')
            for comment_elem in comment_elements:
                comments.append(comment_elem.text.strip())
            
            # 댓글 리스트를 하나의 문자열로 합치기 (혹은 리스트 그대로 저장)
            comments_str = " | ".join(comments) if comments else ""

        except TimeoutException:
            content = ''
            comments_str = ''
            error_count += 1
            #print(f"경고: 게시글 {article_id} 로딩 타임아웃 발생. 내용 및 댓글을 가져올 수 없습니다.")
        except NoSuchElementException:
            content = ''
            comments_str = ''
            error_count += 1
            #print(f"경고: 게시글 {article_id}에서 필요한 요소를 찾을 수 없습니다. 내용 및 댓글을 가져올 수 없습니다.")
        except Exception as e:
            content = ''
            comments_str = ''
            error_count += 1
            #print(f"오류: 게시글 {article_id} 처리 중 예상치 못한 오류 발생: 내용 및 댓글을 가져올 수 없습니다.")

        # DataFrame에 결과 추가
        new_row = pd.DataFrame([{'게시글': article_id, '게시글내용': content, '댓글': comments_str}]) # comments_str로 변경
        content_df = pd.concat([content_df, new_row], ignore_index=True)
        
        # 각 게시글 처리 후 잠시 대기
        time.sleep(0.1) 

    # WebDriver 종료
    driver.quit()
    if error_count > 0:
        print(f"경고: 총 {error_count}개의 게시글에서 오류가 발생했습니다. 일부 게시글 내용이나 댓글을 가져오지 못했을 수 있습니다.")
    return content_df
            
if __name__ == "__main__":
    driver = None # 드라이버 변수 선언
    try:
        driver = initialize_driver() # 드라이버 초기화
        
        df_list = []
        item_code_list = get_item_code_list()
        
        # 실제 크롤링 시에는 전체 item_code_list를 사용합니다.
        # 테스트를 위해 일부 종목만 크롤링하려면 슬라이싱을 사용하세요.
        # item_code_list = item_code_list[:3]
        
        # item_code_list의 각 종목 코드를 순회
        for item_code in trange(len(item_code_list), desc="전체 종목 크롤링 진행률"):
            current_item_code = item_code_list[item_code] # 리스트에서 실제 종목 코드를 가져옴
            df = get_all_pages(driver, current_item_code) # 드라이버 인스턴스 전달
            if not df.empty:
                df_list.append(df)
            time.sleep(1) # 각 종목 크롤링 후 대기
        
        if not df_list:
            print("모든 종목에서 데이터를 가져오는 데 실패했습니다.")
            df_all = pd.DataFrame()
        else:
            # 모든 종목의 데이터프레임을 하나로 합치기
            df_all = pd.concat(df_list, ignore_index=True)
            print("\n--- 모든 종목의 게시글 데이터 ---")
            print(df_all.head())
            print(f"총 {len(df_all)}개의 게시글을 수집했습니다.")

        # data 디렉토리 생성
        if not os.path.exists('data'):
            os.makedirs('data')
        
        # df_all을 csv로 저장
        if not df_all.empty:
            df_all.to_csv(filepath, index=False, encoding='utf-8-sig') # 한글 깨짐 방지를 위해 encoding 추가
            print(f"저장 완료: {filepath}")
        else:
            print("수집된 데이터가 없어 CSV 파일을 저장하지 않습니다.")

    except Exception as e:
        print(f"크롤링 과정 중 심각한 오류 발생: {e}")
    finally:
        if driver:
            driver.quit() # 드라이버 종료
            print("WebDriver가 종료되었습니다.")
            
    # CSV 파일 불러오기: '종목'과 '게시글' 컬럼을 문자열로 지정
    df_all = pd.read_csv(filepath, encoding='utf-8-sig', dtype={'종목': str, '게시글': str})
    
    # 필요한 '종목'과 '게시글' 정보만 추출하여 list of lists 형태로 변환
    article_list_for_content = df_all[['종목', '게시글']].values.tolist()
    
    print(f"총 {len(article_list_for_content)}개의 게시글 내용을 크롤링합니다.")
    
    # 게시글 내용 및 댓글 크롤링 함수 호출
    df_article = get_article_content(article_list_for_content)
    
    # 두 데이터프레임을 '게시글' 컬럼을 기준으로 합치기
    merged_df = pd.merge(df_all, df_article, on='게시글', how='left')
    
    # data 디렉토리 생성 (이미 존재하면 건너김)
    os.makedirs('data', exist_ok=True)
    
    # 합쳐진 DataFrame을 CSV로 저장
    current_date = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    contentfilepath = f'data/naver_stock_community_{current_date}.csv'
    merged_df.to_csv(contentfilepath, index=False, encoding='utf-8-sig') # 한글 깨짐 방지를 위해 encoding 추가
    
    print(f"최종 데이터 저장 완료: {contentfilepath}")
    print("\n--- 합쳐진 데이터프레임의 상위 5개 행 ---")
    print(merged_df.head())