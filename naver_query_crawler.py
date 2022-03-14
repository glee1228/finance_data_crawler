import platform
import argparse
from time import sleep
from multiprocessing import Process
import datetime
from exceptions import *
from tqdm import tqdm
import pandas as pd
import requests
import re
from bs4 import BeautifulSoup


class ArticleParser(object):
    special_symbol = re.compile('[\{\}\[\]\/?,;:|\)*~`!^\-_+<>@\#$&▲▶◆◀■【】\\\=\(\'\"]')
    content_pattern = re.compile('본문 내용|TV플레이어| 동영상 뉴스|flash 오류를 우회하기 위한 함수 추가function  flash removeCallback|tt|앵커 멘트|xa0')

    @classmethod
    def clear_content(cls, text):
        # 기사 본문에서 필요없는 특수문자 및 본문 양식 등을 다 지움
        newline_symbol_removed_text = text.replace('\\n', '').replace('\\t', '').replace('\\r', '')
        special_symbol_removed_content = re.sub(cls.special_symbol, ' ', newline_symbol_removed_text)
        end_phrase_removed_content = re.sub(cls.content_pattern, '', special_symbol_removed_content)
        blank_removed_content = re.sub(' +', ' ', end_phrase_removed_content).lstrip()  # 공백 에러 삭제
        reversed_content = ''.join(reversed(blank_removed_content))  # 기사 내용을 reverse 한다.
        content = ''
        for i in range(0, len(blank_removed_content)):
            # reverse 된 기사 내용중, ".다"로 끝나는 경우 기사 내용이 끝난 것이기 때문에 기사 내용이 끝난 후의 광고, 기자 등의 정보는 다 지움
            if reversed_content[i:i + 2] == '.다':
                content = ''.join(reversed(reversed_content[i:]))
                break
        return content

    @classmethod
    def clear_headline(cls, text):
        # 기사 제목에서 필요없는 특수문자들을 지움
        newline_symbol_removed_text = text.replace('\\n', '').replace('\\t', '').replace('\\r', '')
        special_symbol_removed_headline = re.sub(cls.special_symbol, '', newline_symbol_removed_text)
        return special_symbol_removed_headline

    @classmethod
    def find_news_totalpage(cls, url): # 10000추가되어서 들어옴
        def is_target(url, middle):  # 정답 243 o 244 x
            new_url = url[:-5] + str(middle) + '1'
            request_content = requests.get(new_url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            document_content = BeautifulSoup(request_content.content, 'html.parser')
            po = document_content.select('.sc_page_inner .btn')
            return po
        # 당일 기사 목록 전체를 알아냄
        try:
            request_content = requests.get(url, timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
            document_content = BeautifulSoup(request_content.content, 'html.parser')
            po = document_content.select('.sc_page_inner .btn')
            return int(po[-1].text)
        except Exception:
            # print('마지막 페이지 위한 이진탐색중...')
            search_page = [i for i in range(0, 400)]
            left = 0
            right = len(search_page) - 1
            check_list = []
            while left <= right:
                mid = (left + right) // 2
                preset = (left, mid, right)
                check_list.append(preset)
                if len(is_target(url, mid)) != 0:
                    left = mid + 1
                elif len(is_target(url, mid)) == 0:
                    right = mid + 1
                nowset = (left, mid, right)
                check_list.append(nowset)
                if check_list[0]==check_list[1]:
                    break
                else:
                    check_list.pop(0)
            # print('마지막 페이지 :',left-1)
            return int(left-1)

class ArticleCrawler(object):
    def __init__(self):
        self.selected_queries = []
        self.user_operating_system = str(platform.system())
        self.len_target_urls = 0




    def set_date_range(self, ds, de):#datestart, dateend
        self.ds = ds
        self.de = de
        print(self.ds, self.de)


    @staticmethod
    def get_url_data(url, max_tries=5):
        remaining_tries = int(max_tries)
        headers_list = [{'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'},]
        while remaining_tries > 0:
            try:
                return requests.get(url, headers=headers_list[0])
            except requests.exceptions:
                sleep(1)
            remaining_tries = remaining_tries - 1
        raise ResponseTimeout()

    def make_news_page_url_my(self, category_url, ds, de):
        made_urls = []
        while True:
            url = category_url + f'ds={ds}&de={de}' \
                f'&mynews=0&office_type=0&office_section_code=0&news_office_checked=&nso=so:dd,' \
                f'p:from{ds}to{de},a:all&start='
            totalpage = ArticleParser.find_news_totalpage(url + "10000")  # 어떨땐 400, 아닐땐 1~399중 하나
            request = self.get_url_data(url + f"{totalpage - 1}1")  # 마지막 페이지
            document = BeautifulSoup(request.content, 'html.parser')
            last_time_post = document.select('.api_subject_bx .group_news li .news_area .info_group span.info')

            if totalpage == 400 and len(last_time_post) >= 1:
                try:
                    last_time = datetime.datetime.strptime(last_time_post[-1].text, '%Y.%m.%d.')
                except:
                    pass
                    # breakpoint()
                for page in range(1, totalpage+1):
                    made_urls.append(url + f'{page-1}1')
                # 다했으면 de 날짜 바꾸기
                if last_time > datetime.datetime.strptime(ds, '%Y%m%d'):
                    de = last_time - datetime.timedelta(1)
                    de = de.strftime('%Y%m%d')
                    # print(f'바뀔 시작 : {ds}, 종료 : {de}')
                else:
                    break

            elif totalpage != 400:
                # print(totalpage - 1, '막지막 여기페이지는 여기올')  # totalpage가 400이 아니라 그 미만일 때
                for page in range(1, totalpage):
                    made_urls.append(url + f'{page}1')
                # breakpoint()
                # print('-' * 100)
                # print('\n최종 크롤링 페이지 수 :', len(made_urls))
                # print('-' * 100)
                break
            # print('누적 페이지 수:', len(made_urls))
        return made_urls

    def crawling(self, query, ds, de):
        # breakpoint()
        # Multi Process PID
        print(query + " PID: " + str(os.getpid()))
        # current = current_process()
        # pos = current._identity[0] - 1

        url_format = f'https://search.naver.com/search.naver?where=news&query=' \
                     f'{query}&sm=tab_opt&sort=1&photo=0&field=0&pd=3&'
        target_urls = self.make_news_page_url_my(url_format, ds, de)
        # print(query + " Urls are generated")
        # print("The naver_crawler starts")
        # print(len(target_urls))
        data = pd.DataFrame()
        for url in tqdm(target_urls, desc=f'[{query} {ds}_{de}]', leave=False):
            request = self.get_url_data(url)
            document = BeautifulSoup(request.content, 'html.parser')
            temp_post = document.select("a[href^='https://news.naver.com/main/read']")
            # 각 페이지에 있는 기사들의 url 저장
            post_urls = []
            for line in temp_post:
                # 해당되는 page에서 모든 기사들의 URL을 post_urls 리스트에 넣음
                post_urls.append(line.attrs['href'])
            # print(post_urls)
            del temp_post

            for content_url in post_urls:  # 기사 url
                # 크롤링 대기 시간
                sleep(0.01)
                # print(content_url)
                # 기사 HTML 가져옴
                request_content = self.get_url_data(content_url)

                try:
                    document_content = BeautifulSoup(request_content.content, 'html.parser')
                except:
                    # breakpoint()
                    continue

                try:
                    # 기사 제목 가져옴
                    tag_headline = document_content.find_all('h3', {'id': 'articleTitle'}, {'class': 'tts_head'})
                    # 뉴스 기사 제목 초기화
                    text_headline = ''
                    text_headline = text_headline + ArticleParser.clear_headline(
                        str(tag_headline[0].find_all(text=True)))
                    # 공백일 경우 기사 제외 처리
                    if not text_headline:
                        continue
                    # 기사 본문 가져옴
                    tag_content = document_content.find_all('div', {'id': 'articleBodyContents'})
                    # 뉴스 기사 본문 초기화
                    text_sentence = ''
                    text_sentence = text_sentence + ArticleParser.clear_content(str(tag_content[0].find_all(text=True)))
                    # 공백일 경우 기사 제외 처리
                    if not text_sentence:
                        continue
                    # print(text_sentence)
                    # 기사 언론사 가져옴
                    tag_company = document_content.find_all('meta', {'property': 'me2:category1'})

                    # 언론사 초기화
                    text_company = ''
                    text_company = text_company + str(tag_company[0].get('content'))

                    # 공백일 경우 기사 제외 처리
                    if not text_company:
                        continue
                    # print(text_company)
                    # 기사 시간대 가져옴
                    time = re.findall('<span class="t11">(.*)</span>', request_content.text)[0]

                    temp1 = pd.DataFrame(([
                        [time, query, text_company, text_headline, text_sentence, content_url]]),
                        columns=["일자", "종목명", "출처", "헤드라인", "기사", "url"])
                    data = pd.concat([data, temp1])
                    # del time
                    del text_company, text_sentence, text_headline
                    del tag_company
                    del tag_content, tag_headline
                    del request_content, document_content

                # UnicodeEncodeError
                except Exception as ex:
                    # breakpoint()
                    del request_content, document_content
                    pass

        output_path = f'../data/{query}'
        os.makedirs(f'{output_path}', exist_ok=True)
        csv_path = f'{output_path}/{query}_{ds}_{de}.csv'
        excel_path = f'{output_path}/{query}_{ds}_{de}.xlsx'

        data.to_excel(os.path.join(f'{excel_path}'), encoding="euc-kr", index=False)
        data.to_csv(os.path.join(f'{csv_path}'), encoding="utf-8", index=False, header=False)

    def date_range(self, start, end):
        start = datetime.datetime.strptime(start, "%Y%m%d")
        end = datetime.datetime.strptime(end, "%Y%m%d")
        dates = [(start + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range((end - start).days + 1)]
        return dates

    def chunks(self, lst, n):
        """Yield successive n-sized chunks from lst."""
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    def split_dates(self, ds, de, part_size):
        dates = self.date_range(ds, de)
        dates_list = list(self.chunks(dates, part_size))
        return dates_list

    def start(self, n_days):
        # MultiProcess 크롤링 시작
        procs = []
        for category_name in self.selected_queries: #selected_cate : 리스트임
            dates_list = self.split_dates(self.ds, self.de, n_days)
            for _, dates in enumerate(dates_list):
                ds, de = dates[0], dates[-1]
                proc = Process(target=self.crawling, args=(category_name, ds, de))
                procs.append(proc)
                proc.start()
        for proc in procs:
            proc.join()



    def start_single(self, n_days):
        for category_name in self.selected_queries: #selected_cate : 리스트임
            # dates_list = self.split_dates(self.ds, self.de, n_days)
            self.crawling(category_name, self.ds, self.de)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query', type=str, default='제3자+배정+유상증자')
    parser.add_argument('--start_date', type=str, default='20220220')
    parser.add_argument('--end_date', type=str, default='20220302')
    args = parser.parse_args(args=[])

    Crawler = ArticleCrawler()
    Crawler.set_category(args.query)#, '포스코','KT','검색어',...
    Crawler.set_date_range(args.start_date, args.end_date)
    Crawler.start_single(150)
