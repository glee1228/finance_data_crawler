import ssl, os
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import traceback
from time import sleep
from urllib import request
from zipfile import ZipFile
import xml.etree.ElementTree as ET

import argparse
from tqdm import tqdm

class DartCrawler(object):
    def __init__(self):
        self.file_path = None
        self.api_key = None
        self.selected_queries = []
        self.avail_query_codes = []
        self.corp_encoder = {}
        self.corp_decoder = {}
        self.is_daily_usage = True

    def set_file_path(self, path):
        self.file_path = path
        os.makedirs(self.file_path, exist_ok=True)

    def set_api_key(self, api_key):
        self.api_key = api_key

    def set_query_info(self, *args):
        corp_tree = dartCrawler.download_corp_info(self.file_path)
        if self.is_daily_usage:
            self.selected_queries = args
            root = corp_tree.getroot()

            for x in root:
                corp_code = x.findall('corp_code')[0].text
                corp_name = x.findall('corp_name')[0].text
                stock_code = x.findall('stock_code')[0].text
                # modify_date = x.findall('modify_date')[0].text

                if corp_name and len(stock_code) == 6:
                    self.corp_encoder[corp_name] = corp_code
                    self.corp_decoder[corp_code] = corp_name

            for query_name in self.selected_queries:
                root = corp_tree.getroot()
                query_code = None
                for x in root:
                    corp_code = x.findall('corp_code')[0].text
                    corp_name = x.findall('corp_name')[0].text
                    stock_code = x.findall('stock_code')[0].text
                    # modify_date = x.findall('modify_date')[0].text

                    if query_name == corp_name and len(stock_code) == 6:
                        query_code = corp_code
                        self.avail_query_codes.append(corp_code)
                        break

                if query_code is None:
                    print(f'{query_name}이 DART에서 조회되지 않습니다.')
                    raise KeyError

    def download_corp_info(self, path):
        context = ssl._create_unverified_context()
        url = "https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key=" + self.api_key
        print("요청주소 : " + url)

        data = request.urlopen(url, context=context)
        filename = data.info().get_filename()
        # print("파일명 : " + filename)
        if filename is None:
            self.is_daily_usage = False

        if self.is_daily_usage:
            xml_path = f'{path}/{filename.split(".")[0]}.xml'
            if not os.path.exists(xml_path):
                with open(filename, 'wb') as f:
                    f.write(data.read())
                    f.close

                print("다운로드 완료.")

                with ZipFile(filename, 'r') as zipObj:
                    zipObj.extractall(f'{path}/')  # 현재 디렉토리에 압축을 해제

                if os.path.isfile(filename):
                    os.remove(filename)  # 원본 압축파일 삭제

            xml_file = open(xml_path, 'rt', encoding='UTF8')
            corp_info = ET.parse(xml_file)
            return corp_info

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

    def get_paid_in_capital_increase_decision(self, corp_no, ds, de):
        assert isinstance(corp_no, str)
        url = f"https://opendart.fss.or.kr/api/piicDecsn.xml?crtfc_key={self.api_key}&corp_code={corp_no}&bgn_de={ds}&end_de={de}"
        # print(url)
        resultXML = urlopen(url)
        result = resultXML.read()

        xmlsoup = BeautifulSoup(result, 'html.parser')
        te = xmlsoup.findAll("list")
        if len(te) == 0:
            return None

        dates_list = self.split_dates(ds, de, 1)
        data = pd.DataFrame()
        for _, dates in enumerate(dates_list):
            ds, de = dates[0], dates[-1]
            try:
                # 유상증자 결정
                sleep(0.002)
                url1 = f"https://opendart.fss.or.kr/api/piicDecsn.xml?crtfc_key={self.api_key}&corp_code={corp_no}&bgn_de={ds}&end_de={de}"
                # print(url1)
                resultXML1 = urlopen(url1)
                result1 = resultXML1.read()

                xmlsoup1 = BeautifulSoup(result1, 'html.parser')
                te1 = xmlsoup1.findAll("list")
                # 컬럼명 바꿔 정렬
                for t1 in te1:
                    temp1 = pd.DataFrame(([
                        [t1.rcept_no.string, ds, t1.corp_name.string, t1.corp_code.string,
                         t1.corp_cls.string, t1.nstk_ostk_cnt.string, t1.nstk_estk_cnt.string, t1.fv_ps.string,
                         t1.bfic_tisstk_ostk.string, t1.bfic_tisstk_estk.string, t1.fdpp_fclt.string, t1.fdpp_bsninh.string,
                         t1.fdpp_op.string, t1.fdpp_dtrp.string, t1.fdpp_ocsa.string, t1.fdpp_etc.string,
                         t1.ic_mthn.string,t1.ssl_at.string,t1.ssl_bgd.string, t1.ssl_edd.string]]),
                        columns=["접수번호", "접수일자", "회사명", "고유번호", "법인구분", "신주의 종류와 수(보통주식 (주))", "신주의 종류와 수(기타주식 (주))", "1주당 액면가액 (원)",
                                 "증자전 발행주식총수 (주)(보통주식 (주))", "증자전 발행주식총수 (주)(기타주식 (주))", "자금조달의 목적(시설자금 (원))","자금조달의 목적(영업양수자금 (원))",
                                 "자금조달의 목적(운영자금 (원))","자금조달의 목적(채무상환자금 (원))","자금조달의 목적(타법인 증권 취득자금 (원))","자금조달의 목적(기타자금 (원))",
                                 "증자방식","공매도 해당여부","공매도 시작일","공매도 종료일"])
                    data = pd.concat([data, temp1])


            except Exception as e:
                print('error')
                print(traceback.format_exc())

        if len(data) != 0:
            # 둘 이상의 Dataframe이 동일한 컬럼을 갖고 있다는 가정에서 row가 늘어하는 형태로 데이터가 늘어납니다.
            file_nm = f"{corp_no}_유상증자결정.xlsx"
            os.makedirs(f'{self.file_path}/{corp_no}', exist_ok=True)
            data.to_excel(os.path.join(f'{self.file_path}/{corp_no}/{file_nm}'), encoding="euc-kr", index=False)
        return corp_no

    def get_largest_shareholder(self, corp_no):
        assert isinstance(corp_no, str)
        try:
            # 최대주주 현황
            bsns_year_lst = range(2014, 2023)
            report_code_lst = [11011,11013,11012,11014]
            data = pd.DataFrame()
            for bsns_year in bsns_year_lst:
                for report_code in report_code_lst:
                    url1 = f"https://opendart.fss.or.kr/api/hyslrSttus.xml?crtfc_key={self.api_key}&corp_code={corp_no}&bsns_year={bsns_year}&reprt_code={report_code}"
                    print(url1)
                    resultXML1 = urlopen(url1)
                    result1 = resultXML1.read()

                    xmlsoup1 = BeautifulSoup(result1, 'html.parser')

                    te1 = xmlsoup1.findAll("list")

                    # 컬럼명 바꿔 정렬
                    for t1 in te1:
                        temp1 = pd.DataFrame(([
                            [t1.rcept_no.string, t1.corp_name.string, t1.corp_code.string,
                             t1.corp_cls.string, t1.nm.string,
                             t1.stock_knd.string,
                             t1.bsis_posesn_stock_co.string,
                             t1.bsis_posesn_stock_qota_rt.string, t1.trmend_posesn_stock_co.string,t1.trmend_posesn_stock_qota_rt.string,
                             t1.rm.string]]),
                            columns=["접수번호", "회사명", "종목코드", "법인구분", "최대 주주 명", "주식 종류", "기초 소유 주식 수", "기초 소유 주식 지분 율",
                                     "기말 소유 주식 수", "기말 소유 주식 지분 율", "비고"])
                        data = pd.concat([data, temp1])

            if len(data) != 0:
                # 둘 이상의 Dataframe이 동일한 컬럼을 갖고 있다는 가정에서 row가 늘어하는 형태로 데이터가 늘어납니다.
                file_nm = f"{corp_no}_최대주주현황.xlsx"
                os.makedirs(f'{self.file_path}/{corp_no}', exist_ok=True)
                data.to_excel(os.path.join(f'{self.file_path}/{corp_no}/{file_nm}'), encoding="euc-kr", index=False)

        except Exception as e:
            print('error')
            print(traceback.format_exc())

    def get_largest_shareholder_change(self, corp_no):
        assert isinstance(corp_no, str)
        try:
            # 최대주주 변동 현황
            bsns_year_lst = range(2014, 2023)
            report_code_lst = [11011,11013,11012,11014]
            data = pd.DataFrame()
            for bsns_year in bsns_year_lst:
                for report_code in report_code_lst:
                    url1 = f"https://opendart.fss.or.kr/api/hyslrChgSttus.xml?crtfc_key={self.api_key}&corp_code={corp_no}&bsns_year={bsns_year}&reprt_code={report_code}"
                    print(url1)
                    resultXML1 = urlopen(url1)
                    result1 = resultXML1.read()

                    xmlsoup1 = BeautifulSoup(result1, 'html.parser')

                    te1 = xmlsoup1.findAll("list")

                    # 컬럼명 바꿔 정렬
                    for t1 in te1:
                        temp1 = pd.DataFrame(([
                            [t1.rcept_no.string, t1.change_on.string, t1.corp_name.string, t1.corp_code.string,
                             t1.corp_cls.string, t1.mxmm_shrholdr_nm.string, t1.posesn_stock_co.string, t1.qota_rt.string, t1.change_cause.string,
                             t1.rm.string]]),
                            columns=["접수번호", "변동 일" ,  "회사명", "종목코드", "법인구분", "최대 주주 명", "소유 주식 수", "지분 율",
                                     "변동 원인", "비고"])
                        data = pd.concat([data, temp1])

            if len(data) != 0:
                # 둘 이상의 Dataframe이 동일한 컬럼을 갖고 있다는 가정에서 row가 늘어하는 형태로 데이터가 늘어납니다.
                file_nm = f"{corp_no}_최대주주변동현황.xlsx"
                os.makedirs(f'{self.file_path}/{corp_no}', exist_ok=True)
                data.to_excel(os.path.join(f'{self.file_path}/{corp_no}/{file_nm}'), encoding="euc-kr", index=False)

        except Exception as e:
            print('error')
            print(traceback.format_exc())

    def get_large_possession(self, corp_no):
        assert isinstance(corp_no, str)
        try:
            # 대량보유상황보고서
            url1 = "https://opendart.fss.or.kr/api/majorstock.xml?crtfc_key=" + self.api_key + "&corp_code=" + corp_no
            print(url1)
            resultXML1 = urlopen(url1)
            result1 = resultXML1.read()

            xmlsoup1 = BeautifulSoup(result1, 'html.parser')

            data = pd.DataFrame()
            te1 = xmlsoup1.findAll("list")

            # 컬럼명 바꿔 정렬
            for t1 in te1:
                temp1 = pd.DataFrame(([
                    [t1.rcept_no.string, t1.rcept_dt.string, t1.corp_name.string, t1.corp_code.string,
                     t1.report_tp.string,
                     t1.repror.string, t1.stkqy.string, t1.stkqy_irds.string, t1.stkrt.string, t1.stkrt_irds.string,
                     t1.ctr_stkqy.string,
                     t1.ctr_stkrt.string, t1.report_resn.string]]),
                    columns=["접수번호", "접수일자", "회사명", "종목코드", "보고구분", "대표보고자", "보유주식등의 수", "보유주식등의 증감",
                             "보유비율", "보유비율 증감", "주요체결 주식등의 수", "주요체결 보유비율", "보고사유"])
                data = pd.concat([data, temp1])

            if len(data) != 0:
                # 둘 이상의 Dataframe이 동일한 컬럼을 갖고 있다는 가정에서 row가 늘어하는 형태로 데이터가 늘어납니다.
                file_nm = f"{corp_no}_대량보유상황보고서.xlsx"
                os.makedirs(f'{self.file_path}/{corp_no}', exist_ok=True)
                data.to_excel(os.path.join(f'{self.file_path}/{corp_no}/{file_nm}'), encoding="euc-kr", index=False)

        except Exception as e:
            print(traceback.format_exc())

    def get_major_shareholders(self, corp_no):
        assert isinstance(corp_no, str)
        try:
            # 임원주요주주 소유 보고서
            url2 = "https://opendart.fss.or.kr/api/elestock.xml?crtfc_key=" + self.api_key + "&corp_code=" + corp_no
            print(url2)

            resultXML2 = urlopen(url2)
            result2 = resultXML2.read()

            xmlsoup2 = BeautifulSoup(result2, 'html.parser')
            status_list = xmlsoup2.findAll("status")[0].get_text()
            data, te2 = [], []
            # 조회된 데이터가 없을 때 처리
            if status_list == "NON_DATA":
                print("조회된 데이터가 없습니니다.")
            else:
                data = pd.DataFrame()
                te2 = xmlsoup2.findAll("list")

            for t2 in te2:
                temp2 = pd.DataFrame(
                    ([[t2.rcept_no.string, t2.rcept_dt.string, t2.corp_name.string, t2.corp_code.string,
                       t2.repror.string, t2.isu_exctv_rgist_at.string, t2.isu_exctv_ofcps.string,
                       t2.isu_main_shrholdr.string, t2.sp_stock_lmp_cnt.string, t2.sp_stock_lmp_irds_cnt.string,
                       t2.sp_stock_lmp_rate.string, t2.sp_stock_lmp_irds_rate.string]]),
                    columns=["접수번호", "접수일자", "회사명", "회사번호", "보고자", "발행 회사 관계 임원(등기여부)", "발행 회사 관계 임원 직위",
                             "발행 회사 관계 주요 주주", "특정 증권 등 소유 수",
                             "특정 증권 등 소유 증감 수 ", "특정 증권 등 소유 비율", "특정 증권 등 소유 증감 비율"])
                data = pd.concat([data, temp2])

            if len(data) != 0:
                file_nm = f"{corp_no}_임원주요주주_소유_보고서.xlsx"
                os.makedirs(f'{self.file_path}/{corp_no}',exist_ok=True)
                data.to_excel(os.path.join(f'{self.file_path}/{corp_no}/{file_nm}'), encoding="euc-kr", index=False)

        except Exception as e:
            print(traceback.format_exc())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--query_name', type=str, default='에치에프알')
    parser.add_argument('--file_path',type=str,default='./data')
    parser.add_argument('--start_date', type=str, default='20220220')
    parser.add_argument('--end_date', type=str, default='20220302')
    parser.add_argument('--api_key', type=str, default='6f7c4a97fbac7c000f8a17c677f64faaddcfd094')
    args = parser.parse_args(args=[])

    dartCrawler = DartCrawler()
    api_key_list = ['39849de683995a798ae30f7ceecf0bc4a7c923bf',
                    '17d85a9fa96c26649c024277fafbfce9d597af66',
                    '7f6ee52d85573132981012eb8a73ab95a4aaa86b',
                    '1bb59e2a4ed4ced80e181370f312d48a424d4394',
                    '6f7c4a97fbac7c000f8a17c677f64faaddcfd094']

    for api_key in api_key_list:
        dartCrawler.set_api_key(api_key)
        dartCrawler.set_query_info(args.query_name)
        dartCrawler.set_file_path(args.file_path)
        if dartCrawler.is_daily_usage:
            # avail_cap_inc_corps = []
            # for query_code in tqdm(dartCrawler.corp_encoder.values()):
            #     corp_no = dartCrawler.get_paid_in_capital_increase_decision(query_code, args.start_date, args.end_date)
            #     if corp_no is not None:
            #         avail_cap_inc_corps.append(corp_no)

            avail_cap_inc_corps = os.listdir('./data')
            for query_code in avail_cap_inc_corps:
                dartCrawler.get_major_shareholders(query_code)

            for query_code in avail_cap_inc_corps:
                dartCrawler.get_largest_shareholder(query_code)

            for query_code in avail_cap_inc_corps:
                dartCrawler.get_largest_shareholder_change(query_code)

            for query_code in avail_cap_inc_corps:
                dartCrawler.get_large_possession(query_code)

            print('종료')
            break

        else:
            print('일일 사용량 초과. 다음 키를 사용합니다.')






