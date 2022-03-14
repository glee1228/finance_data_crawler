# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from request import Request

def get_stock_market_list(corp_cls: str, include_corp_name=True) -> dict:
    """ 상장 회사 dictionary 반환
    Parameters
    ----------
    corp_cls: str
        Y: stock market(코스피), K: kosdaq market(코스닥), N: konex Market(코넥스)
    include_corp_name: bool, optional
        if True, returning dictionary includes corp_name(default: True)
    Returns
    -------
    dict of {stock_code: information}
        상장 회사 정보 dictionary 반환(회사 이름, 섹터, 물품)
    """

    if corp_cls.upper() == 'E':
        raise ValueError('ETC market is not supported')

    corp_cls_to_market = {
        "Y": "stockMkt",
        "K": "kosdaqMkt",
        "N": "konexMkt",
    }

    url = 'https://kind.krx.co.kr/corpgeneral/corpList.do'
    referer = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage'

    market_type = corp_cls_to_market[corp_cls.upper()]
    payload = {
        'method': 'download',
        'pageIndex': 1,
        'currentPageSize': 5000,
        'orderMode': 3,
        'orderStat': 'D',
        'searchType': 13,
        'marketType': market_type,
        'fiscalYearEnd': 'all',
        'location': 'all',
    }

    stock_market_list = dict()
    # Request object
    request = Request()
    resp = request.post(url=url, payload=payload, referer=referer)
    html = BeautifulSoup(resp.text, 'html.parser')
    rows = html.find_all('tr')

    for row in rows:
        cols = row.find_all('td')
        if len(cols) > 0:
            corp_name = cols[0].text.strip()
            stock_code = cols[1].text.strip()
            sector = cols[2].text.strip()
            product = cols[3].text.strip()
            corp_info = {'sector': sector,
                         'product': product, 'corp_cls': corp_cls}
            if include_corp_name:
                corp_info['corp_name'] = corp_name
            stock_market_list[stock_code] = corp_info

    return stock_market_list

# def get_stock_financial_statistics(corp_no_list, year, report_num):
#     url = 'https://kind.krx.co.kr/compfinance/financialinfo.do'
#     referer = 'https://kind.krx.co.kr/compfinance/financialinfo.do?method=loadInitPage&searchgubun=corporation'
# 
#     corp_no_str = corp_no_list[0]
#     for corp_no in corp_no_list:
#         corp_no_str += f'&arrIsurCd={corp_no}'
# 
#     report_type = {0:'1_4',1:'half',2:'3_4',3:'accntclosing',4:''}
# 
#     payload = {
#         'method': 'download',
#         'acntgType': 'I',
#         'currentPageSize': 5000,
#         'orderMode': 'A010',
#         'orderStat': 'D',
#         'searchType': 13,
#         'arrIsurCd': corp_no_str,
#         'fininfotype': 'finstat',
#         'fiscalyear': 'all',
#     }
# 
#     stock_market_list = dict()
#     # Request object
#     request = Request()
#     resp = request.post(url=url, payload=payload, referer=referer)
#     html = BeautifulSoup(resp.text, 'html.parser')
#     rows = html.find_all('tr')
# 
#     for row in rows:
#         cols = row.find_all('td')
#         if len(cols) > 0:
#             corp_name = cols[0].text.strip()
#             stock_code = cols[1].text.strip()
#             sector = cols[2].text.strip()
#             product = cols[3].text.strip()
#             corp_info = {'sector': sector,
#                          'product': product, 'corp_cls': corp_cls}
#             if include_corp_name:
#                 corp_info['corp_name'] = corp_name
#             stock_market_list[stock_code] = corp_info
# 
#     return stock_market_listk_financial_statistics(corp_no_list, year, report_num):
#     url = 'https://kind.krx.co.kr/compfinance/financialinfo.do'
#     referer = 'https://kind.krx.co.kr/compfinance/financialinfo.do?method=loadInitPage&searchgubun=corporation'
# 
#     corp_no_str = corp_no_list[0]
#     for corp_no in corp_no_list:
#         corp_no_str += f'&arrIsurCd={corp_no}'
# 
#     report_type = {0:'1_4',1:'half',2:'3_4',3:'accntclosing',4:''}
# 
#     payload = {
#         'method': 'download',
#         'acntgType': 'I',
#         'currentPageSize': 5000,
#         'orderMode': 'A010',
#         'orderStat': 'D',
#         'searchType': 13,
#         'arrIsurCd': corp_no_str,
#         'fininfotype': 'finstat',
#         'fiscalyear': 'all',
#     }
# 
#     stock_market_list = dict()
#     # Request object
#     request = Request()
#     resp = request.post(url=url, payload=payload, referer=referer)
#     html = BeautifulSoup(resp.text, 'html.parser')
#     rows = html.find_all('tr')
# 
#     for row in rows:
#         cols = row.find_all('td')
#         if len(cols) > 0:
#             corp_name = cols[0].text.strip()
#             stock_code = cols[1].text.strip()
#             sector = cols[2].text.strip()
#             product = cols[3].text.strip()
#             corp_info = {'sector': sector,
#                          'product': product, 'corp_cls': corp_cls}
#             if include_corp_name:
#                 corp_info['corp_name'] = corp_name
#             stock_market_list[stock_code] = corp_info
# 
#     return stock_market_list

stock_market_list = get_stock_market_list('Y',True)

breakpoint()