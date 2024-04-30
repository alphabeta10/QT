from utils.actions import try_get_action
from pymongo import UpdateOne
import akshare as ak
from tqdm import tqdm
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data
import requests
import pandas as pd

def get_stock_info_data():
    ticker_info = get_mongo_table(collection='ticker_info')
    tickers_cursor = ticker_info.find(projection={'_id': False, 'ts_code': True})
    new_codes = []
    for ticker in tickers_cursor:
        ts_code = ticker['ts_code']
        code, lr = ts_code.split(".")
        if lr == 'SH':
            new_codes.append(f"sh{code}")
        if lr == 'SZ':
            new_codes.append(f"sz{code}")
        if lr == 'BJ':
            new_codes.append(f"bj{code}")
    return new_codes


def common_stock_fin_report_em(request_update: list, symbol, data_type='zcfz_report_detail'):
    """
        按报告期东方财富数据获取资产负债，现金流量，利润数据
        :param request_update:
        :param symbol:
        :return:
        """
    api_mapping = {
        "zcfz_report_detail": ak.stock_balance_sheet_by_report_em,
        "cash_flow_report_em_detail": ak.stock_cash_flow_sheet_by_report_em,
        "profit_report_em_detail": ak.stock_profit_sheet_by_report_em
    }
    df_data = try_get_action(api_mapping.get(data_type), try_count=3, symbol=symbol)
    if df_data is not None:
        for index in df_data.index:
            dict_data = dict(df_data.loc[index])
            dict_data['code'] = symbol[2:]
            dict_data['date'] = dict_data.get("REPORT_DATE")[0:10]
            new_dict = {}
            for k, v in dict_data.items():
                if str(v) in ['None', 'nan']:
                    pass
                else:
                    new_dict[k] = str(v)
            request_update.append(UpdateOne(
                {"code": new_dict['code'], "date": new_dict['date'], "data_type": data_type},
                {"$set": new_dict},
                upsert=True))
    return df_data


def get_stock_financial_analysis_indicator(request_update: list, symbol="603288"):
    df_data = try_get_action(ak.stock_financial_analysis_indicator, try_count=3, symbol=symbol)
    if df_data is not None:
        for index in df_data.index:
            dict_data = dict(df_data.loc[index])
            dict_data['code'] = symbol
            dict_data['data_type'] = "fin_indicator"
            dict_data['date'] = dict_data.get("日期", "").replace("-", "")
            request_update.append(UpdateOne(
                {"code": dict_data['code'], "date": dict_data['date'], "data_type": "fin_indicator"},
                {"$set": dict_data},
                upsert=True))


def handle_fin_analysis_indicator(codes=None):
    if codes is None:
        codes = get_stock_info_data()
    request_update = []
    fin_col = get_mongo_table(collection='fin')
    for code in tqdm(codes):
        if "sz" in code or "sh" in code:
            code = code[2:]
        get_stock_financial_analysis_indicator(request_update, symbol=code)
        if len(request_update) > 5000:
            mongo_bulk_write_data(fin_col, request_update)
            request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(fin_col, request_update)


def handle_comm_stock_fin_em(codes=None, data_type="zcfz_report_detail"):
    if codes is None:
        codes = get_stock_info_data()
    request_update = []
    fin_col = get_mongo_table(collection='fin')
    for code in tqdm(codes):
        if "sz" in code or "sh" in code:
            code = code.upper()
            common_stock_fin_report_em(request_update, symbol=code, data_type=data_type)
            if len(request_update) > 5000:
                mongo_bulk_write_data(fin_col, request_update)
                request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(fin_col, request_update)

def get_stock_em_metric_data(request_update:list,code=None):
    if code is None:
        code = 'SZ000001'
    if request_update is None:
        request_update = []
    url = f'https://emweb.securities.eastmoney.com/PC_HSF10/NewFinanceAnalysis/ZYZBAjaxNew?type=0&code={code}'
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Cookie": "_trs_uv=ld8nyuch_6_10t8; wzws_sessionid=oGYenO+AMTQuMTU1LjEzOS4xNTOBMDJhYWFhgmZjNWVlMQ==; u=2; JSESSIONID=DIfu5jBC7Rk_wHgprZXDGNikJl7W0d6pkaMD7LMCQZX8rb7Z4oge!460167158; wzws_cid=0fd9f5b2faab8659d5dd69df23c2814722c6acaff4f558615968963c3a9392071577c0015b12aaeedf5ab7b5701849eca6564a3d1be89a7794c7e7d5c3f6fd637fe956cb95f77ec62ef646d8643596e3"}
    response = requests.get(url, headers=headers)
    if response.status_code==200:
        df = pd.DataFrame(response.json()['data'])
        for index in df.index:
            dict_data = dict(df.loc[index])
            dict_data['data_type'] = "stock_em_metric"
            dict_data['code'] = dict_data['SECURITY_CODE']
            dict_data['date'] = str(dict_data['REPORT_DATE'])[0:10]
            new_dict = {}
            for k, v in dict_data.items():
                if str(v) in ['None', 'nan']:
                    pass
                else:
                    new_dict[k] = str(v)
            request_update.append(UpdateOne(
                {"code": new_dict['code'], "date": new_dict['date'], "data_type": new_dict['data_type']},
                {"$set": new_dict},
                upsert=True))

def handle_em_stock_metric(codes=None):
    if codes is None:
        return
    request_update = []
    fin_col = get_mongo_table(collection='fin')
    for code in codes:
        get_stock_em_metric_data(request_update,code)
        if len(request_update) > 5000:
            mongo_bulk_write_data(fin_col, request_update)
            request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(fin_col, request_update)


def get_data():
    fin_col = get_mongo_table(collection='fin')
    fin_col.create_index([("date", 1), ("code", 1), ("data_type", 1)], unique=True, background=True)

if __name__ == '__main__':
    pass
