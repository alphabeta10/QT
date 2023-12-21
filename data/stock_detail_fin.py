from utils.actions import try_get_action,show_data
from pymongo import UpdateOne
import akshare as ak
from tqdm import tqdm
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data

def get_stock_info_data():
    ticker_info = get_mongo_table(collection='ticker_info')
    tickers_cursor = ticker_info.find(projection={'_id': False,'ts_code':True})
    new_codes = []
    for ticker in tickers_cursor:
        ts_code = ticker['ts_code']
        code,lr = ts_code.split(".")
        if lr=='SH':
            new_codes.append(f"sh{code}")
        if lr=='SZ':
            new_codes.append(f"sz{code}")
        if lr=='BJ':
            new_codes.append(f"bj{code}")
    return new_codes


def stock_balance_sheet_by_report_em(request_update:list,symbol):
    """
    资产负债按报告期东方财富数据
    :param request_update:
    :param symbol:
    :return:
    """
    df_data = try_get_action(ak.stock_balance_sheet_by_report_em,try_count=3,symbol=symbol)
    if df_data is not None:
        for index in df_data.index:
            dict_data = dict(df_data.loc[index])
            dict_data['code'] = symbol[2:]
            dict_data['date'] = dict_data.get("REPORT_DATE")[0:10]
            new_dict = {}
            for k,v in dict_data.items():
                if str(v) in ['None','nan']:
                    pass
                else:
                    new_dict[k] = str(v)
            request_update.append(UpdateOne(
                {"code": new_dict['code'], "date": new_dict['date'], "data_type": "zcfz_report_detail"},
                {"$set": new_dict},
                upsert=True))
    return df_data


def common_stock_fin_report_em(request_update:list,symbol,data_type='zcfz_report_detail'):
    """
        按报告期东方财富数据获取资产负债，现金流量，利润数据
        :param request_update:
        :param symbol:
        :return:
        """
    api_mapping = {
        "zcfz_report_detail":ak.stock_balance_sheet_by_report_em,
        "cash_flow_report_em_detail":ak.stock_cash_flow_sheet_by_report_em,
        "profit_report_em_detail":ak.stock_profit_sheet_by_report_em
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
                {"code": new_dict['code'], "date": new_dict['date'], "data_type":data_type },
                {"$set": new_dict},
                upsert=True))
    return df_data



def get_stock_financial_analysis_indicator(request_update:list,symbol="603288"):
    df_data = try_get_action(ak.stock_financial_analysis_indicator,try_count=3,symbol=symbol)
    if df_data is not None:
        for index in df_data.index:
            dict_data = dict(df_data.loc[index])
            dict_data['code'] = symbol
            dict_data['data_type'] = "fin_indicator"
            dict_data['date'] = dict_data.get("日期","").replace("-","")
            request_update.append(UpdateOne(
                {"code": dict_data['code'], "date": dict_data['date'], "date_type":"fin_indicator"},
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
        get_stock_financial_analysis_indicator(request_update,symbol=code)
        if len(request_update)>5000:
            mongo_bulk_write_data(fin_col,request_update)
            request_update.clear()
    if len(request_update)>0:
        mongo_bulk_write_data(fin_col,request_update)



def handle_comm_stock_fin_em(codes=None, data_type="zcfz_report_detail"):
    if codes is None:
        codes = get_stock_info_data()
    request_update = []
    fin_col = get_mongo_table(collection='fin')
    for code in tqdm(codes):
        if "sz" in code or "sh" in code:
            code = code.upper()
            common_stock_fin_report_em(request_update,symbol=code,data_type=data_type)
            if len(request_update)>5000:
                mongo_bulk_write_data(fin_col,request_update)
                request_update.clear()
    if len(request_update)>0:
        mongo_bulk_write_data(fin_col,request_update)


def get_data():
    fin_col = get_mongo_table(collection='fin')
    fin_col.delete_many({"date_type":"zcfz_report_detail"})



if __name__ == '__main__':
    df = stock_balance_sheet_by_report_em([],"sz000063")
    show_data(df.head())

