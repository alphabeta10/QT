import akshare as ak
import json
import pandas as pd
from utils.actions import try_get_action, show_data
from data.stock_detail_fin import get_stock_info_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
from tqdm import tqdm
from datetime import datetime,timedelta

rename_cols = {"报告日期":"date","分类类型":"class_dire","主营构成":"class",'股票代码':'code'}

def get_zygc_em(code='000001'):
    stock_zygc_ym_df = try_get_action(ak.stock_zygc_em,try_count=1,symbol=code)
    if stock_zygc_ym_df is not None and stock_zygc_ym_df.empty is False:
        return stock_zygc_ym_df
    print(f'error get data from {code}')
    return None

def get_code_by_date(date_str:str):
    ticker_daily = get_mongo_table(collection="business")
    condition = {"date":date_str}
    ticker_cursor = ticker_daily.find(condition,projection={"_id":False,"code":True,})
    codes = []
    for ticker in ticker_cursor:
        ts_code = ticker['code']
        if ts_code not in codes:
            codes.append(ts_code)
    return codes

def product_data_to_mongo(indb_str=None):
    codes = get_stock_info_data()
    business = get_mongo_table(collection='business')
    if indb_str is None:
        if datetime.now().month>7:
            indb_str = datetime.now().strftime("%Y-06-30")
        else:
            indb_str = str(datetime.now().year-1)+"-12-31"

    in_db_codes = get_code_by_date(indb_str)
    print("in db len is ",len(in_db_codes),in_db_codes)
    for code in tqdm(codes):
        if ("SZ" in code.upper() or "SH" in code.upper()) and code[2:] not in in_db_codes:
            code = code.upper()
            pd_data = get_zygc_em(code)
            if pd_data is not None:
                request_update = []
                pd_data = pd_data.rename(columns=rename_cols)
                for index in pd_data.index:
                    dict_data = dict(pd_data.loc[index])
                    dict_data['date'] = str(dict_data['date'])
                    dict_data['主营收入'] = float(dict_data['主营收入'])
                    dict_data['主营成本'] = float(dict_data['主营成本'])
                    dict_data['主营利润'] = float(dict_data['主营利润'])
                    request_update.append(UpdateOne(
                        {"code": dict_data['code'], "date": dict_data['date'],"class_dire":dict_data['class_dire'],"class":dict_data['class']},
                        {"$set": dict_data},
                        upsert=True))
                try:
                    mongo_bulk_write_data(business,request_update)
                except Exception as e:
                    print(e)
                    print(f"error code {code}")
                    print(pd_data)
        else:
            pass





def get_product_list(pd_data:pd.DataFrame):
    pd_data.sort_values(by='report_date',inplace=True,ascending=False)
    report_dict = {}
    for index in pd_data.index:
        ele = pd_data.loc[index]
        class_dire = ele['class_dire']
        class_name = ele['class']
        report_date = ele['report_date']
        if report_date not in report_dict.keys():
            report_dict[report_date] = set()
        if class_dire=='按产品分':
            if class_name not in ['合计','其他业务','其他'] and '其他业务' not in class_name:
                report_dict[report_date].add(class_name)
    key_sort = sorted(report_dict,reverse=True)
    report_dict = {k:list(v) for k,v in report_dict.items() if len(list(v))>0}
    product_list = None
    for key in key_sort:
        if key in report_dict.keys() and len(list(report_dict[key]))>0:
            product_list = list(report_dict[key])
            break
    return product_list,report_dict

def construct_graph():
    code_list = ['600111', '000629', '600989', '600884', '002709', '600132', '603589', '600600', '000895',"605117","002459"]
    for code in code_list:
        pd_data = get_zygc_em(code)
        if pd_data is not None:
            pd_data = pd_data.rename(columns=rename_cols)
            product_list,report_dict = get_product_list(pd_data)
            if len(product_list) > 0:
                dict_data = {"code": code, "product_list": product_list}
                print(json.dumps(dict_data, ensure_ascii='utf8'))
                print(report_dict)


def create_index():
    business = get_mongo_table(database='stock', collection='business')
    business.create_index([("code", 1), ("date", 1),("class_dire",1),("class",1)],unique=True,background=True)


if __name__ == '__main__':
    product_data_to_mongo(indb_str='2026-06-30')

