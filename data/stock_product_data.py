import akshare as ak
import json
import pandas as pd
from utils.actions import try_get_action
from data.stock_detail_fin import get_stock_info_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
from tqdm import tqdm

rename_cols = {"报告期":"date","分类方向":"class_dire","分类":"class"}

def get_zygc_ym(code='000001'):
    stock_zygc_ym_df = try_get_action(ak.stock_zygc_ym,try_count=3,symbol=code)
    if stock_zygc_ym_df is not None and stock_zygc_ym_df.empty is False:
        stock_zygc_ym_df['code'] = code
        return stock_zygc_ym_df
    return None


def product_data_to_mongo():
    codes = get_stock_info_data()
    business = get_mongo_table(collection='business')
    for code in tqdm(codes):
        if "sz" in code or "sh" in code:
            code = code[2:]
            pd_data = get_zygc_ym(code)
            if pd_data is not None:
                request_update = []
                pd_data = pd_data.rename(columns=rename_cols)
                for index in pd_data.index:
                    dict_data = dict(pd_data.loc[index])
                    dict_data['code'] = code
                    request_update.append(UpdateOne(
                        {"code": dict_data['code'], "date": dict_data['date'],"class_dire":dict_data['class_dire'],"class":dict_data['class']},
                        {"$set": dict_data},
                        upsert=True))
                mongo_bulk_write_data(business,request_update)






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
        pd_data = get_zygc_ym(code)
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
    product_data_to_mongo()

