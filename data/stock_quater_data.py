import sys
import os

import pandas as pd

from data.board_data import show_data
from utils.tool import mongo_bulk_write_data

#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import akshare as ak
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from tqdm import tqdm
from utils.actions import try_get_action
from data.stock_daily_data import get_stock_info_data

def stock_profile_cninfo(symbols=None):
    """
    stock_common数据库存储的格式是 metric_code data_type 为主键的数据
    :return:
    """
    if symbols is None:
        symbols = ['002555']
    stock_common = get_mongo_table(collection='stock_common')
    request_update = []
    for symbol in symbols:
        stock_profile_cninfo_df = try_get_action(ak.stock_profile_cninfo,try_count=3,symbol=symbol)
        if stock_profile_cninfo_df is not None:
            for index in stock_profile_cninfo_df.index:
                dict_data = dict(stock_profile_cninfo_df.iloc[index])
                dict_data['metric_code'] = symbol
                dict_data['data_type'] = 'company_profile'
                request_update.append(UpdateOne(
                    {"metric_code": dict_data['metric_code'], "data_type": dict_data['data_type']},
                    {"$set": dict_data},
                    upsert=True))
                if len(request_update) > 100:
                    mongo_bulk_write_data(stock_common, request_update)
                    request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(stock_common, request_update)
        request_update.clear()


def stock_gdfx_holding_analyse_em(time='20240930'):
    file_name = time+"_holder.csv"
    three_code_db = get_mongo_table(collection='three_code_db')
    if os.path.exists(file_name):
        print("本地读取数据")
        stock_gdfx_holding_analyse_em_df = pd.read_csv(file_name,dtype={"股票代码":str})
    else:
        print("网上拉取数据")
        stock_gdfx_holding_analyse_em_df = ak.stock_gdfx_holding_analyse_em(date=time)
        stock_gdfx_holding_analyse_em_df.to_csv(file_name,index=False)
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    name_vol_dict = {}
    for index in stock_zh_a_spot_em_df.index:
        dict_data = dict(stock_zh_a_spot_em_df.loc[index])
        name = dict_data['名称']
        dict_data['metric_code'] = dict_data['代码']
        dict_data['data_type'] = 'stock_indicator'
        total_vol = dict_data['总市值'] / dict_data['最新价']
        name_vol_dict[dict_data['代码']] = total_vol

    name_rate = stock_gdfx_holding_analyse_em_df[['股东名称','股票代码','股票简称','期末持股-数量']]
    graph_edge_dict = {}
    for index in name_rate.index:
        dict_data = dict(name_rate.iloc[index])
        gd_name = dict_data['股东名称']
        stock_code = dict_data['股票代码']
        stock_name = dict_data['股票简称']
        holder_num = float(dict_data['期末持股-数量'])
        graph_edge_dict.setdefault(gd_name,{})
        holder_rate = round(holder_num/name_vol_dict.get(stock_code,1),4)
        # graph_edge_dict[gd_name].append([stock_code,stock_name,holder_rate])
        holder_rate = holder_rate if holder_rate<=1 else ''
        graph_edge_dict[gd_name][stock_code] = {"name":stock_name,"holder_rate":holder_rate,"holder_num":holder_num,"code":stock_code}
    request_update = []
    for index in name_rate.index:
        dict_data = dict(name_rate.iloc[index])
        stock_code = dict_data['股票代码']
        stock_name = dict_data['股票简称']
        gd_name = dict_data['股东名称']
        all_dict_data = graph_edge_dict.get(gd_name)
        all_dict_data = list(all_dict_data.values())
        new_dict = {}
        result_dict = {"code":stock_code,"name":stock_name,"gd_name":gd_name,"gd_other_company":all_dict_data}
        new_dict['data_type'] = 'gd_other_company_rate'
        new_dict['metric_code'] = result_dict['code']
        new_dict['sub_metric_code'] = result_dict['gd_name']
        new_dict['time'] = time
        new_dict['gd_other_company'] = all_dict_data

        request_update.append(UpdateOne(
            {"data_type": new_dict['data_type'], "metric_code": new_dict['metric_code'], "sub_metric_code": new_dict['sub_metric_code'], "time": new_dict['time']},
            {"$set": new_dict},
            upsert=True))
        if len(request_update) > 100:
            mongo_bulk_write_data(three_code_db, request_update)
            request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(three_code_db, request_update)
        request_update.clear()


def get_stock_financial_analysis_indicator(request_update: list, symbol="603288"):
    df_data = try_get_action(ak.stock_financial_abstract_ths, try_count=3, symbol=symbol,indicator="按报告期")
    if df_data is not None:
        for index in df_data.index:
            dict_data = dict(df_data.loc[index])
            dict_data['code'] = symbol
            dict_data['data_type'] = "fin_indicator"
            dict_data['date'] = str(dict_data.get("报告期", "")).replace("-", "")
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

if __name__ == '__main__':
    handle_fin_analysis_indicator()
