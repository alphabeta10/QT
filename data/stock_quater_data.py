import sys
import os

import pandas as pd

#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from data.board_data import show_data
from utils.tool import mongo_bulk_write_data
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

def handle_stock_qbzf_em_data():
    '''
    个股增发
    '''
    stock_qbzf_em_df = ak.stock_qbzf_em()
    common_seq_data = get_mongo_table(collection='common_seq_data')
    request_update = []
    col_mapping = {'股票代码': 'code', '股票简称': 'name', '增发代码': 'add_code', '发行方式': 're_type', '发行总数': 're_num', '网上发行': 'net_num', '发行价格': 're_price', '最新价': 'new_price', '发行日期': 'time', '增发上市日期': 'add_time', '锁定期': 'lock_year'}
    stock_qbzf_em_df.rename(columns=col_mapping,inplace=True)
    for index in stock_qbzf_em_df.index:
        dict_data = dict(stock_qbzf_em_df.loc[index])
        # dict_data = {k:v for k,v in dict_data.items() if v!=None}
        for k in ['re_num','net_num','re_price','new_price']:
            dict_data[k] = float(dict_data[k])
        dict_data['metric_code'] = dict_data['code']
        dict_data['time'] = str(dict_data['time'])
        dict_data['add_time'] = str(dict_data['add_time'])
        dict_data['data_type'] = 'stock_qbzf'
        request_update.append(UpdateOne(
            {"metric_code": dict_data['metric_code'], "data_type": dict_data['data_type'],'time':dict_data['time']},
            {"$set": dict_data},
            upsert=True))
        print(dict_data)
        if len(request_update) > 100:
            mongo_bulk_write_data(common_seq_data, request_update)
            request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(common_seq_data, request_update)
        request_update.clear()

def handle_stock_pg_em_data():
    '''
    个股配股
    '''
    stock_pg_em_df = ak.stock_pg_em()
    common_seq_data = get_mongo_table(collection='common_seq_data')
    request_update = []
    col_mapping = {'股票代码': 'code', '股票简称': 'name', '配售代码': 'sell_code', '配股数量': 'sell_num', '配股比例': 'sell_rate', '配股价': 'sell_price', '最新价': 'new_price', '配股前总股本': 'old_total_num', '配股后总股本': 'new_total_num', '股权登记日': 'record_time', '缴款起始日期': 'start_time', '缴款截止日期': 'end_time', '上市日': 'time'}
    stock_pg_em_df.rename(columns=col_mapping,inplace=True)
    for index in stock_pg_em_df.index:
        dict_data = dict(stock_pg_em_df.loc[index])
        print(dict_data)
        for k in ['sell_num','sell_price','new_price','old_total_num','new_total_num']:
            dict_data[k] = float(dict_data[k])
        dict_data['metric_code'] = dict_data['code']
        dict_data['time'] = str(dict_data['time'])
        dict_data['record_time'] = str(dict_data['record_time'])
        dict_data['start_time'] = str(dict_data['start_time'])
        dict_data['end_time'] = str(dict_data['end_time'])
        dict_data['data_type'] = 'stock_pg'
        request_update.append(UpdateOne(
            {"metric_code": dict_data['metric_code'], "data_type": dict_data['data_type'],'time':dict_data['time']},
            {"$set": dict_data},
            upsert=True))
        if len(request_update) > 100:
            mongo_bulk_write_data(common_seq_data, request_update)
            request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(common_seq_data, request_update)
        request_update.clear()

def handle_stock_report_fund_hold_detail_data():
    fund_name_em_df = ak.fund_name_em()
    stock_fund_hold_detail = get_mongo_table(collection='stock_fund_hold_detail')
    fund_type_list = [
    "混合型-灵活",
    "债券型-混合二级",
    "债券型-混合一级",
    "混合型-偏股",
    "指数型-股票",
    "QDII-普通股票",
    "指数型-海外股票",
    "股票型",
    "QDII-混合偏股",
    "混合型-绝对收益",
    "QDII-混合灵活",
    "混合型-平衡",
    "QDII-混合平衡",
    "FOF-进取型",
    "FOF-均衡型",
    "QDII-REITs",
    "QDII-FOF"]
    #fund_name_em_df = fund_name_em_df[fund_name_em_df['基金类型'].isin(fund_type_list)]
    quarter = '20240930'
    col_mapping = {'股票代码': 'code', '股票简称': 'name', '持股数': 'hold_num', '持股市值': 'hold_value', '占总股本比例': 'hold_rate', '占流通股本比例': 'hold_rate_flow'}
    for fund_code in tqdm(fund_name_em_df['基金代码'].values):
        stock_report_fund_hold_detail_df = try_get_action(ak.stock_report_fund_hold_detail,try_count=1,symbol=fund_code, date=quarter)
        if stock_report_fund_hold_detail_df is not None:
            stock_report_fund_hold_detail_df.rename(columns=col_mapping,inplace=True)
            stock_report_fund_hold_detail_df = stock_report_fund_hold_detail_df[list(col_mapping.values())]
            request_update = []
            for index in stock_report_fund_hold_detail_df.index:
                dict_data = dict(stock_report_fund_hold_detail_df.loc[index])
                for k in ['hold_num','hold_value','hold_rate','hold_rate_flow']:
                    dict_data[k] = float(dict_data[k])
                dict_data['metric_code'] = dict_data['code']
                dict_data['sub_metric_code'] = fund_code
                dict_data['data_type'] = 'stock_report_fund_hold_detail'
                dict_data['time'] = quarter
                request_update.append(UpdateOne(
                    {"metric_code": dict_data['metric_code'], "data_type": dict_data['data_type'],'time':dict_data['time'],'sub_metric_code':dict_data['sub_metric_code']},
                    {"$set": dict_data},
                    upsert=True))
                if len(request_update) > 100:
                    mongo_bulk_write_data(stock_fund_hold_detail, request_update)
                    request_update.clear()
            if len(request_update) > 0:
                mongo_bulk_write_data(stock_fund_hold_detail, request_update)
                request_update.clear()
        else:
            print('error ',fund_code)
    

if __name__ == '__main__':
    handle_stock_report_fund_hold_detail_data()
