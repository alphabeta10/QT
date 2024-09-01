import sys
import os

# 可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import akshare as ak
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.actions import try_get_action
from utils.tool import mongo_bulk_write_data, load_json_data
from datetime import datetime, timedelta


def stock_market_cn_fund_flow():
    stock_seq_daily = get_mongo_table(collection='stock_seq_daily')
    ret_data = try_get_action(ak.stock_market_fund_flow, try_count=3)
    request_update = []
    col_mapping = {'日期': 'time', '上证-收盘价': 'sh_close', '上证-涨跌幅': 'sh_change',
                   '深证-收盘价': 'sz_close', '深证-涨跌幅': 'sz_change', '主力净流入-净额': 'main_force_flow_in',
                   '主力净流入-净占比': 'main_force_flow_in_rate', '超大单净流入-净额': 'supper_order_flow_in',
                   '超大单净流入-净占比': 'supper_order_flow_in_rate',
                   '大单净流入-净额': 'big_order_flow_in', '大单净流入-净占比': 'big_order_flow_in_rate',
                   '中单净流入-净额': 'mid_order_flow_in',
                   '中单净流入-净占比': 'mid_order_flow_in_rate', '小单净流入-净额': 'small_order_flow_in',
                   '小单净流入-净占比': 'small_order_flow_in_rate'}

    if ret_data is not None and len(ret_data) > 0:
        for index in ret_data.index:
            dict_data = dict(ret_data.loc[index])
            new_dict = {"metric_key": "cn_stock_market_fun_flow", "sub_key": "sh_sz_fun_flow"}
            for raw_key, key in col_mapping.items():
                if raw_key in dict_data.keys():
                    val = str(dict_data.get(raw_key))
                    if val == 'nan':
                        val = ''
                    new_dict[key] = val
                else:
                    print(f"no maaping data key {raw_key}")
            # metric_key:一级编码,sub_key：二级编码,time：时间
            request_update.append(UpdateOne(
                {"metric_key": new_dict['metric_key'], "sub_key": new_dict['sub_key'], "time": new_dict['time']},
                {"$set": new_dict},
                upsert=True))
            if len(request_update) > 100:
                mongo_bulk_write_data(stock_seq_daily, request_update)
                request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(stock_seq_daily, request_update)
        request_update.clear()


def stock_market_us_fund_flow():
    stock_seq_daily = get_mongo_table(collection='stock_seq_daily')
    index_mappinng = {".IXIC": "纳斯达克综合指数", ".DJI": "道琼斯工业平均指数", ".INX": "标普500指数",
                      ".NDX": "纳斯达克100指数"}

    for symbol, symbol_name in index_mappinng.items():
        ret_data = try_get_action(ak.index_us_stock_sina, try_count=3, symbol=symbol)
        request_update = []
        if ret_data is not None and len(ret_data) > 0:
            for index in ret_data.index:
                dict_data = dict(ret_data.loc[index])
                new_dict = {"metric_key": "us_stock_market_fun_flow", "sub_key": symbol_name}
                for k, v in dict_data.items():
                    # 单独处理时间的数据
                    if k == 'date':
                        new_dict['time'] = str(v)
                    else:
                        new_dict[k] = str(v)
                # metric_key:一级编码,sub_key：二级编码,time：时间
                request_update.append(UpdateOne(
                    {"metric_key": new_dict['metric_key'], "sub_key": new_dict['sub_key'], "time": new_dict['time']},
                    {"$set": new_dict},
                    upsert=True))
                if len(request_update) > 100:
                    mongo_bulk_write_data(stock_seq_daily, request_update)
                    request_update.clear()
        if len(request_update) > 0:
            mongo_bulk_write_data(stock_seq_daily, request_update)
            request_update.clear()


def currency_fund_flow(start_date=None,end_date=None):
    stock_seq_daily = get_mongo_table(collection='stock_seq_daily')
    api_key = load_json_data('currencyscoop.json')['api_key']
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30 * 8)).strftime("%Y-%m-%d")
    ret_data = try_get_action(ak.currency_time_series, try_count=3, base="USD", start_date=start_date,
                              end_date=end_date,
                              symbols="", api_key=api_key)
    request_update = []
    if ret_data is not None and len(ret_data) > 0:
        for index in ret_data.index:
            dict_data = dict(ret_data.loc[index])
            for k in dict_data.keys():
                if k != 'date':
                    v = dict_data.get(k)
                    time = str(dict_data.get("date"))
                    new_dict = {"target_currency_value": float(v), "metric_key": "global_currency",
                                "sub_key": f"USD_{k}", "time": time}
                    request_update.append(UpdateOne(
                    {"metric_key": new_dict['metric_key'], "sub_key": new_dict['sub_key'], "time": new_dict['time']},
                    {"$set": new_dict},
                    upsert=True))
            if len(request_update) > 500:
                mongo_bulk_write_data(stock_seq_daily, request_update)
                request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(stock_seq_daily, request_update)
        request_update.clear()


if __name__ == '__main__':
    stock_market_cn_fund_flow()
    stock_market_us_fund_flow()
    currency_fund_flow()
