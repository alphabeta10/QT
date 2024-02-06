"""
公共方法的指标，例如，股指期货多空比
"""
import akshare as ak
from pymongo import UpdateOne
from utils.actions import try_get_action
from utils.tool import mongo_bulk_write_data
from data.mongodb import get_mongo_table
from datetime import datetime


def get_fin_futures_long_short_rate(codes,start_time):
    """
    返回最新一条多空比的数据
    :param codes:
    :param start_time:
    :return:
    """
    futures_basic_info = get_mongo_table(database='futures', collection='futures_basic_info')
    result_data = {}
    for ele in futures_basic_info.find(
            {"data_type": "futures_long_short_rate", "date": {"$gt": start_time},"code": {"$in": codes}},
            projection={'_id': False}).sort("date"):
        result_data[ele['code']] = {"date":ele['date'],"long_short_rate":ele['long_short_rate']}
    return result_data


def get_stock_last_dzjy(codes,start_time):
    """
    返回最新一条多空比的数据
    :param codes:
    :param start_time:
    :return:
    """
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    result_data = {}
    for ele in stock_common.find(
            {"data_type": "stock_dzjy", "time": {"$gt": start_time},"metric_code": {"$in": codes}},
            projection={'_id': False}).sort("time"):
        print(ele)
        result_data[ele['metric_code']] = ele
    return result_data



if __name__ == '__main__':
    pass
