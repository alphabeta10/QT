import akshare as ak
import copy
from utils.actions import try_get_action
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data
from datetime import datetime,timedelta
def global_micro_data(arg_start_date_str = None):
    if arg_start_date_str is None:
        arg_start_date_str = datetime.now().strftime("%Y%m01")

    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    start_date_str = copy.deepcopy(arg_start_date_str)
    now_date_str = datetime.now().strftime("%Y%m%d")
    start_date = datetime.strptime(start_date_str,"%Y%m%d")
    while int(start_date_str)<=int(now_date_str):
        print(f"start handle date={start_date_str}")
        news_economic_baidu_df = try_get_action(ak.news_economic_baidu,try_count=3,date=start_date_str)
        update_request = []
        for index in news_economic_baidu_df.index:
            ele = dict(news_economic_baidu_df.loc[index])
            dict_data = {
                "data_type":"global_micro_data",
                "metric_code":ele['事件'],
                "time":str(ele['日期']),
                "country":ele['地区'],
                "pub_value":str(ele['公布']),
                "predict_value":str(ele['预期']),
                "pre_value":str(ele['前值']),
                "weight":str(ele['重要性']),
            }

            update_request.append(
                UpdateOne(
                    {"data_type": dict_data['data_type'], "time": dict_data['time'],
                     "metric_code": dict_data['metric_code']},
                    {"$set": dict_data},
                    upsert=True)
            )
        if len(update_request)>0:
            mongo_bulk_write_data(stock_common, update_request)
            update_request.clear()

        start_date = start_date + timedelta(days=1)
        start_date_str = start_date.strftime("%Y%m%d")


def find_data():
    news = get_mongo_table(database='stock', collection='common_seq_data')
    for data in news.find({"data_type": "global_micro_data", "country": "美国", "time": {"$gt": "2023-01-01"}},
                          projection={'_id': False}).sort("time"):
        if '大豆' in data['metric_code']:
            print(data)


if __name__ == '__main__':
    global_micro_data()