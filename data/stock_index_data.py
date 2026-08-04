import sys
import os

import pandas as pd

from data.board_data import show_data

#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import akshare as ak
from tqdm import tqdm
from utils.actions import try_get_action
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
from datetime import datetime,timedelta

def get_trade_date(start_date_str:str):
    tool_trade_date_hist_sina_df = try_get_action(ak.tool_trade_date_hist_sina, try_count=3)
    if tool_trade_date_hist_sina_df is not None:
        trade_dates = []
        month_dates = set()
        now_int = int(datetime.now().strftime("%Y%m%d"))
        before_day_int = int(start_date_str.replace("-",""))
        for index in tool_trade_date_hist_sina_df.index:
            trade_date = tool_trade_date_hist_sina_df.loc[index]['trade_date']
            date_str = str(trade_date).replace("-", "")
            if int(date_str) >= before_day_int and int(date_str) <= now_int:
                trade_dates.append(date_str)
                month_dates.add(date_str[:6])
        return trade_dates
    return None

def get_code_by_date(date_str:str,last_index_df,last_trade_date):
    ticker_daily = get_mongo_table(collection="index_data")
    condition = {"date":{"$gte":date_str}}
    ticker_cursor = ticker_daily.find(condition,projection={"_id":False,"code":True,"amount":True,"date":True})
    index_data_dict = {}
    for ticker in ticker_cursor:
        code = ticker['code']
        date = ticker['date']
        amount = ticker['amount']
        index_data_dict.setdefault(date,{})
        index_data_dict[date][code] = amount
    if last_index_df is not None:
        for index in last_index_df.index:
            dict_data = dict(last_index_df.loc[index])
            amount = dict_data.get('成交额')
            code = dict_data.get('代码')
            index_data_dict.setdefault(last_trade_date, {})
            index_data_dict[last_trade_date][code] = amount
    return index_data_dict


def index_data(dict_list=None,start_date = None):
    if dict_list is None:
        if start_date is None:
            start_date = (datetime.now()-timedelta(days=15)).strftime("%Y%m%d")

        trade_dates = get_trade_date(start_date)
        last_trade_date =  datetime.strftime(datetime.strptime(trade_dates[-1],'%Y%m%d'),'%Y-%m-%d')
        get_db_start_date_str = datetime.strftime(datetime.strptime(start_date,'%Y%m%d'),'%Y-%m-%d')
        stock_zh_index_spot_df = try_get_action(ak.stock_zh_index_spot_sina,try_count=3)
        index_codes = ['sh000001','sz399001','sz399300','sz399006','sh000688','sh000905','sz399852']

        if stock_zh_index_spot_df is not None:
            last_index_data = stock_zh_index_spot_df[stock_zh_index_spot_df['代码'].isin(index_codes)]
            his_index_amt_dict_data = get_code_by_date(get_db_start_date_str,last_index_data,last_trade_date)

            stock_zh_index_spot_df.to_csv("index.csv",index=False)
            index_table = get_mongo_table(database='stock', collection='index_data')
            for index in tqdm(stock_zh_index_spot_df.index):
                code = stock_zh_index_spot_df.loc[index]['代码']
                name = stock_zh_index_spot_df.loc[index]['名称']
                if code in index_codes:
                    #stock_zh_index_daily_df = try_get_action(ak.stock_zh_index_daily_em,try_count=3,symbol=code)
                    stock_zh_index_daily_tx_df = try_get_action(ak.stock_zh_index_daily_tx,try_count=3,symbol=code,start_date=start_date)
                    if stock_zh_index_daily_tx_df is not None:
                        update_request = []
                        for index in stock_zh_index_daily_tx_df.index:
                            data = stock_zh_index_daily_tx_df.loc[index]
                            date = str(data['date'])
                            open = float(data['open'])
                            high = float(data['high'])
                            low = float(data['low'])
                            close = float(data['close'])
                            volume = int(data['amount'])*100
                            amount = float(his_index_amt_dict_data.get(date).get(code))
                            dict_data = {
                                "date":date,
                                "code":code,
                                "name":name,
                                "open":open,
                                "high":high,
                                "low":low,
                                "close":close,
                                "volume":volume,
                                'amount':amount
                            }
                            update_request.append(
                                UpdateOne(
                                    {"code": dict_data['code'],"date":dict_data['date']},
                                    {"$set": dict_data},
                                    upsert=True)
                            )
                        mongo_bulk_write_data(index_table,update_request)




def create_index():
    index_table = get_mongo_table(database='stock', collection='index_data')
    index_table.create_index([("date",1),("code",1)],unique=True,background=True)


if __name__ == '__main__':
    index_data()
