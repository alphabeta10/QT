import akshare as ak
from pymongo import UpdateOne
from utils.actions import show_data, try_get_action
from utils.tool import mongo_bulk_write_data
from data.mongodb import get_mongo_table

def handle_futures_daily_data(symbols=None):
    if symbols is None:
        symbols = ['B0']
    futures_daily = get_mongo_table(database='futures', collection='futures_daily')
    for symbol in symbols:
        datas = []
        futures_zh_daily_sina_df = try_get_action(ak.futures_zh_daily_sina, try_count=3, symbol=symbol)
        for index in futures_zh_daily_sina_df.index:
            ele_dict = dict(futures_zh_daily_sina_df.loc[index])
            dict_data = {}
            for k, v in ele_dict.items():
                if k == 'date':
                    dict_data[k] = str(v)
                else:
                    dict_data[k] = float(v)
            dict_data['symbol'] = symbol
            datas.append(UpdateOne(
                {"symbol": dict_data['symbol'], "date": dict_data['date']},
                {"$set": dict_data},
                upsert=True))
        if len(datas) > 0:
            mongo_bulk_write_data(futures_daily, datas)



def col_create_index():
    # futures_daily = get_mongo_table(database='futures', collection='futures_daily')
    # futures_daily.drop()
    # futures_daily.drop_index([("code", 1), ("time", 1)])
    futures_daily = get_mongo_table(database='futures', collection='futures_daily')
    futures_daily.create_index([("symbol", 1), ("date", 1)], unique=True, background=True)


if __name__ == '__main__':
    col_create_index()
    handle_futures_daily_data()
