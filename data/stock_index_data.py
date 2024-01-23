import akshare as ak
from tqdm import tqdm
from utils.actions import try_get_action
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data

def index_data(dict_list=None):
    if dict_list is None:
        stock_zh_index_spot_df = try_get_action(ak.stock_zh_index_spot_sina,try_count=3)
        if stock_zh_index_spot_df is not None:
            index_table = get_mongo_table(database='stock', collection='index_data')
            for index in tqdm(stock_zh_index_spot_df.index):
                code = stock_zh_index_spot_df.loc[index]['代码']
                name = stock_zh_index_spot_df.loc[index]['名称']
                stock_zh_index_daily_df = try_get_action(ak.stock_zh_index_daily,try_count=3,symbol=code)
                if stock_zh_index_daily_df is not None:
                    update_request = []
                    for index in stock_zh_index_daily_df.index:
                        data = stock_zh_index_daily_df.loc[index]
                        date = str(data['date'])
                        open = float(data['open'])
                        high = float(data['high'])
                        low = float(data['low'])
                        close = float(data['close'])
                        volume = int(data['volume'])
                        dict_data = {
                            "date":date,
                            "code":code,
                            "name":name,
                            "open":open,
                            "high":high,
                            "low":low,
                            "close":close,
                            "volume":volume
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
