import akshare as ak
from utils.actions import show_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from tqdm import tqdm


def global_index_data():
    global_index_daily = get_mongo_table(collection="global_index_daily")
    update_request = []
    index_df = ak.index_global_spot_em()
    symbols = index_df['名称'].values
    dict_mapping = {'日期': 'date', '代码': 'code', '名称': 'name', '今开': 'open', '最新价':'close', '最高':'high', '最低': 'low', '振幅': 'amplitude'}
    for symbol in tqdm(symbols):
        index_global_hist_em_df = ak.index_global_hist_em(symbol=symbol)
        for index in index_global_hist_em_df.index:
            dict_data = dict(index_global_hist_em_df.iloc[index])
            dict_ele = {}
            for raw_key,key in dict_mapping.items():
                val = dict_data.get(raw_key)
                if '日期' in raw_key:
                    val = str(val)
                dict_ele[key] = val
            update_request.append(
                UpdateOne({"code": dict_ele['code'],"date":dict_ele['date']},
                          {"$set": dict_ele},
                          upsert=True)
            )

            if len(update_request) > 500:
                update_result = global_index_daily.bulk_write(update_request, ordered=False)
                print('插入：%4d条, 更新：%4d条' %
                      (update_result.upserted_count, update_result.modified_count),
                      flush=True)
                update_request.clear()
    if len(update_request) > 1000:
        update_result = global_index_daily.bulk_write(update_request, ordered=False)
        print('插入：%4d条, 更新：%4d条' %
              (update_result.upserted_count, update_result.modified_count),
              flush=True)
        update_request.clear()


def create_index():
    global_index_daily = get_mongo_table(collection="global_index_daily")
    global_index_daily.create_index([("code", 1), ("date", 1)], unique=True, background=True)


if __name__ == '__main__':
    global_index_data()