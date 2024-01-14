import akshare as ak
from utils.actions import try_get_action
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from datetime import datetime



def ths_concept_daily_data(start_year='2020'):
    concept_data_col = get_mongo_table(collection='concept_data')
    col_mapping = {'日期': "time", '开盘价': "open", '最高价': "high",
                   '最低价': "low", '收盘价': "close", '成交量': "amount", '成交额': 'vol'}
    concept_name_info = ak.stock_board_concept_name_ths()
    for concept_index in concept_name_info.index:
        concept_ele = concept_name_info.loc[concept_index]
        name = concept_ele['概念名称']
        code = concept_ele['代码']
        stock_board_concept_hist_ths_df = try_get_action(ak.stock_board_concept_hist_ths, try_count=3,
                                                         start_year=start_year, symbol=name)
        request_update = []
        if stock_board_concept_hist_ths_df is not None:
            for index in stock_board_concept_hist_ths_df.index:
                dict_data = dict(stock_board_concept_hist_ths_df.loc[index])
                new_dict_data = {}
                for k, v in dict_data.items():
                    new_col = col_mapping[k]
                    if k in ['日期', '成交量']:
                        v = str(v)
                    new_dict_data[new_col] = v
                new_dict_data["name"] = name
                new_dict_data["code"] = code

                request_update.append(UpdateOne(
                    {"code": new_dict_data['code'], "time": new_dict_data['time']},
                    {"$set": new_dict_data},
                    upsert=True))
            if len(request_update) > 0:
                update_result = concept_data_col.bulk_write(request_update, ordered=False)
                print(' 插入：%4d条, 更新：%4d条' %
                      (update_result.upserted_count, update_result.modified_count),
                      flush=True)
        else:
            print(f"{name} not find data")


def col_create_index():
    concept_data = get_mongo_table(collection="concept_data")
    concept_data.create_index([("code", 1), ("time", 1)], unique=True, background=True)


if __name__ == '__main__':
    start_year = 2024
    cur_year = int(datetime.now().strftime("%Y"))
    while start_year <= cur_year:
        print(f"handle year={start_year}")
        ths_concept_daily_data(start_year=str(start_year))
        start_year += 1
