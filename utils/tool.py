import json
import pandas as pd
import pymongo
import operator
from data.mongodb import get_mongo_table


def is_json(str_: str):
    """
    判断是否是json数据
    :param str_:
    :return:
    """
    try:
        json.loads(str_)
    except Exception as e:
        return False
    return True
def dump_json_data(file_name,json_data:dict):
    with open(file_name,'w') as f:
        json.dump(json_data,f)

def load_json_data(file_name):
    with open(file_name,'r') as f:
        return json.load(f)
def mongo_bulk_write_data(db_col, upsert_datas: list):
    """
    批量数据录入mongo db
    :param db_col:
    :param upsert_datas:
    :return:
    """
    if len(upsert_datas) > 0:
        update_result = db_col.bulk_write(upsert_datas, ordered=False)
        print('数据录入插入：%4d条, 更新：%4d条' % (update_result.upserted_count, update_result.modified_count),
              flush=True)


def get_data_from_mongo(database='stock', collection='goods', condition=None, projection=None, sort_key='time',
                        sort_type=pymongo.ASCENDING):
    """
    get data from mongo
    :param database:
    :param collection:
    :param condition:
    :param projection:
    :param sort_key:
    :param sort_type:
    :return:
    """
    if projection is None or sort_key is None:
        return None
    goods = get_mongo_table(database=database, collection=collection)
    datas = []
    for ele in goods.find(condition, projection=projection).sort(sort_key, sort_type):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    return pd_data


def sort_dict_data_by(dict_data, by='key', reverse=False):
    """
    字典按key或者value排序
    :param dict_data:
    :param by:
    :param reverse:
    :return:
    """
    if by == 'key':
        return dict(sorted(dict_data.items(), key=operator.itemgetter(0), reverse=reverse))  # 按照key值升序
    else:
        return dict(sorted(dict_data.items(), key=operator.itemgetter(1), reverse=reverse))  # 按照value值升序
