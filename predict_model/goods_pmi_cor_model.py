from utils.tool import get_data_from_mongo, sort_dict_data_by
from utils.actions import show_data
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def corr_pmi_goods_data():
    """
    计算强相关的商品和pmi数据
    :return:
    """
    # 商品数据
    time = (datetime.now() - timedelta(days=400)).strftime("%Y%m%d")
    goods_list = ['铜', '铝', '锡', 'WTI原油', 'Brent原油', '氧化镝', '金属镝', '镨钕氧化物']
    condition = {"name": {"$in": goods_list}, "time": {"$gte": time}, "data_type": "goods_price"}
    condition = {"time": {"$gte": time}, "data_type": "goods_price"}
    sort_key = 'time'
    database = 'stock'
    collection = 'goods'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data[['value']] = data[['value']].astype(float)
    nc = data['name'].value_counts()
    filter_dict = {k: v for k, v in dict(nc).items() if v > 300}
    data['time'] = pd.to_datetime(data['time'], format='%Y%m%d')
    data = pd.pivot_table(data, index='time', columns='name', values='value')
    data = data[list(filter_dict.keys())]
    goods_data = data.resample("M").mean()
    goods_data['date'] = goods_data.index
    goods_data['date'] = goods_data['date'].apply(lambda ele: str(ele)[0:8].replace("-", ""))

    # pmi指数数据
    code_dict = {'A0B0101_yd': '制造业采购经理指数(%)',
                 'A0B0102_yd': '生产指数(%)',
                 'A0B0103_yd': '新订单指数(%)',
                 'A0B0104_yd': '新出口订单指数(%)',
                 'A0B0301_yd': '综合PMI产出指数(%)',
                 }
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    time = time[0:6]
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    pmi_data = pd.pivot_table(data, index='time', columns='code', values='data')
    pmi_data['date'] = pmi_data.index

    # 合并数据，并计数相关性
    merge_pd_data = pd.merge(pmi_data, goods_data, on=['date'], how='inner')
    merge_pd_data.set_index(keys='date', inplace=True)
    merge_pd_data.sort_index(inplace=True)
    corr = merge_pd_data.corr()
    dict_data = dict(corr.loc['A0B0101_yd'])
    new_dict = {}
    select_goods_dict = {}
    for k, v in dict_data.items():
        if str(v) == 'nan':
            v = -1000
        new_dict[k] = v
        if v > 0.1 and k not in code_dict.keys():
            select_goods_dict[k] = v
    return sort_dict_data_by(select_goods_dict, by='value')


def predict_pmi_data(goods_dict_data: dict, is_show=False):
    """
    预测pmi，如果pmi预测大于0，pmi很可能是大于50，
    :param goods_dict_data:
    :param is_show:
    :return:
    """
    keys = list(goods_dict_data.keys())
    time = (datetime.now() - timedelta(days=365 * 4)).strftime("%Y%m%d")
    condition = {"name": {"$in": keys}, "time": {"$gte": time}, "data_type": "goods_price"}
    sort_key = 'time'
    database = 'stock'
    collection = 'goods'
    projection = {"_id": False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data[['value']] = data[['value']].astype(float)
    data['time'] = pd.to_datetime(data['time'], format='%Y%m%d')
    data = pd.pivot_table(data, index='time', columns='name', values='value')
    goods_data = data.resample("M").mean()

    cycle_data = goods_data.pct_change(1)
    cycle_dict_data = dict(cycle_data.tail(1).iloc[0])
    cycle_pmi = 0
    for k, p_chg in cycle_dict_data.items():
        if str(p_chg) != 'nan' and str(goods_dict_data.get(k)) != 'nan':
            cycle_pmi += p_chg * goods_dict_data.get(k)

    datas = []
    for index in cycle_data.index:
        dict_data = dict(cycle_data.loc[index])
        cycle_pmi = 0
        for k, p_chg in dict_data.items():
            if str(p_chg) != 'nan' and str(goods_dict_data.get(k)) != 'nan':
                cycle_pmi += p_chg * goods_dict_data.get(k)
        cycle_pmi = round(cycle_pmi, 4)
        datas.append({"time": str(index)[0:7], "predict_pmi": cycle_pmi})

    cyc_pmi_predict = pd.DataFrame(datas)
    cyc_pmi_predict.set_index(keys='time', inplace=True)
    if is_show:
        cyc_pmi_predict.plot(kind='line', title='predict pmi', rot=45, figsize=(15, 8), fontsize=10)
        plt.show()
        show_data(cyc_pmi_predict)
        print(f"cycle data {cycle_dict_data}")
        print(f"cycle_pmi = {cycle_pmi}")
        print(f"cor_data={goods_dict_data}")
    return cyc_pmi_predict

def cor_predict_pmi(is_show=False):
    cor_dict = corr_pmi_goods_data()
    pmi_predict = predict_pmi_data(cor_dict, is_show=is_show)
    return pmi_predict


if __name__ == '__main__':
    cor_predict_pmi(is_show=True)
