from utils.tool import get_data_from_mongo, sort_dict_data_by
from utils.actions import show_data
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def predict_export_import(is_show=True):
    time = (datetime.now() - timedelta(days=365*10)).strftime("%Y%m%d")
    #pmi指数数据
    code_dict = {
        'A0B0108_yd': '进口指数',
        'A0B0104_yd': '新出口订单指数(%)',
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
    pmi_same_df = pmi_data.pct_change(12)
    pmi_same_df.dropna(inplace=True)
    pmi_same_df.reset_index(inplace=True)
    #进出口数据
    database = 'govstats'
    collection = 'customs_goods'
    cols = ['export_amount', 'import_amount']

    rename_col_dict = {
        "export_amount":"出口同比",
        "import_amount":"进口同比",
        'A0B0108_yd': '进口指数同比',
        'A0B0104_yd': '新出口订单指数同比',
    }
    projection = {'_id': False}
    time = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y-01-01")
    condition = {"data_type": "country_export_import", "name": "总值", "date": {"$gte": time}}
    sort_key = 'date'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data['time'] = data['date'].apply(lambda ele:ele.replace("-","")[0:6])
    data.set_index(keys='time', inplace=True)
    data = data[cols]
    data[cols] = data[cols].astype(float)
    export_import_same_df = data.pct_change(12)
    export_import_same_df.dropna(inplace=True)
    export_import_same_df.reset_index(inplace=True)
    new_data = pd.merge(pmi_same_df,export_import_same_df, on=['time'], how='left')
    new_data.set_index(keys='time',inplace=True)
    new_data.rename(columns=rename_col_dict,inplace=True)
    if is_show:
        show_data(new_data)
        new_data[['出口同比','新出口订单指数同比']].plot(kind='line', title='预测出口', rot=45, figsize=(15, 8), fontsize=10)
        plt.show()

        new_data[['进口同比', '进口指数同比']].plot(kind='line', title='预测进口', rot=45, figsize=(15, 8),
                                                          fontsize=10)
        plt.show()
    last_dict_data = dict(new_data.tail(1).iloc[0])
    return round(last_dict_data['新出口订单指数同比'],4),round(last_dict_data['进口指数同比'],4)



if __name__ == '__main__':
    export_same,import_same = predict_export_import(is_show=True)
