import pandas as pd
from data.mongodb import get_mongo_table
from utils.actions import show_data
import matplotlib.pyplot as plt
#设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')


def get_data(names=None):
    if names is None:
        names = ['大麦','小麦']
    data_info = get_mongo_table(database='govstats', collection='customs_goods')
    datas = []
    for ele in data_info.find({"name":{"$in":names}, "data_type": "import_goods_detail", "unit": "万吨"},
                              projection={'_id': False}).sort("date"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    return pd_data



def get_st_data():
    code_dict = {"A02090A01_yd":"啤酒产量_当期值","A02090A02_yd":"啤酒产量_累计值",
                 "A02090A03_yd":"啤酒产量_同比增长","A02090A04_yd":"啤酒产量_累计增长"}
    code_dict = {"A02090A03_yd": "啤酒产量_同比增长", "A02090A04_yd": "啤酒产量_累计增长"}
    data_info = get_mongo_table(database='govstats', collection='data_info')
    datas = []
    code_list = {"$in": list(code_dict.keys())}
    for ele in data_info.find({"code": code_list}, projection={'_id': False}):
        ele['data'] = float(ele['data'])
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    pd_data = pd.pivot_table(pd_data, values='data', index=['time'], columns=['code'])
    pd_data.rename(columns=code_dict, inplace=True)
    pd_data.sort_index(inplace=True)
    pd_data.plot(kind='bar', title='啤酒同比和累计同比增长', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    show_data(pd_data)
    plt.show()

def board_beer_data():
    pd_data = get_data()
    pd_data = pd.pivot_table(pd_data, values='acc_month_volume_cyc', index=['date'], columns=['name']).tail(100)
    pd_data.sort_index(inplace=True)
    pd_data.plot(kind='bar', title='进出口大麦和小麦数据', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    show_data(pd_data)
    plt.show()

if __name__ == '__main__':
    get_st_data()



