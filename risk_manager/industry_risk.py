# 涉及行业cpi数据
import pandas as pd
from utils.tool import get_data_from_mongo
from utils.actions import show_data
from utils.tool import sort_dict_data_by
import warnings
import matplotlib.pyplot as plt
from risk_manager.code_config import industry_metric_code_dict,industry_metric_risk_key_code_dict,industry_op_profit_code_dict,industry_num_of_loss_code_dict,industry_num_of_loss_risk_key_code_dict
from datetime import datetime,timedelta
# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
warnings.filterwarnings('ignore')


def cn_industry_metric_risk(is_show=False):
    """
    行业整体风险国家统计局工业指标风险
    :param is_show:
    :return:
    """
    code_dict = industry_metric_code_dict
    code_st_dict = industry_metric_risk_key_code_dict
    all_risk_dict_data = comm_cn_industry_metric_sort(code_dict,code_st_dict)
    datas = []
    for time,combine_dict in all_risk_dict_data.items():
        total_risk = combine_dict['total_risk']
        risk = round(sum(list(total_risk.values()))/len(total_risk.keys()),4)
        datas.append({"time":time,"risk":risk})
    risk_df = pd.DataFrame(data=datas)
    risk_df.set_index(keys='time',inplace=True)
    if is_show==True:
        risk_df.plot(kind='line', title='中国工业指标风险', rot=45, figsize=(15, 8), fontsize=10)
        show_data(risk_df)
        plt.show()
    return risk_df


def comm_cn_industry_metric_sort(code_dict: dict = None, is_down_code_dict:dict=None):
    """
    公共指标衡量风险,增长率大于或者小于
    :param code_dict:
    :return:
    """
    if code_dict is None:
        print("代码不能为空")
        return
    if is_down_code_dict is None:
        is_down_code_dict = {}

    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    time = (datetime.now() - timedelta(days=365)).strftime("%Y01")
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data['month'] = data['time'].apply(lambda ele: int(ele[4:6]))
    data = data[data['month'] >= 2]
    new_data = pd.pivot_table(data, values='data', index='time', columns='code')
    before_num = 5
    for code in code_dict.keys():
        for i in range(before_num):
            new_data[f'{code}_pct_{i + 1}'] = new_data[code].diff(i + 1)
    new_data = new_data.dropna()
    all_dict_data = {}
    for index in new_data.index:
        dict_data = dict(new_data.loc[index])
        dict_data['time'] = index
        detail_industry_risk = {}
        for code, name in code_dict.items():
            detail_industry_risk[code] = {"up": 0, "down": 0, "name": name}
            for i in range(before_num):
                if dict_data.get(f'{code}_pct_{i + 1}') > 0:
                    detail_industry_risk[code]['up'] += 1
                else:
                    detail_industry_risk[code]['down'] += 1
        total_industry_risk = {}

        for code, combine_dict in detail_industry_risk.items():
            key = 'up' if is_down_code_dict.get(code,'')=='up' else 'down'
            total_industry_risk[code_dict.get(code)] = round(combine_dict[key] / before_num, 4)
        total_industry_risk = sort_dict_data_by(total_industry_risk, by='value')
        all_dict_data[index] = {"detail":detail_industry_risk,"total_risk":total_industry_risk}
    return all_dict_data

def get_new_industry_op_profit_risk():
    code_dict = industry_op_profit_code_dict
    code_st_dict = {}
    all_risk_dict_data = comm_cn_industry_metric_sort(code_dict, code_st_dict)
    return all_risk_dict_data
def get_new_industry_num_loss_risk():
    code_dict = industry_num_of_loss_code_dict
    code_st_dict = industry_num_of_loss_risk_key_code_dict
    all_risk_dict_data = comm_cn_industry_metric_sort(code_dict, code_st_dict)
    return all_risk_dict_data


if __name__ == '__main__':
    get_new_industry_num_loss_risk()
