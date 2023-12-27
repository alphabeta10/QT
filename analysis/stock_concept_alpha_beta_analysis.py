from analysis.analysis_tool import get_market_data, LR
from utils.tool import get_data_from_mongo
import pandas as pd
import numpy as np
import os
import akshare as ak
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
#设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')

def concept_beta_alpha_analysis(start_day_str = None):
    # 获取指数数据，相当于市场走势数据
    if start_day_str is None:
        start_day_str = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    condition = {"code": {"$in": ["sh000001"]},
                 "date": {"$gte": f"{start_day_str}"}}
    market_data = get_market_data(condition=condition)
    market_data = market_data.dropna()

    database = 'stock'
    collection = 'concept_data'
    projection = {"_id": False, "name": True, "close": True, "time": True}
    sort_key = 'time'

    tmp_board_concept_file_name = 'temp_board_concept_name.csv'
    # 判断数据是否在本地存在
    if os.path.exists(tmp_board_concept_file_name) is False:
        concept_name_info = ak.stock_board_concept_name_ths()
        concept_name_info.to_csv(tmp_board_concept_file_name, index=False)
        print("get data from  net interface")
    else:
        concept_name_info = pd.read_csv(tmp_board_concept_file_name)
        print("get data from local")

    # 获取概念数据
    code_name_mapping = {}
    for index in concept_name_info.index:
        ele = dict(concept_name_info.loc[index])
        code = str(ele['代码'])
        name = ele['概念名称']
        code_name_mapping[code] = name

    codes = list(code_name_mapping.keys())
    condition = {"code": {"$in": codes},
                 "time": {"$gte": f"{start_day_str}"}}

    concept_seq_data = get_data_from_mongo(database=database, collection=collection, condition=condition,
                                           projection=projection, sort_key=sort_key)
    concept_seq_data['time'] = pd.to_datetime(concept_seq_data['time'])
    concept_data = pd.pivot_table(concept_seq_data, values='close', index="time", columns='name')
    concept_data = concept_data.pct_change()
    concept_cols = concept_data.columns

    now_str = datetime.now().strftime("%Y%m%d")

    start_time_str = start_day_str
    end_time_str = (datetime.strptime(start_time_str, '%Y-%m-%d') + timedelta(days=40)).strftime("%Y-%m-%d")

    alpha_change_list = []
    while int(end_time_str.replace("-", "")) < int(now_str):
        print(f"handle date={end_time_str}")
        time_range_market = market_data[start_time_str:end_time_str]
        time_range_concept_data = concept_data[start_time_str:end_time_str]
        time_range_market_val = time_range_market.values
        len = time_range_market_val.shape[0]
        for code, name in code_name_mapping.items():
            if name in concept_cols:
                concept_values = time_range_concept_data[name].values
                isnan = True in np.isnan(concept_values)
                if len != concept_values.shape[0] or isnan:
                    continue
                beta, alpha = LR(time_range_market_val, concept_values)
                combine_data = [end_time_str,name,alpha[0],beta[0][0]]
                alpha_change_list.append(combine_data)
        start_time_str = (datetime.strptime(start_time_str, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        end_time_str = (datetime.strptime(start_time_str, '%Y-%m-%d') + timedelta(days=40)).strftime("%Y-%m-%d")
    alpha_change_pd = pd.DataFrame(alpha_change_list,columns=['time','name','alpha','beta'])
    alpha_change_pd.to_csv("alpha_beta.csv",index=False)

if __name__ == '__main__':
    concept_beta_alpha_analysis(start_day_str='2020-01-01')