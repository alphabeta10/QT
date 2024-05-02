from datetime import datetime, timedelta
from utils.tool import get_data_from_mongo
import numpy as np
import pandas as pd

def global_risk():
    database = 'stock'
    collection = 'model_new_indicator'
    projection = {'_id': False}
    codes = ['乌克兰', '以色列']
    time = (datetime.now() - timedelta(days=365)).strftime("%Y-01-01")
    code_list = {"$in": codes}
    condition = {"code": code_list, "time": {"$gte": time}, "data_type": "big_model_sentiment"}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    st = {}
    for index in data.index:
        dict_data = dict(data.loc[index])
        day = dict_data['time'][0:10]
        sentiment = dict_data['sentiment']
        if '悲观' in sentiment or '消极' in sentiment:
            sentiment = '悲观'
        st.setdefault(day, {})
        st[day].setdefault(sentiment, 0)
        st[day][sentiment] += 1
    risk_day = {}
    datas = []
    for day,combine_dict in st.items():
        day = day.replace("-","")
        total_num = np.sum(list(combine_dict.values()))
        risk_day.setdefault(day,{})
        risk_day[day]['risk'] = round(combine_dict.get('悲观',0)/total_num,4)
        risk_day[day]['no_risk'] = round(combine_dict.get("中性",0)/total_num,4)
        datas.append({"time":day,"risk":round(combine_dict.get('悲观',0)/total_num,4),"no_risk":round(combine_dict.get("中性",0)/total_num,4)})
    pd_data = pd.DataFrame(data=datas)
    pd_data.set_index(keys='time',inplace=True)
    pd_data['time'] = pd_data.index
    return pd_data


if __name__ == '__main__':
    pd_data = global_risk()
    print(pd_data)
