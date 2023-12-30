from analysis.analysis_tool import plot_year_seq_data
from utils.tool import get_data_from_mongo
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')

def analysis_futrues_by_plot_year_day_data(condition=None):
    if condition is None:
        condition = {"symbol": 'B0', "date": {"$gt": '2023-01-01', "$lt": "2023-12-31"}}
    database = 'futures'
    collection = 'futures_daily'
    projection = {'_id': False}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    plot_year_seq_data(data, index_key='date', val_key='close')


def analysis_futrues_by_plot_year_month_data(condition=None,title=None):
    if condition is None:
        condition = {"symbol": 'B0', "date": {"$gt": '2023-01-01', "$lt": "2023-12-31"}}
    if title is None:
        title = '大豆月度分析'
    database = 'futures'
    collection = 'futures_daily'
    projection = {'_id': False}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    data['date'] = pd.to_datetime(data['date'])
    data.set_index(keys='date', inplace=True)
    data['close'] = data['close'].astype(float)
    data = data['close'].resample("M").mean()
    show_dict_data = {}
    month_dict_config = {"01": '1月', "02": '2月', "03": '3月', "04": '4月', "05": '5月', "06": '6月',
                         "07": '7月', "08": '8月', "09": '9月', "10": '10月', "11": '11月', "12": '12月'}
    for index in data.index:
        date = str(index)
        month = date.split("-")[1]
        year = date.split("-")[0]
        val = data.loc[index]
        if year not in show_dict_data.keys():
            show_dict_data[year] = { k:0 for k,v in month_dict_config.items()}
        show_dict_data[year][month] = val
    pd_data_dict = {}
    for year,month_dict in show_dict_data.items():
        if year not in pd_data_dict.keys():
            pd_data_dict[year] = []
        for k,_ in month_dict_config.items():
            pd_data_dict[year].append(month_dict.get(k))
    pd.DataFrame(pd_data_dict,index=list(month_dict_config.values())).plot(kind='line', title=title, rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


if __name__ == '__main__':
    analysis_futrues_by_plot_year_month_data()
