from data.mongodb import get_mongo_table
import pandas as pd
from utils.actions import show_data
from utils.tool import get_data_from_mongo, sort_dict_data_by
import matplotlib.pyplot as plt
import seaborn as sns
import akshare as ak
from datetime import datetime
#设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')

"""
影响大盘数据分析
"""
def analysis_market_emo():
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    data_type_list = ['stock_reduction',"stock_overweight"]
    st_result_dict = {}
    for ele in news.find({"data_type": {"$in":data_type_list},"time":{"$gt":"2023-01-01"}}, projection={'_id': False}).sort("time"):
        datas.append(ele)
        data_type = ele['data_type']
        time = ele['time']
        if time not in st_result_dict.keys():
            st_result_dict[time] = {}
        if data_type not in st_result_dict[time].keys():
            st_result_dict[time][data_type] = 0
        st_result_dict[time][data_type] += 1
    pd_data_list = []
    for time,st_rs in st_result_dict.items():
        stock_overweight = 0
        stock_reduction = 0
        if "stock_overweight" in st_rs.keys():
            stock_overweight = st_rs['stock_overweight']
        if "stock_reduction" in st_rs.keys():
            stock_reduction = st_rs['stock_reduction']
        dict_data = {"date":time,"stock_overweight":stock_overweight,"stock_reduction":stock_reduction,"diff":stock_overweight-stock_reduction}
        pd_data_list.append(dict_data)
    stock_st_hold_data = pd.DataFrame(data=pd_data_list)
    stock_st_hold_data['date'] = pd.to_datetime(stock_st_hold_data['date'])
    stock_st_hold_data.set_index(keys='date',inplace=True)
    stock_st_hold_data = stock_st_hold_data.resample("W").sum()
    data = stock_st_hold_data.copy()
    data.plot(kind='line', title="股东增持情况", rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


    index_data = get_mongo_table(database='stock', collection='index_data')
    code_list = ['sh000001']
    datas = []
    for ele in index_data.find({"code": {"$in":code_list},"date":{"$gt":"2023-01-01"}}, projection={'_id': False,"date":True,"close":True}).sort("date"):
        datas.append(ele)
    index_pd = pd.DataFrame(data=datas)
    index_pd['date'] = pd.to_datetime(index_pd['date'])
    index_pd.set_index(keys='date',inplace=True)
    index_pd['ret'] = index_pd['close'].pct_change()
    index_pd.dropna(inplace=True)

    index_pd = (index_pd+1).resample("W").prod()-1
    data = index_pd.copy()
    data['ret'].plot(kind='line', title="上证收益情况", rot=45, figsize=(15, 8), fontsize=10)
    plt.show()

    merge_data = pd.merge(index_pd,stock_st_hold_data,left_index=True,right_index=True)
    print(merge_data.head())
    merge_data = merge_data[['ret','stock_overweight','stock_reduction','diff']]
    method = 'pearson' # spearman
    corr = merge_data.corr(method=method)
    print(corr)
    sns.heatmap(corr, linewidths=0.1, vmax=1.0, square=True, linecolor='white', annot=True)
    plt.show()

    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    for ele in goods.find({"time": {"$gt":"2023-01-01"}, "data_type": "goods_price"}, projection={'_id': False,"name":True,"value":True,"time":True}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    pd_data['date'] = pd.to_datetime(pd_data['time'])
    pd_data[['value']] = pd_data[['value']].astype(float)
    pivot_data = pd.pivot_table(pd_data,values='value',index='date',columns='name')
    pivot_data = pivot_data.pct_change(1)
    goods_data = (pivot_data+1).resample("W").prod()-1
    merge_data = pd.merge(merge_data, goods_data, left_index=True, right_index=True)
    corr = merge_data.corr()
    corr_dict = dict(corr.loc['ret'])
    print(sort_dict_data_by(corr_dict,by='value'))

    # sns.heatmap(corr, linewidths=0.1, vmax=1.0, square=True, linecolor='white', annot=True)
    # plt.show()

    """
    解禁和股东减持和市场收益的相关系数为0.12-0.2之间，统计结论是弱相关
    """



def temp_analysis():
    stock_restricted_release_summary_em_df = ak.stock_restricted_release_summary_em(symbol="全部股票",
                                                                                    start_date="20230101",
                                                                                    end_date="20231216")
    #show_data(stock_restricted_release_summary_em_df)

    stock_restricted_release_summary_em_df['当日解禁股票家数'].plot(kind='line', title="当日解禁股票家数情况", rot=45, figsize=(15, 8), fontsize=10)
    stock_restricted_release_summary_em_df['解禁时间'] = pd.to_datetime(stock_restricted_release_summary_em_df['解禁时间'])
    stock_restricted_release_summary_em_df.set_index(keys='解禁时间',inplace=True)
    print(stock_restricted_release_summary_em_df['当日解禁股票家数'])
    month_count = stock_restricted_release_summary_em_df['当日解禁股票家数'].resample("M").sum()
    print(month_count)

    corr = stock_restricted_release_summary_em_df[['当日解禁股票家数','解禁数量','实际解禁数量','实际解禁市值','沪深300指数','沪深300指数涨跌幅']].corr()
    show_data(corr)
    sns.heatmap(corr, linewidths=0.1, vmax=1.0, square=True, linecolor='white', annot=True)
    plt.show()

    month_300ret = (stock_restricted_release_summary_em_df['沪深300指数涨跌幅']+1).resample("M").prod()-1
    print(month_300ret)

    merge_data = pd.merge(month_300ret,month_count,left_index=True,right_index=True)
    corr = merge_data.corr()
    show_data(corr)
    sns.heatmap(corr, linewidths=0.1, vmax=1.0, square=True, linecolor='white', annot=True)
    plt.show()

    index_data = get_mongo_table(database='stock', collection='index_data')
    code_list = ['sh000001']
    datas = []
    for ele in index_data.find({"code": {"$in": code_list}, "date": {"$gt": "2023-01-01"}},
                               projection={'_id': False, "date": True, "close": True}).sort("date"):
        datas.append(ele)
    index_pd = pd.DataFrame(data=datas)
    index_pd['date'] = pd.to_datetime(index_pd['date'])
    index_pd.set_index(keys='date', inplace=True)
    index_pd['ret'] = index_pd['close'].pct_change()
    index_pd.dropna(inplace=True)

    month_001_ret = (index_pd['ret'] + 1).resample("M").prod() - 1
    print(month_001_ret)
    merge_data = pd.merge(month_001_ret, month_count, left_index=True, right_index=True)
    corr = merge_data.corr()
    show_data(corr)
    sns.heatmap(corr, linewidths=0.1, vmax=1.0, square=True, linecolor='white', annot=True)
    plt.show()


def get_index_data(codes = None,time='203-01-01'):
    index_data = get_mongo_table(database='stock', collection='index_data')
    if codes is None:
        codes = ['sh000001']
    datas = []
    for ele in index_data.find({"code": {"$in": codes}, "date": {"$gt": time}},
                               projection={'_id': False, "date": True, "close": True}).sort("date"):
        datas.append(ele)
    index_pd = pd.DataFrame(data=datas)
    index_pd['date'] = pd.to_datetime(index_pd['date'])
    index_pd.set_index(keys='date', inplace=True)
    return index_pd

def st_market_analysis():

    """
    cpi和大盘指数分析
    :return:
    """
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False, 'code': True, 'time': True, "data": True}
    time = '202201'
    code_dict = {'A01020101_yd': '居民消费价格指数(上年同期=100)'}
    if time is None:
        time = str(int(datetime.now().strftime("%Y")) - 0)
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data['time'] = pd.to_datetime(data['time'],format="%Y%m")
    data.set_index(keys='time',inplace=True)
    data = data['data']
    data = data.to_period("M")
    print(data)
    index_pd = get_index_data(time='2022-01-01')
    index_pd['daily_ret'] = index_pd['close'].pct_change()
    index_pd.dropna(inplace=True)
    month_001_ret = (index_pd['daily_ret'] + 1).resample("M").prod() - 1
    month_001_ret = month_001_ret.to_period("M")
    print(month_001_ret)
    merge_data = pd.merge(month_001_ret, data, left_index=True, right_index=True)
    corr = merge_data.corr()
    show_data(corr)
    sns.heatmap(corr, linewidths=0.1, vmax=1.0, square=True, linecolor='white', annot=True)
    plt.show()

    """
    当前环境影响市场的因素有很多，2022年有疫情影响，cpi的影响偏小，疫情放开之后，市场对未来预期偏好，大盘上涨，最近大盘下跌，
    很有可能对外面环境经济增长放缓，而导致市场情绪波动大
    美国 2024 GDP增长1.4%低于2023
    中国 2024 GDP增长5%，不同国家对此有不同的预期、
    日本 无
    德国 2024 GDP增长0.4%
    印度 2024 GDP增长 7%
    英国 2024 GDP增长 0.8%
    法国
    俄罗斯 2.7%
    意大利 0.6%
    整体欧盟下调2024GDP增长
    大概率2024经济增长放缓
    中国、日本、德国、印度、英国、法国、俄罗斯、加拿大、意大利
    """

def fund_and_market_analysis():
    income_config = {
        "住户贷款": "loans_to_households",
        "住户短期贷款": "short_term_loans",
        "住户中长期贷款": "mid_long_term_loans",
        "(事)业单位贷款": "loans_to_non_financial_enterprises_and_government_departments_organizations",
        "企业短期贷款": "short_term_loans_1",
        "企业中长期贷款": "mid_long_term_loans_1",
    }
    all_income_config = {
        "time": True,
        "_id": False
    }
    for _, v in income_config.items():
        all_income_config[v] = True

    re_all_config = {}
    for k, v in income_config.items():
        re_all_config[v] = k
    print(re_all_config)
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    for ele in news.find({"data_type": "credit_funds", "metric_code": "credit_funds_fin_inst_rmb","time":{"$gt":"20220101"}},
                         projection=all_income_config).sort(
        "time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data.rename(columns=re_all_config)
    for col in income_config.keys():
        data[[col]] = data[[col]].astype(float)

    data['time'] = pd.to_datetime(data['time'],format="%Y%m%d")
    data.set_index(keys=['time'], inplace=True)
    data = data.diff()
    data = data.to_period("M")
    show_data(data)

    index_pd = get_index_data(time='2022-01-01')
    index_pd['daily_ret'] = index_pd['close'].pct_change()
    index_pd.dropna(inplace=True)
    month_001_ret = (index_pd['daily_ret'] + 1).resample("M").prod() - 1
    month_001_ret = month_001_ret.to_period("M")
    print(month_001_ret)
    merge_data = pd.merge(month_001_ret, data, left_index=True, right_index=True)
    corr = merge_data.corr()
    show_data(corr)
    sns.heatmap(corr, linewidths=0.1, vmax=1.0, square=True, linecolor='white', annot=True)
    plt.show()



if __name__ == '__main__':
    fund_and_market_analysis()