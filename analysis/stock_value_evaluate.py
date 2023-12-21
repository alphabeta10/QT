import akshare as ak
import numpy as np
from utils.actions import show_data
from utils.tool import get_data_from_mongo, sort_dict_data_by
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')

# 显示所有列
pd.set_option('display.max_columns', None)
# 显示所有行
pd.set_option('display.max_rows', None)
# 设置value的显示长度为100，默认为50
pd.set_option('max_colwidth', 100)
import warnings

warnings.filterwarnings('ignore')


def stock_dividend_model(symbol='600919', k=0.06, g=0, year='2023'):
    """
    股息股价
    :param symbol:
    :param k:
    :param g:
    :param year:
    :return:
    """
    stock_history_dividend_detail_df = ak.stock_history_dividend_detail(symbol=symbol, indicator="分红")
    data = stock_history_dividend_detail_df[['除权除息日', '派息']]
    data['除权除息日'] = data['除权除息日'].astype(str)
    data = data[data['除权除息日'].str.contains(year)]
    data['除权除息日'] = pd.to_datetime(data['除权除息日'])
    sum_year_of_rate = data.resample('Y', on='除权除息日').sum()
    # k = 0.07 #年化回报率 自定义
    # g = 0.0 #股息增长率 自定义可根据派息历史增长率做出合理估值
    sum_year_of_rate['k'] = 0.3
    # sum_year_of_rate['g'] = sum_year_of_rate['派息'].pct_change()
    sum_year_of_rate['predict_price'] = sum_year_of_rate['派息'] / ((k - g) * 10)
    return round(sum_year_of_rate.iloc[0].predict_price, 4)


def stock_bank_div_model_ev():
    """
    银行分红模型评估股价入口
    :return:
    """
    database = 'stock'
    collection = 'ticker_info'
    projection = {'_id': False}
    condition = {"name": {"$regex": "银行"}}
    sort_key = "ts_code"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)

    print(data)
    code_dict_data = {}
    for index in data.index:
        ele = data.loc[index]
        ts_code = ele['ts_code']
        code_dict_data[ts_code.split(".")[0]] = ele['name']
    print(code_dict_data)
    codes = list(code_dict_data.keys())

    collection = 'ticker_daily'
    projection = {"_id": False, "time": True, "code": True, "close": True}
    condition = {"code": {"$in": codes}, "time": "2023-09-01"}
    close_data = get_data_from_mongo(database=database, collection=collection, projection=projection,
                                     condition=condition,
                                     sort_key=sort_key)

    rs_dict = {}
    for code, name in code_dict_data.items():
        price = stock_dividend_model(code)
        rs_dict[name] = price
        print(name, price)
    print(rs_dict)
    diff_rs = {}
    for index in close_data.index:
        ele = dict(close_data.loc[index])
        code = ele['code']
        close = ele['close']
        ev_price = rs_dict[code_dict_data.get(code)]
        rate = (ev_price - close) / close
        print(code, close, ev_price, rate)
        if rate > 0:
            if "gt" not in diff_rs.keys():
                diff_rs['gt'] = []
            diff_rs['gt'].append([code_dict_data.get(code), code, close, ev_price, rate])
        else:
            if "lt" not in diff_rs.keys():
                diff_rs['lt'] = []
            diff_rs['lt'].append([code_dict_data.get(code), code, close, ev_price, rate])
    print(diff_rs)


def stock_dcf_model(code_dict=None,dis_rate=None,last_year_growth=None,custom_cur_cash=None,custom_cash_growth=None,year=10):
    # 自定义数值处理逻辑
    def convert_ele(ele):
        if ele == '--':
            ele = 0
        if float(ele) < 0:
            return 0
        return float(ele) / 1e8

    if code_dict is None:
        code_dict = {"sh600519": "贵州茅台", "sh603919": "金微酒", "sh603019": "中科曙光", "sz002594": "比亚迪"}
    if dis_rate is None:
        dis_rate = {"600519":0.03,"603919":0.03,"603019":0.03,"002594":0.03}
    if last_year_growth is None:
        last_year_growth = {"600519": 0.02, "603919": 0.02, "603019": 0.02, "002594": 0.02}
    data_type = 'cash_flow_report_em_detail'
    #handle_comm_stock_fin_em(codes=list(code_dict.keys()), data_type=data_type)
    codes = [code[2:] for code in code_dict.keys()]
    database = 'stock'
    collection = 'fin'
    projection = {'_id': False, 'date': True, 'NETCASH_OPERATE': True, 'code': True}
    condition = {"code": {"$in": codes}, "data_type": data_type}
    sort_key = "date"
    pd_data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                                  sort_key=sort_key)
    pd_data['NETCASH_OPERATE'] = pd_data['NETCASH_OPERATE'].apply(convert_ele)
    pd_data = pd_data[pd_data['date'].str.contains("12-31")]
    data = pd.pivot_table(pd_data, values='NETCASH_OPERATE', index=['date'], columns=['code'])
    cur_cash = dict(data.tail(4).mean())
    cur_cash['temp_code'] = 8.28
    dis_rate['temp_code'] = 0.03
    if custom_cur_cash is not None:
        for k,v in custom_cur_cash.items():
            cur_cash[k] = v
    print(data)
    data = data.tail(5).pct_change()
    print(data)
    cash_growth_rate = dict(data.mean())
    cash_growth_rate['temp_code'] = 0.15
    last_year_growth['temp_code'] = 0.02
    if custom_cash_growth is not None:
        for k,v in custom_cash_growth.items():
            cash_growth_rate[k] = v
    print(cash_growth_rate)
    dcf_model_result = {}
    for i in range(year):
        for code, cash in cur_cash.items():
            if code not in dcf_model_result.keys():
                dcf_model_result[code] = 0
            rate_tmp = np.power((1 + cash_growth_rate.get(code)) / (1 + dis_rate.get(code)), i + 1)
            dcf_model_result[code] += cash * rate_tmp
            if i + 1 == year:
                dcf_model_result[code] += ((cash * np.power(1 + cash_growth_rate.get(code), year) * (
                        1 + last_year_growth.get(code)) * (1 + dis_rate.get(code))) / (dis_rate.get(code) - last_year_growth.get(code))) * (
                                                  1 / np.power(1 + dis_rate.get(code), year + 1))

    print(dcf_model_result)
    return dcf_model_result


if __name__ == '__main__':
    stock_dcf_model()
