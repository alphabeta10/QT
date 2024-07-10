"""
股票基本分析，波动率，风险，等
"""
import os.path
from datetime import datetime, timedelta
from big_models.google_api import *
from analysis.analysis_tool import *
import pandas as pd
import matplotlib.pyplot as plt
from utils.tool import load_json_data, comm_read_stock
import google.generativeai as genai
from utils.actions import try_get_action
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
from tqdm import tqdm
from analysis.fin_analysis import get_fin_common_metric
from utils.tool import sort_dict_data_by
from pyecharts.faker import Faker
from pyecharts.charts import Bar
from pyecharts.components import Table
from pyecharts import options as opts
from pyecharts.charts import Page
import akshare as ak

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


def bank_stock_analysis():
    start_date = '2023-01-01'
    end_date = '2023-09-11'

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
        name = ele['name']
        ts_code = ele['ts_code']
        print(dict(ele))
        code_dict_data[ts_code.split(".")[0]] = ele['name']
    print(code_dict_data)
    codes = list(code_dict_data.keys())
    data = get_data_from_mongo(database="stock", collection='ticker_daily',
                               condition={"code": {"$in": codes}, "time": {"$gte": start_date}},
                               projection={'_id': False, "code": True, "close": True, "time": True})

    data = pd.pivot_table(data, values='close', index=['time'], columns=['code'])
    data.rename(columns=code_dict_data, inplace=True)
    data.index = pd.to_datetime(data.index)
    ret_daily = data.pct_change(1)
    ret_daily = ret_daily.dropna()

    df_week = (ret_daily + 1).resample('W').prod() - 1
    df_week = df_week.drop(df_week.index[[0]])
    df_week = df_week.drop(df_week.index[[-1]])
    data = df_week

    n_dec = 2
    SumStat = pd.DataFrame(index=data.columns)
    SumStat['Geo Mean(Annu,%)'] = np.round(data.apply(ann_geo_mean) * 100, n_dec)
    SumStat['Volatility(Annu,%)'] = np.round(ann_std(data.std()) * 100, n_dec)
    SumStat['Sharpe Ratio (Annu)'] = np.round(data.apply(ann_sr, rf=0.0025), n_dec)
    # SumStat['Max Drawdown(%)'] = np.round(data.apply(mdd) * 100, n_dec)
    SumStat.sort_values(by='Volatility(Annu,%)', inplace=True)
    print("*" * 25 + "sort by Volatility(Annu,%)" + "*" * 25)
    print(SumStat)

    print("*" * 25 + "sort by Geo Mean(Annu,%)" + "*" * 25)
    SumStat.sort_values(by='Geo Mean(Annu,%)', inplace=True)
    print(SumStat)

    print("*" * 25 + "sort by Sharpe Ratio (Annu)" + "*" * 25)
    SumStat.sort_values(by='Sharpe Ratio (Annu)', inplace=True)
    print(SumStat)

    sh001 = get_market_data()
    sh001 = (sh001 + 1).resample('W').prod() - 1
    sh001 = sh001.drop(sh001.index[[0]])
    sh001 = sh001.drop(sh001.index[[-1]])

    ind = (data.index >= start_date) * (data.index <= end_date)
    data = data[ind]
    ind = (sh001.index >= start_date) * (sh001.index <= end_date)
    sh001 = sh001[ind]

    mkt_ex_ret = sh001 - 0.00025
    ex_ret = data - 0.00025

    n = len(ex_ret.columns)
    beta = np.zeros(n)
    alpha = np.zeros(n)
    for i in range(n):
        beta[i], alpha[i] = LR(mkt_ex_ret.values, ex_ret[ex_ret.columns[i]].values)
    AlphaBeta = pd.DataFrame(index=data.columns)
    AlphaBeta['Alpha(Annu,%)'] = np.round(ann_ret(alpha) * 100, n_dec)
    AlphaBeta['Beta'] = np.round(beta, n_dec)

    print("*" * 25 + "sort by Alpha(Annu,%)" + "*" * 25)
    AlphaBeta.sort_values(by='Alpha(Annu,%)', inplace=True)
    print(AlphaBeta)

    print("*" * 25 + "sort by Beta" + "*" * 25)
    AlphaBeta.sort_values(by='Beta', inplace=True)
    print(AlphaBeta)


def mark_port_opt(r, data_mean, data_cov, silent=False):
    def constraint0(w):
        return np.sum(w) - 1.0

    def constraint1(w):
        return 1.0 - np.sum(w)

    def constraint2(w):
        return w

    def constraint3(w):
        diff = ereturn(w, data_mean) - r
        return diff

    con1 = {'type': 'ineq', 'fun': constraint0}
    con2 = {'type': 'ineq', 'fun': constraint1}
    con3 = {'type': 'ineq', 'fun': constraint2}
    con4 = {'type': 'ineq', 'fun': constraint3}

    cons = ([con1, con2, con3, con4])
    w0 = np.ones(len(data_mean))
    sol = minimize(pvol, w0, args=(data_cov,), method="SLSQP", constraints=cons)

    if (not silent):
        print("Solution to the Markowitz Problem with r =  ", round(r * 100, 3), "%:")
        print(sol)
        print("")
    elif (not sol['success']):  # check if the optimizer exit successfully
        print("WARNING:  the optimizer did NOT exit successfully!!")
    return sol


def bank_stock_portfolio():
    start_date = '2023-01-01'
    end_date = '2023-09-11'

    database = 'stock'
    collection = 'ticker_info'
    projection = {'_id': False}
    condition = {"name": {"$regex": "银行"}}
    sort_key = "ts_code"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    code_dict_data = {}
    for index in data.index:
        ele = data.loc[index]
        ts_code = ele['ts_code']
        print(dict(ele))
        code_dict_data[ts_code.split(".")[0]] = ele['name']
    codes = list(code_dict_data.keys())
    data = get_data_from_mongo(database="stock", collection='ticker_daily',
                               condition={"code": {"$in": codes}, "time": {"$gte": start_date}},
                               projection={'_id': False, "code": True, "close": True, "time": True})

    data = pd.pivot_table(data, values='close', index=['time'], columns=['code'])
    data.rename(columns=code_dict_data, inplace=True)
    data.index = pd.to_datetime(data.index)
    ret_daily = data.pct_change(1)
    ret_daily = ret_daily.dropna()

    df_week = (ret_daily + 1).resample('W').prod() - 1
    df_week = df_week.drop(df_week.index[[0]])
    df_week = df_week.drop(df_week.index[[-1]])
    data = df_week
    data_train_mean = data.mean()
    data_train_cov_mat = data.cov()

    print(data_train_cov_mat)
    print("*" * 50)
    print(data_train_mean)
    sol = mark_port_opt(r=0.001, data_mean=data_train_mean, data_cov=data_train_cov_mat, silent=False)

    num_r = 14
    r_bar = np.array([i for i in range(num_r - 3)]) * 0.0003 + 0.002
    r_bar = np.append(r_bar, np.array([0.00505, 0.0051, 0.00515]))

    # r_bar = np.array([i for i in range(num_r - 3)]) * 0.003 + 0.0002
    # r_bar = np.append(r_bar, np.array([0.00505, 0.0051, 0.00515]))

    print(r_bar)

    if (len(r_bar) != num_r):
        num_r = len(r_bar)
    print("Number of targeted returns (or r-bar) specified is: ", num_r)

    # Two lists to record the volatility and expected return for each portfilio
    port_vol = []
    port_return = []

    # A matrix storing the portfolio alloaction
    alloc_r = np.zeros((len(data.columns), num_r))

    # Solve the Markowitz problem for each r-bar and output the results
    for i in range(num_r):
        r = r_bar[i]
        print("* For the case r-bar = ", round(r * 100, 3), "%:")
        sol = mark_port_opt(r, data_train_mean, data_train_cov_mat, silent=True)

        if (not sol['success']):  # check if the optimizer exit successfully
            print("NOTE: solution to this r-bar will be dropped!")
        else:  # only keeping the r-bar that has sucessful optmization
            print(sol['message'])
            alloc_r[:, i] = sol['x']
            port_vol.append(sol['fun'])
            port_return.append(ereturn(sol['x'], data_train_mean))
        print("")

    port_vol = np.asarray(port_vol)
    port_return = np.asarray(port_return)

    num_rbar = len(port_vol)  # update the number of r-bar recorded/kept
    print("The number of recoreded the efficient frontier points is:", num_rbar)

    # Display the optimal allocation for each specified target return
    DF_Alloc_R = pd.DataFrame(alloc_r)
    DF_Alloc_R.index = data.columns
    DF_Alloc_R.columns = [str(round(ann_ret(r) * 100, 1)) + "%" for r in r_bar]
    DF_Alloc_R = DF_Alloc_R.loc[:,
                 (DF_Alloc_R != 0).any(axis=0)]  # drop the r-bar solution(s) that failed the opt. problem

    print('Optimal allocation (in %) for specified (annualized) target return:')
    print(np.round(DF_Alloc_R * 100, 1))  # allocation in % and round (to the 1st decimal)

    # Plotting efficient frontier
    plt.rcParams['figure.figsize'] = (11, 6)
    plt.plot(ann_std(port_vol), ann_ret(port_return), 'ro--', label='efficient_frontier')
    plt.xlabel('Annualized Volatility')
    plt.ylabel('Annualized Return')
    plt.grid(True, linestyle='--')
    plt.legend()
    plt.title('Efficient Frontier (' + str(start_date) + ' to ' + str(end_date) + ')')
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.gca().xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.show()

    sol = MaxSR(data, 0.003)
    w_sr = sol['x']

    opt_vol = pvol(sol['x'], data_train_cov_mat)
    opt_return = ereturn(sol['x'], data_train_mean)

    sr = ann_sr(sol['x'], 0.003)

    # Print (annualized) return, volatiltiy and Sharpe ratio information
    print("* The expected return (annualized) for the optimal portfolio is ", ann_ret(opt_return))
    print("* The volatility (annualized) for the optimal portfolio is ", ann_std(opt_vol))
    print("* The Sharpe ratio (annualized) for the optimal portfolio is ", sr)
    print()

    # Display the optimal allocation after adding in the allocation correspond to the max SR
    DF_Alloc_R['maxSR'] = w_sr
    DF_Alloc_R = DF_Alloc_R.round(4)
    print('Optimal allocation (in %) for specified target return:')
    print(DF_Alloc_R)


def comm_portfolio_analysis(code_dict_data, start_date, end_date):
    codes = list(code_dict_data.keys())
    data = get_data_from_mongo(database="stock", collection='ticker_daily',
                               condition={"code": {"$in": codes}, "time": {"$gte": start_date}},
                               projection={'_id': False, "code": True, "close": True, "time": True})

    data = pd.pivot_table(data, values='close', index=['time'], columns=['code'])
    data.rename(columns=code_dict_data, inplace=True)
    data.index = pd.to_datetime(data.index)
    ret_daily = data.pct_change(1)
    ret_daily = ret_daily.dropna()

    df_week = (ret_daily + 1).resample('W').prod() - 1
    df_week = df_week.drop(df_week.index[[0]])
    df_week = df_week.drop(df_week.index[[-1]])
    data = df_week
    data_train_mean = data.mean()
    data_train_cov_mat = data.cov()

    print(data_train_cov_mat)
    print("*" * 50)
    print(data_train_mean)
    sol = mark_port_opt(r=0.001, data_mean=data_train_mean, data_cov=data_train_cov_mat, silent=False)

    num_r = 14
    r_bar = np.array([i for i in range(num_r - 3)]) * 0.0003 + 0.002
    r_bar = np.append(r_bar, np.array([0.00505, 0.0051, 0.00515]))

    # r_bar = np.array([i for i in range(num_r - 3)]) * 0.003 + 0.0002
    # r_bar = np.append(r_bar, np.array([0.00505, 0.0051, 0.00515]))

    print(r_bar)

    if (len(r_bar) != num_r):
        num_r = len(r_bar)
    print("Number of targeted returns (or r-bar) specified is: ", num_r)

    # Two lists to record the volatility and expected return for each portfilio
    port_vol = []
    port_return = []

    # A matrix storing the portfolio alloaction
    alloc_r = np.zeros((len(data.columns), num_r))

    # Solve the Markowitz problem for each r-bar and output the results
    for i in range(num_r):
        r = r_bar[i]
        print("* For the case r-bar = ", round(r * 100, 3), "%:")
        sol = mark_port_opt(r, data_train_mean, data_train_cov_mat, silent=True)

        if (not sol['success']):  # check if the optimizer exit successfully
            print("NOTE: solution to this r-bar will be dropped!")
        else:  # only keeping the r-bar that has sucessful optmization
            print(sol['message'])
            alloc_r[:, i] = sol['x']
            port_vol.append(sol['fun'])
            port_return.append(ereturn(sol['x'], data_train_mean))
        print("")

    port_vol = np.asarray(port_vol)
    port_return = np.asarray(port_return)

    num_rbar = len(port_vol)  # update the number of r-bar recorded/kept
    print("The number of recoreded the efficient frontier points is:", num_rbar)

    # Display the optimal allocation for each specified target return
    DF_Alloc_R = pd.DataFrame(alloc_r)
    DF_Alloc_R.index = data.columns
    DF_Alloc_R.columns = [str(round(ann_ret(r) * 100, 1)) + "%" for r in r_bar]
    DF_Alloc_R = DF_Alloc_R.loc[:,
                 (DF_Alloc_R != 0).any(axis=0)]  # drop the r-bar solution(s) that failed the opt. problem

    print('Optimal allocation (in %) for specified (annualized) target return:')
    print(np.round(DF_Alloc_R * 100, 1))  # allocation in % and round (to the 1st decimal)

    # Plotting efficient frontier
    plt.rcParams['figure.figsize'] = (11, 6)
    plt.plot(ann_std(port_vol), ann_ret(port_return), 'ro--', label='efficient_frontier')
    plt.xlabel('Annualized Volatility')
    plt.ylabel('Annualized Return')
    plt.grid(True, linestyle='--')
    plt.legend()
    plt.title('Efficient Frontier (' + str(start_date) + ' to ' + str(end_date) + ')')
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.gca().xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.show()

    sol = MaxSR(data, 0.003)
    w_sr = sol['x']

    opt_vol = pvol(sol['x'], data_train_cov_mat)
    opt_return = ereturn(sol['x'], data_train_mean)

    sr = ann_sr(sol['x'], 0.003)

    # Print (annualized) return, volatiltiy and Sharpe ratio information
    print("* The expected return (annualized) for the optimal portfolio is ", ann_ret(opt_return))
    print("* The volatility (annualized) for the optimal portfolio is ", ann_std(opt_vol))
    print("* The Sharpe ratio (annualized) for the optimal portfolio is ", sr)
    print()

    # Display the optimal allocation after adding in the allocation correspond to the max SR
    DF_Alloc_R['maxSR'] = w_sr
    DF_Alloc_R = DF_Alloc_R.round(4)
    print('Optimal allocation (in %) for specified target return:')
    print(DF_Alloc_R)
    return DF_Alloc_R


def index_stock_portfolio():
    start_date = '2020-01-01'
    end_date = '2023-11-12'
    data = get_data_from_mongo(database="stock", collection='concept_data',
                               condition={"time": {"$gte": start_date}},
                               projection={'_id': False, "code": True, "close": True, "time": True, "name": True})

    data = pd.pivot_table(data, values='close', index=['time'], columns=['name'])
    # data.rename(columns=code_dict_data, inplace=True)
    data.dropna(axis=1, inplace=True)
    data.index = pd.to_datetime(data.index)
    ret_daily = data.pct_change(1)
    ret_daily = ret_daily.dropna()

    df_week = (ret_daily + 1).resample('W').prod() - 1
    df_week = df_week.drop(df_week.index[[0]])
    df_week = df_week.drop(df_week.index[[-1]])
    data = df_week
    data_train_mean = data.mean()
    data_train_cov_mat = data.cov()

    print(data_train_cov_mat)
    print("*" * 50)
    print(data_train_mean)
    sol = mark_port_opt(r=0.001, data_mean=data_train_mean, data_cov=data_train_cov_mat, silent=False)

    num_r = 14
    r_bar = np.array([i for i in range(num_r - 3)]) * 0.0003 + 0.002
    r_bar = np.append(r_bar, np.array([0.00505, 0.0051, 0.00515]))

    # r_bar = np.array([i for i in range(num_r - 3)]) * 0.003 + 0.0002
    # r_bar = np.append(r_bar, np.array([0.00505, 0.0051, 0.00515]))

    print(r_bar)

    if (len(r_bar) != num_r):
        num_r = len(r_bar)
    print("Number of targeted returns (or r-bar) specified is: ", num_r)

    # Two lists to record the volatility and expected return for each portfilio
    port_vol = []
    port_return = []

    # A matrix storing the portfolio alloaction
    alloc_r = np.zeros((len(data.columns), num_r))

    # Solve the Markowitz problem for each r-bar and output the results
    for i in range(num_r):
        r = r_bar[i]
        print("* For the case r-bar = ", round(r * 100, 3), "%:")
        sol = mark_port_opt(r, data_train_mean, data_train_cov_mat, silent=True)

        if (not sol['success']):  # check if the optimizer exit successfully
            print("NOTE: solution to this r-bar will be dropped!")
        else:  # only keeping the r-bar that has sucessful optmization
            print(sol['message'])
            alloc_r[:, i] = sol['x']
            port_vol.append(sol['fun'])
            port_return.append(ereturn(sol['x'], data_train_mean))
        print("")

    port_vol = np.asarray(port_vol)
    port_return = np.asarray(port_return)

    num_rbar = len(port_vol)  # update the number of r-bar recorded/kept
    print("The number of recoreded the efficient frontier points is:", num_rbar)

    # Display the optimal allocation for each specified target return
    DF_Alloc_R = pd.DataFrame(alloc_r)
    DF_Alloc_R.index = data.columns
    DF_Alloc_R.columns = [str(round(ann_ret(r) * 100, 1)) + "%" for r in r_bar]
    DF_Alloc_R = DF_Alloc_R.loc[:,
                 (DF_Alloc_R != 0).any(axis=0)]  # drop the r-bar solution(s) that failed the opt. problem

    print('Optimal allocation (in %) for specified (annualized) target return:')
    print(np.round(DF_Alloc_R * 100, 1))  # allocation in % and round (to the 1st decimal)

    # Plotting efficient frontier
    plt.rcParams['figure.figsize'] = (11, 6)
    plt.plot(ann_std(port_vol), ann_ret(port_return), 'ro--', label='efficient_frontier')
    plt.xlabel('Annualized Volatility')
    plt.ylabel('Annualized Return')
    plt.grid(True, linestyle='--')
    plt.legend()
    plt.title('Efficient Frontier (' + str(start_date) + ' to ' + str(end_date) + ')')
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.gca().xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.show()

    sol = MaxSR(data, 0.003)
    w_sr = sol['x']

    opt_vol = pvol(sol['x'], data_train_cov_mat)
    opt_return = ereturn(sol['x'], data_train_mean)

    sr = ann_sr(sol['x'], 0.003)

    # Print (annualized) return, volatiltiy and Sharpe ratio information
    print("* The expected return (annualized) for the optimal portfolio is ", ann_ret(opt_return))
    print("* The volatility (annualized) for the optimal portfolio is ", ann_std(opt_vol))
    print("* The Sharpe ratio (annualized) for the optimal portfolio is ", sr)
    print()

    # Display the optimal allocation after adding in the allocation correspond to the max SR
    DF_Alloc_R['maxSR'] = w_sr
    DF_Alloc_R = DF_Alloc_R.round(4)
    print('Optimal allocation (in %) for specified target return:')
    print(DF_Alloc_R)


def month_portfolio_analysis():
    dict_data = {}
    with open("concept_code.txt", mode='r') as f:
        lines = f.readlines()

        for line in lines:
            splits = line.replace("\n", "").split(",")
            if len(splits) == 2:
                code, name = splits
                dict_data[code] = name
    comm_portfolio_analysis(code_dict_data=dict_data, start_date='2022-01-01', end_date='2023-12-18')


def big_model_stock_price_data(codes: list, model):
    if codes is None:
        print("股票代码必须不为空")
        return
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    condition = {"code": {"$in": codes}, "time": {"$gte": start_date}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False}
    sort_key = "time"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data = pd.pivot_table(data, values='close', index=['time'], columns=['code'])

    def period_metric(pd_data: pd.DataFrame, periods=None):
        if periods is None:
            periods = [30, 60, 240]
        result_df = pd.DataFrame(index=data.columns)
        for peroid in periods:
            acc_ret = (pd_data.tail(peroid) + 1).prod() - 1
            mean = pd_data.tail(peroid).mean()
            std = pd_data.tail(peroid).std()
            sharpe = mean / std
            volatility = pd_data.tail(peroid).std()
            md = mdd(pd_data.tail(peroid))
            result_df[f'{peroid}日夏普'] = np.round(sharpe, 4)
            result_df[f'{peroid}日波动率'] = np.round(volatility, 4)
            result_df[f'{peroid}日最大回撤'] = np.round(md, 4)
            result_df[f'{peroid}日累计收益'] = np.round(acc_ret, 4)
        return result_df

    daily_ret = data.pct_change(1)
    result_df = period_metric(daily_ret)
    result_df['股票代码'] = result_df.index
    request_txt = """给定计算好的不同日期的夏普，波动率，最大回撤以及累计收益，综合分析以及给出一个投资分类[1，0]0表示不能投资，1表示可以投资。输入：| 30日夏普 | 30日波动率 | 30日最大回撤 | 30日累计收益 | 60日夏普 | 60日波动率 | 60日最大回撤 | 60日累计收益 | 240日夏普 | 240日波动率 | 240日最大回撤 | 240日累计收益 | 股票代码 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| -0.1019 | 0.0164 | -0.1081 | -0.0527 | -0.2653 | 0.0142 | -0.2111 | -0.2072 | -0.1124 | 0.0161 | -0.4017 | -0.3714 | 000858 |
| -0.0702 | 0.0136 | -0.0863 | -0.0308 | -0.1444 | 0.0122 | -0.1231 | -0.104 | -0.0277 | 0.0129 | -0.1687 | -0.1004 | 600519 |
输出：{"综合分析":"30日夏普为-0.23, 低于0, 表明该股票在过去30天内的表现不佳。30日波动率为0.03, 表明该股票在过去30天内的波动性较小。30日最大回撤为-0.19, 表明该股票在过去30天内的最大跌幅为19%。30日累计收益为-0.20, 表明该股票在过去30天内的总回报率为-20%。60日夏普为-0.13, 低于0, 表明该股票在过去60天内的表现不佳。60日波动率为0.03, 表明该股票在过去60天内的波动性较小。60日最大回撤为-0.24, 表明该股票在过去60天内的最大跌幅为24%。60日累计收益为-0.20, 表明该股票在过去60天内的总回报率为-20%。240日夏普为-0.01, 低于0, 表明该股票在过去240天内的表现不佳。240日波动率为0.04, 表明该股票在过去240天内的波动性较小。240日最大回撤为-0.53, 表明该股票在过去240天内的最大跌幅为53%。240日累计收益为-0.20, 表明该股票在过去240天内的总回报率为-20%。综合以上，002230在过去不同日期的表现都不佳，不建议投资","603019":"30日夏普为-0.28, 低于0, 表明该股票在过去30天内的表现不佳。30日波动率为0.03, 表明该股票在过去30天内的波动性较小。30日最大回撤为-0.21, 表明该股票在过去30天内的最大跌幅为21%。30日累计收益为-0.21, 表明该股票在过去30天内的总回报率为-21%。60日夏普为-0.15, 低于0, 表明该股票在过去60天内的表现不佳。60日波动率为0.03, 表明该股票在过去60天内的波动性较小。60日最大回撤为-0.25, 表明该股票在过去60天内的最大跌幅为25%。60日累计收益为-0.22, 表明该股票在过去60天内的总回报率为-22%。240日夏普为0.04, 高于0, 表明该股票在过去240天内的表现较好。240日波动率为0.04, 表明该股票在过去240天内的波动性较小。240日最大回撤为-0.49, 表明该股票在过去240天内的最大跌幅为49%。 240日累计收益为0.19, 表明该股票在过去240天内的总回报率为19%。综合以上，603019在过去不同日期的表现都不佳，不建议投资","投资分类":0}\n输入：${input_str}输出："""
    json_data = try_get_action(google_big_gen_model_comm_fn, try_count=3, data_df=result_df, model=model,
                               request_txt=request_txt)
    if json_data is not None and isinstance(json_data, dict) and (
            '综合分析' not in json_data.keys() or '投资分类' not in json_data.keys()):
        json_data = try_get_action(google_big_gen_model_comm_fn, try_count=3, data_df=result_df, model=model,
                                   request_txt=request_txt)
    return json_data


def enter_big_model_analysis_stock_indicator(code_dict: dict = None):
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    version = api_key_json['version']
    genai.configure(api_key=api_key, transport='rest')
    model = genai.GenerativeModel(version)
    year = datetime.now().strftime('%Y-01-01')
    if code_dict is None:
        code_dict = comm_read_stock('../stock.txt')
    update_request = []
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    for code, name in tqdm(code_dict.items()):
        print(f"handle name={name}")
        temp_codes = []
        temp_codes.append(code)
        ret_json = big_model_stock_price_data(temp_codes, model)
        if ret_json is not None:
            summary = None
            invent_state = None
            if '综合分析' in ret_json.keys():
                summary = ret_json['综合分析']
            if '投资分类' in ret_json.keys():
                invent_state = ret_json['投资分类']
            if summary is not None and invent_state is not None:
                new_dict = {"data_type": "stock_price_summary", "abstract": summary,
                            "time": year, "code": code, "invent_state": invent_state}

                update_request.append(
                    UpdateOne({"code": code, 'time': new_dict['time'], "data_type": new_dict['data_type']},
                              {"$set": new_dict},
                              upsert=True)
                )
            else:
                print(f"返回 json 有问题 {ret_json}")
            if len(update_request) > 4:
                mongo_bulk_write_data(big_model_col, update_request)
                update_request.clear()
    if len(update_request) > 0:
        mongo_bulk_write_data(big_model_col, update_request)
        update_request.clear()


class StockBasicAnalysis(object):
    def __init__(self, *args, **kwargs):
        self.code = kwargs['code']
        self.date = kwargs.get("date", str(datetime.now().year - 1) + "年度")
        self.fin_start_date = kwargs.get('fin_start_date', None)
        self.cal_cur_cols = kwargs.get("cal_cur_cols", None)
        self.name = kwargs.get('name',self.code)

        if int(self.code[0]) < 6:
            self.pre_market_code = f"sz{self.code}"
        elif int(self.code[0]) == 6:
            self.pre_market_code = f'sh{self.code}'
        elif int(self.code[0]) == 8:
            self.pre_market_code = f"bj{self.code}"
        else:
            print("没有找到有前缀的数据")
            self.pre_market_code = self.code
        self.google_model = None
        self.current_price_market_info = None

    def load_google_model(self):
        if self.google_model is None:
            api_key_json = load_json_data("google_api.json")
            api_key = api_key_json['api_key']
            version = api_key_json['version']
            genai.configure(api_key=api_key, transport='rest')
            self.google_model = genai.GenerativeModel(version)
        return self.google_model

    def get_stock_industry(self):
        database = 'stock'
        collection = 'business'
        condition = {"date": self.date, "code": self.code, "class_dire": {"$in":["按产品分","按地区分"]}}
        projection = {"_id": False}
        data = get_data_from_mongo(database=database, collection=collection,
                                   condition=condition,
                                   projection=projection)
        data['营业收入-占主营收入比'] = data['营业收入-占主营收入比'].apply(
            lambda ele: round(float(ele.replace("%", "")) / 100.0, 4))
        data.sort_values(by=['class_dire','营业收入-占主营收入比'], inplace=True, ascending=(False,False))
        data = data[data['class'] != '合计']
        return data

    def get_stock_fin_data(self, is_local=False):
        self.fin_data = get_fin_common_metric([self.pre_market_code], isZcfcDataFromLocal=is_local,
                                              isProfitDataFromLocal=is_local, isCashDataFromLocal=is_local,
                                              start_date=self.fin_start_date)
        self.fin_data = pd.DataFrame(self.common_cal_result(self.fin_data,{},def_fn=self.cal_metric_fn))
        self.common_cal_cur_data()
        self.analysis_flow_cash_data()

    def cal_metric_fn(self, cur_dict_data: dict, before_dict_data: dict):

        # 资本积累率：所有者权益同比增长 TOTAL_EQUITY
        # 营业利润增长率
        same_config = {"资本积累率": "TOTAL_EQUITY", "营业利润增长率": "OPERATE_PROFIT","净利润增长率":"NETPROFIT",
                       "扣除非经常性损益后的净利润同比增长率":"DEDUCT_PARENT_NETPROFIT",
                       "营业收入增长率":"OPERATE_INCOME",
                       "总资产增长率":"TOTAL_ASSETS",
                       "固定资产增长率":"AVG_FIXED_ASSET",}

        for new_col,c_col in same_config.items():
            cur_dict_data[new_col] = round((cur_dict_data.get(c_col, 0) - before_dict_data.get(c_col)) / before_dict_data.get(c_col, 1), 4)
    def convert_quarter_data(self, data: pd.DataFrame, time_name, code_name, value_name, code_name_mapping=None,
                             handle_year_month_fn=None):
        year_dict_data = {}
        for index in data.index:
            ele = dict(data.loc[index])
            time = ele[time_name]
            if code_name not in ele.keys():
                code = code_name
            else:
                code = ele[code_name]
            if code_name_mapping is not None:
                code = code_name_mapping.get(code)
            val = ele[value_name]
            year = time[0:4]
            quarter = time[5:]
            quarter_mapping = {"03-31": 0, "06-30": 1, "09-30": 2, "12-31": 3}
            if handle_year_month_fn is not None:
                year, quarter = handle_year_month_fn(time)
            combine_key = f"{year}年{code}"
            if combine_key not in year_dict_data.keys():
                year_dict_data[combine_key] = [None] * 4
            year_dict_data[combine_key][quarter_mapping.get(quarter)] = val
        convert_data = pd.DataFrame(data=year_dict_data,
                                    index=['1季度', '2季度', '3季度', '4季度'])
        return convert_data

    def common_cal_cur_data(self):
        """
        计算当期数据,周期分析有用
        :return:
        """

        def cur_period_data(row, key):
            pre_val = row[key + "_PRE"]
            val = row[key]
            date = row['date']
            if "03-31" in date:
                return val
            else:
                return val - pre_val

        if self.cal_cur_cols is not None:
            for col in self.cal_cur_cols:
                self.fin_data[col + "_PRE"] = self.fin_data[col].shift(1)
                self.fin_data['CUR_' + col] = self.fin_data.apply(cur_period_data, args=(col,),
                                                                  axis=1)

    def analysis_flow_cash_data(self):
        cash_flow_jude_stock_type_mapping = {
            "111": "财源滚滚，但需仔细分析，投资以及筹资都为正",
            "110": "企业赚的钱不够还债，投资活动获得流入，看看啥情况",
            "101": "企业营业收入和筹资用于投资，看着不错，看看投资效果如何",
            "100": "企业营业的钱用于投资和还债，看着不错，看看能不能持续下去",
            "011": "营业不赚钱，靠投资和借钱过日子，得细致观察投资业务",
            "001": "借钱经营和投资，应该是个初创企业，看看未来是否可期",
            "010": "营业不赚钱，投资赚钱，并且还还债，得细致看看是不是变卖资产还债",
            "000": "都是亏钱的，可能不要碰了",
        }

        def judge_fn(row, mapping: dict):
            key = "1" if row['NETCASH_OPERATE'] > 0 else "0"
            key += "1" if row['NETCASH_INVEST'] > 0 else "0"
            key += "1" if row['NETCASH_FINANCE'] > 0 else "0"
            return mapping.get(key, 'default')

        # 现金流量判断数据
        self.fin_data['cash_flow_main_result'] = self.fin_data.apply(judge_fn,
                                                                     args=(cash_flow_jude_stock_type_mapping,), axis=1)

    def analysis_fin_profit_data(self):
        """
        1.2.1 利润来源
                营业收入-营业总成本+其他经营收益 = 营业利润
                利润总额 = 营业利润+营业外收入-营业外支出
                扣除非经常性损益后的净利润
            1.2.2 毛利率
                 毛利率=（营业收入-营业成本)/营业收入
            1.2.3 从费用率看企业的管理水平
                费用占比 =（销售费用+管理费用+财务费用）/ 营业收入
                研发费用 占比过高，存在不确定，研发产品成功，好事，如果研发产品失败，赔了夫人又折兵
        :return:
        """
        get_col_dict = {"FE_INTEREST_EXPENSE": "其中:利息费用",
                        "FE_INTEREST_INCOME": "利息收入",
                        "OPERATE_INCOME": "营业收入",
                        "OPERATE_COST": "营业成本",
                        'NETPROFIT': '净利润',
                        'TOTAL_PROFIT': '利润总额',
                        'OPERATE_TAX_ADD': '税金及附加',
                        'OPERATE_PROFIT': '营业利润',
                        'RESEARCH_EXPENSE': '研发费用',
                        'TOTAL_OPERATE_COST': "营业总成本",
                        'DEDUCT_PARENT_NETPROFIT': "扣除非经常性损益后的净利润",
                        'PARENT_NETPROFIT': "归属于母公司股东的净利润",
                        'SALE_EXPENSE': "销售费用",
                        'MANAGE_EXPENSE': "管理费用",
                        'FINANCE_EXPENSE': "财务费用",
                        'TOTAL_OPERATE_INCOME': '营业总收入'
                        }
        self.fin_data['other_operate_income'] = -self.fin_data['TOTAL_OPERATE_INCOME'] + self.fin_data[
            'TOTAL_OPERATE_COST'] + self.fin_data['OPERATE_PROFIT']
        self.fin_data['other_no_op_income'] = self.fin_data['TOTAL_PROFIT'] - self.fin_data['OPERATE_PROFIT']
        self.fin_data['其他营业收入占比净利润'] = round(
            self.fin_data['other_operate_income'] / self.fin_data['NETPROFIT'], 4)
        self.fin_data['除营业外收入占比净利润'] = round(
            self.fin_data['other_no_op_income'] / self.fin_data['NETPROFIT'], 4)
        self.fin_data['扣除非经常性损益后的净利润占比净利润'] = round(
            self.fin_data['DEDUCT_PARENT_NETPROFIT'] / self.fin_data['NETPROFIT'], 4)
        self.fin_data['毛利率'] = round(
            (self.fin_data['OPERATE_INCOME'] - self.fin_data['OPERATE_COST']) / self.fin_data['OPERATE_INCOME'], 4)
        self.fin_data['费用占比'] = round(
            (self.fin_data['SALE_EXPENSE'] + self.fin_data['MANAGE_EXPENSE'] + self.fin_data['FINANCE_EXPENSE']) /
            self.fin_data['OPERATE_INCOME'], 4)
        self.fin_data['研发费用占比'] = round(self.fin_data['RESEARCH_EXPENSE'] / self.fin_data['OPERATE_INCOME'], 4)
        show_col_names = ['其他营业收入占比净利润', '除营业外收入占比净利润', '扣除非经常性损益后的净利润占比净利润',
                          '毛利率', '费用占比', '研发费用占比']
        last_data_dict = dict(self.fin_data.tail(1).iloc[0])
        new_dict = {k: last_data_dict.get(k, '') for k in show_col_names}
        print(new_dict)

        return self.fin_data

    def common_cal_result(self, data: pd.DataFrame, change_config: dict, is_year_end=False, analysis_type='zcfz',
                          def_fn=None):
        record_year_data = {}
        res_data = []
        for index in data.index:
            dict_data = dict(data.loc[index])
            if is_year_end:
                if '12-31' in dict_data.get('date'):
                    record_year_data[dict_data['date']] = dict_data
                before_year = str(int(dict_data['date'][0:4]) - 1) + "-12-31"
            else:
                record_year_data[dict_data['date']] = dict_data
                before_year = str(int(dict_data['date'][0:4]) - 1) + dict_data['date'][4:]
            before_data = record_year_data.get(before_year, None)
            if before_data is not None:
                show_list = []
                for key, sub_dict in change_config.items():
                    key_diff = round((float(dict_data.get(key, 0)) - float(before_data.get(key, 0))) / 1e8, 3)
                    temp_sub_diff = {}
                    name = sub_dict['name']
                    for sub_key, sub_name in sub_dict['sub_key'].items():
                        temp_sub_diff[sub_name] = round(
                            (float(dict_data.get(sub_key, 0)) - float(before_data.get(sub_key, 0))) / 1e8, 3)
                    analysis_result = None
                    show_text = f"{name}"
                    if key_diff > 0:
                        show_text += f"增长{key_diff}亿;"
                        analysis_result = sort_dict_data_by(temp_sub_diff, by='value', reverse=True)
                    elif key_diff < 0:
                        show_text += f"减少{key_diff}亿;"
                        analysis_result = sort_dict_data_by(temp_sub_diff, by='value')
                    else:
                        print("么有分析结果")
                    if analysis_result is not None:
                        show_text += "主要原因是:"
                        for key, val in analysis_result.items():
                            if key_diff > 0:
                                if val > 0:
                                    show_text += f"{key}增长{val}亿;"
                            if key_diff < 0:
                                if val < 0:
                                    show_text += f"{key}减少{val}亿;"
                        show_list.append(show_text)
                if len(show_list) > 0:
                    dict_data[analysis_type] = "|".join(show_list)
                res_data.append(dict_data)
                if def_fn is not None:
                    def_fn(dict_data, before_data)
        return res_data

    def construct_summary(self, data: list, cols: list, d_type: str):
        if data is not None and len(data) > 0:
            date_str = data[-1].get("date")
            comm_summary = f"时间:{date_str};" + data[-1][d_type]
            detail_summary = ''
            for col in cols:
                val = data[-1].get(col, '')
                detail_summary += f"{col}:{val};"
            comm_summary += ";" + detail_summary
            return comm_summary
        return f"{d_type} has not summary"

    def analysis_cash_flow_data(self):

        profit_pd_data = self.fin_data
        change_config_dict = {

            "TOTAL_OPERATE_OUTFLOW": {"sub_key": {
                "BUY_SERVICES": "购买商品、接受劳务支付的现金",
                "PAY_STAFF_CASH": "支付给职工以及为职工支付的现金",
                "PAY_OTHER_OPERATE": "支付其他与经营活动有关的现金",
                "PAY_ALL_TAX": "支付的各项税费",
            }, "name": "经营活动现金流出小计"},

            "TOTAL_OPERATE_INFLOW": {"sub_key": {
                "SALES_SERVICES": "销售商品、提供劳务收到的现金",
                'RECEIVE_TAX_REFUND': '收到的税收返还',
                'RECEIVE_OTHER_OPERATE': '收到其他与经营活动有关的现金',
            }, "name": "经营活动现金流入小计"},

            "TOTAL_INVEST_INFLOW": {"sub_key": {
                "WITHDRAW_INVEST": "收回投资收到的现金",
                "RECEIVE_INVEST_INCOME": "取得投资收益收到的现金",
                "DISPOSAL_LONG_ASSET": "处置固定资产、无形资产和其他长期资产收回的现金净额",
                "RECEIVE_OTHER_INVEST": "收到的其他与投资活动有关的现金",
            }, "name": "投资活动现金流入小计"},

            "TOTAL_INVEST_OUTFLOW": {"sub_key": {
                "CONSTRUCT_LONG_ASSET": "购建固定资产、无形资产和其他长期资产支付的现金",
                "INVEST_PAY_CASH": "投资支付的现金",
                "PAY_OTHER_INVEST": "支付其他与投资活动有关的现金",
            }, "name": "投资活动现金流出小计"},

            "TOTAL_FINANCE_INFLOW": {"sub_key": {
                'ACCEPT_INVEST_CASH': '吸收投资收到的现金',
                'RECEIVE_LOAN_CASH': '取得借款收到的现金',
                'RECEIVE_OTHER_FINANCE': '收到的其他与筹资活动有关的现金',
            }, "name": "筹资活动现金流入小计"},

            "TOTAL_FINANCE_OUTFLOW": {"sub_key": {
                'PAY_DEBT_CASH': '偿还债务所支付的现金',
                'ASSIGN_DIVIDEND_PORFIT': '分配股利、利润或偿付利息支付的现金',
                'PAY_OTHER_FINANCE': '支付的其他与筹资活动有关的现金',
            }, "name": "筹资活动现金流出小计"},

        }

        def mid_fn(cur_dict_data: dict, before_dict_data: dict):
            cur_dict_data['经营活动产生的现金流量净额'] = str(
                round(cur_dict_data.get('NETCASH_OPERATE', 0) / 1e8, 4)) + "亿"
            cur_dict_data['投资活动产生的现金流量净额'] = str(
                round(cur_dict_data.get('NETCASH_INVEST', 0) / 1e8, 4)) + "亿"
            cur_dict_data['筹资活动产生的现金流量净额'] = str(
                round(cur_dict_data.get('NETCASH_FINANCE', 0) / 1e8, 4)) + "亿"

        data = self.common_cal_result(profit_pd_data, change_config_dict, analysis_type='cash_flow', def_fn=mid_fn)
        cols = ['经营活动产生的现金流量净额', '投资活动产生的现金流量净额', '筹资活动产生的现金流量净额']

        self.last_cash_flow_summary = self.construct_summary(data, cols, 'cash_flow')
        return self.last_cash_flow_summary

    def analysis_profit_data(self):
        profit_pd_data = self.fin_data
        profit_pd_data['OTHER_OPERATE_INCOME'] = profit_pd_data['TOTAL_OPERATE_COST'] + profit_pd_data[
            'OPERATE_PROFIT'] - \
                                                 profit_pd_data['TOTAL_OPERATE_INCOME']
        profit_pd_data['OTHER_NO_OPERATE_PROFIT'] = profit_pd_data['NETPROFIT'] - profit_pd_data[
            'DEDUCT_PARENT_NETPROFIT']
        change_config_dict = {
            "OPERATE_PROFIT": {"sub_key": {
                "TOTAL_OPERATE_INCOME": "营业总收入",
                'OTHER_OPERATE_INCOME': '其他经营收益',
                'INVEST_INCOME': '投资收益',
                'FAIRVALUE_CHANGE_INCOME': '加:公允价值变动收益',
                'ASSET_DISPOSAL_INCOME': '资产处置收益',
                'OTHER_INCOME': '其他收益',
            }, "name": "营业利润分析"},

            "NETPROFIT": {"sub_key": {
                "OTHER_NO_OPERATE_PROFIT": "非经常性利润",
                'DEDUCT_PARENT_NETPROFIT': '经常性利润',
            }, "name": "净利润分析"},

            "TOTAL_OPERATE_COST": {"sub_key": {
                'RESEARCH_EXPENSE': '研发费用',
                "OPERATE_COST": "营业成本",
                'SALE_EXPENSE': "销售费用",
                'OPERATE_TAX_ADD': '税金及附加',
                'MANAGE_EXPENSE': "管理费用",
                'FINANCE_EXPENSE': "财务费用",
                "FE_INTEREST_EXPENSE": "其中:利息费用",
                "FE_INTEREST_INCOME": "利息收入",
            }, "name": "营业总成本分析"},
        }

        def mid_fn(cur_dict_data: dict, before_dict_data: dict):
            cur_dict_data['营业收入同比增长率'] = round(
                (cur_dict_data['TOTAL_OPERATE_INCOME'] - before_dict_data['TOTAL_OPERATE_INCOME']) / before_dict_data[
                    'TOTAL_OPERATE_INCOME'], 4)
            cur_dict_data['净利润同比增长率'] = round(
                (cur_dict_data['NETPROFIT'] - before_dict_data['NETPROFIT']) / before_dict_data['NETPROFIT'], 4)
            cur_dict_data['扣除非经常性损益后的净利润同比增长率'] = round(
                (cur_dict_data['DEDUCT_PARENT_NETPROFIT'] - before_dict_data['DEDUCT_PARENT_NETPROFIT']) /
                before_dict_data[
                    'DEDUCT_PARENT_NETPROFIT'], 4)
            cur_dict_data['营业总成本同比增长率'] = round(
                (cur_dict_data['TOTAL_OPERATE_COST'] - before_dict_data['TOTAL_OPERATE_COST']) / before_dict_data[
                    'TOTAL_OPERATE_COST'], 4)
            cur_dict_data['销售费用同比增长率'] = round(
                (cur_dict_data['SALE_EXPENSE'] - before_dict_data['SALE_EXPENSE']) / before_dict_data['SALE_EXPENSE'],
                4)
            cur_dict_data['管理费用同比增长率'] = round(
                (cur_dict_data['MANAGE_EXPENSE'] - before_dict_data['MANAGE_EXPENSE']) / before_dict_data[
                    'MANAGE_EXPENSE'],
                4)
            cur_dict_data['财务费用同比增长率'] = round(
                (cur_dict_data['FINANCE_EXPENSE'] - before_dict_data['FINANCE_EXPENSE']) / before_dict_data[
                    'FINANCE_EXPENSE'], 4)
            cur_dict_data['研发费用同比增长率'] = round(
                (cur_dict_data['RESEARCH_EXPENSE'] - before_dict_data['RESEARCH_EXPENSE']) / before_dict_data[
                    'RESEARCH_EXPENSE'], 4)

        data = self.common_cal_result(profit_pd_data, change_config_dict, analysis_type='profit', def_fn=mid_fn)
        cols = ['营业收入同比增长率', '净利润同比增长率', '扣除非经常性损益后的净利润同比增长率',
                '营业总成本同比增长率', '销售费用同比增长率', '管理费用同比增长率', '财务费用同比增长率',
                '研发费用同比增长率']
        self.last_profit_summary = self.construct_summary(data, cols, 'profit')
        return self.last_profit_summary

    def analysis_fin_data_zcfz_data(self):
        change_config = {
            "TOTAL_ASSETS": {"sub_key": {
                "TOTAL_CURRENT_ASSETS": "流动资产合计",
                'TOTAL_NONCURRENT_ASSETS': '非流动资产合计',
            }, "name": "资产总计"},
            "TOTAL_CURRENT_ASSETS": {"sub_key": {
                "MONETARYFUNDS": "货币资金",
                "INVENTORY": "存货",
                'TRADE_FINASSET_NOTFVTPL': '交易性金融资产',
                "NOTE_ACCOUNTS_RECE": "应收票据及应收账款",
                "ACCOUNTS_RECE": "应收账款",
                "PREPAYMENT": "预付款项",
                'OTHER_CURRENT_ASSET': '其他流动资产',
            }, "name": "流动资产合计"},
            "TOTAL_NONCURRENT_ASSETS": {"sub_key": {
                'LONG_RECE': '长期应收款',
                'LONG_EQUITY_INVEST': '长期股权投资',
                'OTHER_NONCURRENT_FINASSET': '其他非流动金融资产',
                "FIXED_ASSET": "固定资产",
                "CIP": "在建工程",
                "INTANGIBLE_ASSET": "无形资产",
                'GOODWILL': '商誉',
                'LONG_PREPAID_EXPENSE': '长期待摊费用',
                'DEFER_TAX_ASSET': '递延所得税资产',
                'OTHER_NONCURRENT_ASSET': '其他非流动资产',
                'DEVELOP_EXPENSE': '开发支出',
            }, "name": "非流动资产合计"},

            "TOTAL_LIABILITIES": {"sub_key": {
                "TOTAL_CURRENT_LIAB": "流动负债合计",
                'TOTAL_NONCURRENT_LIAB': '非流动负债合计',
            }, "name": "负债合计"},

            "TOTAL_CURRENT_LIAB": {
                "sub_key": {
                    "SHORT_LOAN": "短期借款",
                    'NOTE_PAYABLE': "其中:应付票据",
                    "NOTE_ACCOUNTS_PAYABLE": "应付票据及应付账款",
                    "ACCOUNTS_PAYABLE": "其中:应付账款",
                    'CONTRACT_LIAB': '合同负债',
                    'ADVANCE_RECEIVABLES': '预收款项',
                    'STAFF_SALARY_PAYABLE': '应付职工薪酬',
                    'TOTAL_OTHER_PAYABLE': '其他应付款合计',
                    'NONCURRENT_LIAB_1YEAR': '一年内到期的非流动负债',
                    'OTHER_CURRENT_LIAB': '其他流动负债',
                    'TAX_PAYABLE': '应交税费',
                    'CURRENT_LIAB_OTHER': '流动负债其他项目',
                    'SHORT_BOND_PAYABLE': '应付短期债券',
                },
                "name": "流动负债合计"
            },
            "TOTAL_NONCURRENT_LIAB": {
                "sub_key": {
                    "LONG_LOAN": "长期借款",
                    'LONG_PAYABLE': '长期应付款',
                    'DEFER_INCOME': '递延收益',
                    'LONG_STAFFSALARY_PAYABLE': '长期应付职工薪酬',
                },
                "name": "非流动负债合计"
            },

        }
        zcfz_data = self.fin_data

        def mid_fn(cur_dict_data: dict, before_dict_data: dict):
            current_rate = round(
                float(cur_dict_data.get('TOTAL_CURRENT_ASSETS', 0)) / float(cur_dict_data.get('TOTAL_ASSETS', 0)), 4)
            non_current_rate = round(
                float(cur_dict_data.get('TOTAL_NONCURRENT_ASSETS', 0)) / float(cur_dict_data.get('TOTAL_ASSETS', 0)), 4)
            liabilities_rate = round(
                float(cur_dict_data.get('TOTAL_LIABILITIES')) / float(cur_dict_data.get('TOTAL_ASSETS')),
                4)
            eq_rate = round(float(cur_dict_data.get('TOTAL_EQUITY', 0)) / float(cur_dict_data.get('TOTAL_ASSETS', 1)),
                            4)
            if float(cur_dict_data.get('TOTAL_CURRENT_LIAB', 1)) > 0:

                flow_rate = round(
                    float(cur_dict_data.get('TOTAL_CURRENT_ASSETS', 0)) / float(
                        cur_dict_data.get('TOTAL_CURRENT_LIAB', 1)),
                    4)
                dflow_rate = round(
                    (float(cur_dict_data.get('TOTAL_CURRENT_ASSETS', 0)) - float(
                        cur_dict_data.get('INVENTORY', 0))) / float(
                        cur_dict_data.get('TOTAL_CURRENT_LIAB', 0)), 4)
            else:
                flow_rate = 0
                dflow_rate = 0

            cur_dict_data['流动资产占比'] = current_rate
            cur_dict_data['非流动资产占比'] = non_current_rate
            cur_dict_data['负债率'] = liabilities_rate
            cur_dict_data['所有者权益率'] = eq_rate
            cur_dict_data['流动比率'] = flow_rate
            cur_dict_data['速动比率'] = dflow_rate

        datas = self.common_cal_result(zcfz_data, change_config, is_year_end=True, analysis_type='zcfz', def_fn=mid_fn)
        cols = ['流动资产占比', '非流动资产占比', '负债率',
                '所有者权益率', '流动比率', '速动比率']
        self.last_zcfz_summary = self.construct_summary(datas, cols, 'zcfz')
        return self.last_zcfz_summary

    def bar_chart(self,x_labels, y_dict_data: dict):

        bar = Bar(init_opts=opts.InitOpts(
            width='1700px', height='1000px'
        ))
        bar.add_xaxis(x_labels)
        bar.set_global_opts(
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-15)),
        )
        # bar.set_global_opts(
        #     datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
        # )
        bar.set_global_opts(
            xaxis_opts=opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=False)),
            yaxis_opts=opts.AxisOpts(
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
        for col_name, list_data in y_dict_data.items():
            bar.add_yaxis(col_name, list_data)
        return bar

    def table_chart(self,header, rows, page_title):
        table = Table()
        table.add(header, rows, {"max_width": "100px"})
        return table

    def df_to_table_chart(self,chart: list,df:pd.DataFrame,page_title):
        cols = list(df.columns)
        rows = []
        for index in df.index:
            row = []
            ele = df.loc[index]
            for col in cols:
                value = ele[col]
                if isinstance(value,float):
                    value = round(value,2)
                row.append(value)
            rows.append(row)
        tb = self.table_chart(cols,rows,page_title)
        chart.append(tb)


    def df_to_chart(self,cdf, chart: list, chart_type=None):
        cdf_cols = cdf.columns
        data_dict = {}
        for cdf_col in cdf_cols:
            data_dict[cdf_col] = [round(ele, 2) for ele in list(cdf[cdf_col].values)]
        if chart_type is None:
            bar_c = self.bar_chart(list(cdf.index), data_dict)
            chart.append(bar_c)

    def cal_last_score(self,quarter_df,c_type="gt"):
        before_num = 5
        last_result_dict_data = {}
        for index in quarter_df.index:
            dict_data = sort_dict_data_by(dict(quarter_df.loc[index]),reverse=True)
            for i in range(2):
                col = list(dict_data.keys())[i]
                if str(dict_data.get(col))!='nan':
                    val = dict_data.get(col)
                    start = i+1
                    end = start+before_num
                    type_count = 0
                    for ccol in list(dict_data.keys())[start:end]:
                        cval = dict_data.get(ccol,0)
                        if c_type=='gt':
                            if val>cval:
                                type_count += 1
                        else:
                            if val<cval:
                                type_count += 1
                    last_result_dict_data[col[0:4]+"_"+index] = {"value":round(dict_data.get(col),4),"cnt":type_count,"score":round(type_count/before_num,4),"total_count":before_num}
                    break
        return last_result_dict_data
    def cal_score(self,cols,df:pd.DataFrame,cols_weight=None,is_show=False,cols_st_type=None,self_value_score:dict=None,chars:list=None,is_google_model_analysis=False):
        num = len(cols)
        if cols_weight is None:
            cols_weight = {k: 1 / num for k in cols}
        if cols_st_type is None:
            cols_st_type = {}
        all_last_score_dict = {}
        for col in cols:
            cdf = self.convert_quarter_data(df, 'date', f'报告期{col}', col)
            last_score_dict = self.cal_last_score(cdf, c_type=cols_st_type.get(col, 'gt'))
            for q, vs in last_score_dict.items():
                all_last_score_dict.setdefault(q, 0)
                before_v = all_last_score_dict.get(q)
                if self_value_score is not None:
                    self_value_score.get(col)
                before_v += vs['score'] * cols_weight.get(col)
                all_last_score_dict[q] = before_v
            if chars is not None:
                cdf_cols = cdf.columns
                data_dict = {}
                for cdf_col in cdf_cols:
                    data_dict[cdf_col] = [round(ele,2) for ele in list(cdf[cdf_col].values)]
                bar_c = self.bar_chart(list(cdf.index),data_dict)
                chars.append(bar_c)
            if is_show:
                cdf.plot(kind='bar', title=col, rot=45, figsize=(15, 8), fontsize=10)
                plt.show()
            if is_google_model_analysis and chars is not None:
                cdf['季度'] = cdf.index
                self.google_model_analysis_trend(cdf)
                chars.append(
                    self.table_chart([f'报告期{col}趋势分析'], [[self.google_model_analysis_trend(cdf)]],
                                     ''))

        return all_last_score_dict

    def google_model_analysis_trend(self,df:pd.DataFrame):
        request_txt = "给定表格数，分析趋势,表格:"+handle_model_table_data(df)
        return try_get_action(simple_big_gen_model_fn,try_count=3,model=self.load_google_model(),request_txt=request_txt,is_ret_json=False)

    def get_current_price_market_info(self):
        if self.current_price_market_info is None:
            now_str = datetime.now().strftime("%Y-%m-%d")
            file_name = f"stock_current_price{now_str}.csv"
            if os.path.exists(file_name) is False:
                print("么有数据文件下载最新数据")
                df = ak.stock_zh_a_spot_em()
                df.to_csv(file_name,index=False)
            else:
                df = pd.read_csv(file_name,dtype={"代码":str})
            self.current_price_market_info = dict(df[df['代码'].isin([self.code])].head(1).iloc[0])
            if not self.current_price_market_info:
                print("么有数据重新下载数据")
                df = ak.stock_zh_a_spot_em()
                df.to_csv(file_name, index=False)
                self.current_price_market_info = dict(df[df['代码'].isin([self.code])].head(1).iloc[0])
        if self.current_price_market_info:
            self.current_price_market_info['number_of_shares'] = self.current_price_market_info['总市值']/self.current_price_market_info['最新价']
        return self.current_price_market_info

    def cal_opt_price(self):
        self.get_current_price_market_info()
        if self.current_price_market_info:
            last_dict_data = dict(self.fin_data.tail(1).iloc[0])
            recent_price = self.fin_data.tail(1)['有形账面净值'].values[0]/self.current_price_market_info['number_of_shares']
            print(recent_price)
            print(self.current_price_market_info)


    def generator_analysis(self,is_data_from_local=True,is_model_gen_res=False):
        charts = []

        industry_data = self.get_stock_industry()
        self.df_to_table_chart(charts,industry_data,'test')
        table_header = ['现金流量结论', '利润表结论', '资产负债表结论']
        self.get_stock_fin_data(is_local=is_data_from_local)
        flow_result = self.analysis_cash_flow_data()
        profit_result = self.analysis_profit_data()
        zcfz_result = self.analysis_fin_data_zcfz_data()
        rows = [[flow_result, profit_result, zcfz_result]]

        table = self.table_chart(table_header, rows, '财报最近概括')
        charts.append(table)
        df = self.fin_data

        format_data_to_100million(df, ['CUR_NETCASH_OPERATE'])
        cdf = self.convert_quarter_data(df, 'date', '经营活动产生的现金流量净额(亿)', 'CUR_NETCASH_OPERATE')
        self.df_to_chart(cdf, charts)
        if is_model_gen_res:
            cdf['季度'] = cdf.index
            charts.append(self.table_chart(['经营活动产生的现金流量净额趋势分析'],[[self.google_model_analysis_trend(cdf)]],''))



        format_data_to_100million(df, ['CUR_DEDUCT_PARENT_NETPROFIT'])
        cdf = self.convert_quarter_data(df, 'date', '当期报告净利润(亿)', 'CUR_DEDUCT_PARENT_NETPROFIT')
        self.df_to_chart(cdf, charts)
        if is_model_gen_res:
            cdf['季度'] = cdf.index
            charts.append(
                self.table_chart(['当期报告净利润趋势分析'], [[self.google_model_analysis_trend(cdf)]], ''))

        format_data_to_100million(df, ['DEDUCT_PARENT_NETPROFIT'])
        cdf = self.convert_quarter_data(df, 'date', '报告期净利润', 'DEDUCT_PARENT_NETPROFIT')
        self.df_to_chart(cdf, charts)
        if is_model_gen_res:
            cdf['季度'] = cdf.index
            charts.append(
                self.table_chart(['报告期净利润额趋势分析'], [[self.google_model_analysis_trend(cdf)]], ''))

        format_data_to_100million(df, ['CIP'])
        cdf = self.convert_quarter_data(df, 'date', '报告期在建工程', 'CIP')
        self.df_to_chart(cdf, charts)
        if is_model_gen_res:
            cdf['季度'] = cdf.index
            charts.append(
                self.table_chart(['报告期在建工程趋势分析'], [[self.google_model_analysis_trend(cdf)]], ''))

        format_data_to_100million(df, ['RESEARCH_EXPENSE'])
        cdf = self.convert_quarter_data(df, 'date', '报告期研发费用', 'RESEARCH_EXPENSE')
        self.df_to_chart(cdf, charts)
        # 偿债能力指标分析
        score_cols = []
        cols = ['流动比率', '速动比率', '现金比率', '现金流动负债比率', '资产负债率', '产权比率', '有形净值债务率',
                '权益乘数']
        cols_st_type = {k: "gt" for k in cols}
        cols_st_type['资产负债率'] = 'lt'
        all_score = self.cal_score(cols, df, cols_st_type=cols_st_type, chars=charts,is_google_model_analysis=is_model_gen_res)
        print("偿债能力分数", all_score)

        rows = []
        row = ['偿债能力分数']
        for e in all_score.keys():
            score_cols.append(e)
            row.append(round(all_score.get(e), 2))
        rows.append(row)

        # 运营能力指标的计算
        cols = ['应收账款周转率', '存货周转率', '流动资产周转率', '固定资产周转率', '总资产周转率']
        cols_st_type = {k: "lt" for k in cols}
        all_score = self.cal_score(cols, df, cols_st_type=cols_st_type, chars=charts,is_google_model_analysis=is_model_gen_res)
        print("运营能力分数", all_score)
        row = ['运营能力分数']
        for e in score_cols:
            row.append(round(all_score.get(e), 2))
        rows.append(row)

        # 盈利能力分析
        cols = ['毛利率', '营业利润率', '销售净利率', '净资产收益率', '资本报酬率']
        cols_weight = {'毛利率': 0.2, '营业利润率': 0.2, '销售净利率': 0.2, '净资产收益率': 0.2, '资本报酬率': 0.2}
        all_score = self.cal_score(cols, df, cols_weight=cols_weight, chars=charts,is_google_model_analysis=is_model_gen_res)
        print("盈利能力分数", all_score)
        row = ['盈利能力分数']
        for e in score_cols:
            row.append(round(all_score.get(e), 2))
        rows.append(row)

        # 发展能力
        cols_dict = {"资本积累率": "TOTAL_EQUITY", "营业利润增长率": "OPERATE_PROFIT", "净利润增长率": "NETPROFIT",
                     "扣除非经常性损益后的净利润同比增长率": "DEDUCT_PARENT_NETPROFIT",
                     "营业收入增长率": "OPERATE_INCOME",
                     "总资产增长率": "TOTAL_ASSETS",
                     "固定资产增长率": "AVG_FIXED_ASSET"}
        cols = list(cols_dict.keys())
        all_score = self.cal_score(cols, df, chars=charts,is_google_model_analysis=is_model_gen_res)
        print("发展能力分数", all_score)
        row = ['发展能力分数']
        for e in score_cols:
            row.append(round(all_score.get(e), 2))
        rows.append(row)

        table_header = ['分数名称'] + score_cols

        score_tb = self.table_chart(table_header, rows, '')

        page = Page()
        page.add(score_tb)
        for char in charts:
            page.add(char)
        page.render(f"{self.name}.html")



if __name__ == '__main__':
    enter_big_model_analysis_stock_indicator()
