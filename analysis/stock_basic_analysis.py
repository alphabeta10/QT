"""
股票基本分析，波动率，风险，等
"""
from datetime import datetime, timedelta
from big_models.google_api import *
from analysis.analysis_tool import *
import pandas as pd
import matplotlib.pyplot as plt
from utils.tool import load_json_data,comm_read_stock
import google.generativeai as genai
from utils.actions import try_get_action
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data

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
    if json_data is not None and ('综合分析' not in json_data.keys() or '投资分类' not in json_data.keys()):
        json_data = try_get_action(google_big_gen_model_comm_fn, try_count=3, data_df=result_df, model=model,
                                   request_txt=request_txt)
    return json_data


def enter_big_model_analysis_stock_indicator(code_dict: dict = None):
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    genai.configure(api_key=api_key, transport='rest')
    model = genai.GenerativeModel('gemini-1.0-pro-latest')
    year = datetime.now().strftime('%Y-01-01')
    if code_dict is None:
        code_dict = comm_read_stock('../stock.txt')
    update_request = []
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    for code, name in code_dict.items():
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
                            "time": year, "code": code,"invent_state":invent_state}

                update_request.append(
                    UpdateOne({"code": code, 'time': new_dict['time'], "data_type": new_dict['data_type']},
                              {"$set": new_dict},
                              upsert=True)
                )
            else:
                print(f"返回 json 有问题 {ret_json}")
            if len(update_request)>4:
                mongo_bulk_write_data(big_model_col, update_request)
                update_request.clear()
    if len(update_request) > 0:
        mongo_bulk_write_data(big_model_col, update_request)
        update_request.clear()

if __name__ == '__main__':
    enter_big_model_analysis_stock_indicator()
