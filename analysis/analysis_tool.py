import statsmodels.api as sm
from statsmodels import regression
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')
from utils.tool import get_data_from_mongo
from utils.actions import show_data
import pandas as pd
from sklearn.linear_model import LinearRegression
from scipy.optimize import minimize


def plot_marker_line(data: pd.DataFrame, value_index, show_index, title, x_label=''):
    data.plot(kind='line', rot=45, figsize=(15, 8), fontsize=10, marker='o')
    for i in range(len(data)):
        for col in data.columns:
            plt.text(data.index[i], data[col].values[i], (data[col].values[i]), ha='center', va='bottom', fontsize=10)
    plt.xticks(value_index, show_index)
    ax = plt.gca()
    ax.spines['right'].set_color("none")
    ax.spines['top'].set_color("none")
    plt.title(title, fontsize=15)
    plt.xlabel(x_label)
    plt.show()


def generate_month_and_day():
    dates = pd.date_range('2023-01-01', '2023-12-31')
    dates = {str(ele)[5:10]: 0 for ele in dates.values}
    return dates


def plot_year_seq_data(pd_data: pd.DataFrame, index_key='date', val_key='close', title='月分析数据'):
    year_dict_data = {}
    for index in pd_data.index:
        ele = dict(pd_data.loc[index])
        index_value = ele[index_key]
        year = index_value[0:4]
        if "-" in index_value:
            month = index_value[5:10]
        else:
            month = index_value[4:6] + "-" + index_value[6:8]
        if year not in year_dict_data.keys():
            year_dict_data[year] = generate_month_and_day()
        if month in year_dict_data[year].keys():
            year_dict_data[year][month] = ele[val_key]
    convert_pd_df_dict = {}
    for year, combin_dict in year_dict_data.items():
        convert_pd_df_dict[year] = list(combin_dict.values())
    new_month_day_pd = pd.DataFrame(data=convert_pd_df_dict, index=list(generate_month_and_day().keys()))
    new_month_day_pd.plot(kind='line', title=title, rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


def calc_regress_deg(y_arr, show=True):
    """
    计算y_arr序列的趋势
    :param y_arr:
    :param show:
    :return:
    """
    x = np.arange(0, len(y_arr))
    # 将y_arr缩放到x级别
    zoom_factor = x.mean() / y_arr.max()
    y_arr = zoom_factor * y_arr

    x = sm.add_constant(x)
    model = regression.linear_model.OLS(y_arr, x).fit()

    rad = model.params[1]
    deg = np.rad2deg(rad)
    if show:
        intercept = model.params[0]
        reg_y_fit = x * rad + intercept
        plt.plot(x, y_arr)
        plt.plot(x, reg_y_fit)
        plt.title("deg=" + str(deg))
        plt.show()
    return deg


def ann_ret(x, Freq=52):
    return (x + 1) ** Freq - 1


def ann_std(x, Freq=52):
    return x * np.sqrt(Freq)


def ann_geo_mean(x, Freq=52):
    n = len(x)
    return np.exp(np.sum(np.log(1 + x)) * Freq / n) - 1


def ann_sr(x, rf, Freq=52):
    n = len(x)
    ret_expected = np.sum(x - rf) / n
    ret_avg = np.sum(x) / n
    std_dev = np.sqrt(np.sum((x - ret_avg) ** 2) / n)
    annu_ret_expected = (ret_expected + 1) ** Freq - 1
    ann_std_dev = std_dev * np.sqrt(Freq)
    return annu_ret_expected / ann_std_dev


def mdd(x):
    wealth = (x + 1).cumprod()
    cummax = wealth.cummax()
    drawdown = wealth / cummax - 1
    return drawdown.min()


def get_market_data(condition=None):
    if condition is None:
        condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2023-01-01"}}
    # stock_zh_index_daily_df = ak.stock_zh_index_daily(symbol="sh000001")[['date','close']]
    stock_zh_index_daily_df = get_data_from_mongo(database="stock", collection='index_data',
                                                  condition=condition,
                                                  projection={'_id': False, "close": True, "date": True},
                                                  sort_key='date')
    stock_zh_index_daily_df.set_index(keys='date', inplace=True)
    stock_zh_index_daily_df = stock_zh_index_daily_df.pct_change(1)
    stock_zh_index_daily_df.rename(columns={"close": "sh000001"}, inplace=True)
    stock_zh_index_daily_df.index = pd.to_datetime(stock_zh_index_daily_df.index)
    return stock_zh_index_daily_df


def LR(X, y):
    reg = LinearRegression().fit(X.reshape(-1, 1), y.reshape(-1, 1))
    return reg.coef_, reg.intercept_


# function to compute the expected return for the portfolio
def ereturn(w, mean_return):
    ereturn = w @ mean_return
    return ereturn


# function to compute the portfolio standard deviation
def pvol(w, ret_cov_mat):
    pvar = w @ ret_cov_mat @ w
    return np.sqrt(pvar)


def SR(w, train_data, rf):
    excess_ret = train_data @ w - rf
    train_data_cov = train_data.cov()
    SR = (excess_ret.mean()) / pvol(w, train_data_cov)
    return -SR


# function to find the optimal portfolio that maximize the Sharpe ratio
def MaxSR(data, rf, silent=False):
    n = len(data.columns)

    bonds = tuple((0, 1) for i in range(n))

    def constraint1(w):
        return np.sum(w) - 1.0

    cons = {"type": 'eq', "fun": constraint1}

    w0 = np.array(np.ones(n))
    sol = minimize(SR, w0, args=(data, rf), method="SLSQP", bounds=bonds, constraints=cons)
    if not silent:
        print("Solution to the Max Sharpe Ratio Problem is :")
        print(sol)
        print("")
    elif not sol['success']:
        print("WARNING: the optimizer did NOT exit successfully!!")

    return sol


def stock_potfolio():
    codes = ['601168', '600132', '002709']
    # handle_stock_daily_data(codes=codes,start_date='20100101')
    data = get_data_from_mongo(database="stock", collection='ticker_daily',
                               condition={"code": {"$in": codes}, "time": {"$gte": "2010-01-01"}},
                               projection={'_id': False, "code": True, "close": True, "time": True})
    data = pd.pivot_table(data, values='close', index=['time'], columns=['code'])
    data.index = pd.to_datetime(data.index)
    ret_daily = data.pct_change(1)
    ret_daily = ret_daily.dropna()
    for name in ret_daily.columns:
        print(name)
    print(ret_daily.describe().T)
    sh001 = get_market_data()
    sh001 = (sh001 + 1).resample('W').prod() - 1
    sh001 = sh001.drop(sh001.index[[0]])
    sh001 = sh001.drop(sh001.index[[-1]])
    print(sh001)

    df_week = (ret_daily + 1).resample('W').prod() - 1
    df_week = df_week.drop(df_week.index[[0]])
    df_week = df_week.drop(df_week.index[[-1]])
    data = df_week

    start_date = '2020-12-19'
    end_date = '2023-06-21'
    ind = (data.index >= start_date) * (data.index <= end_date)
    data = data[ind]
    ind = (sh001.index >= start_date) * (sh001.index <= end_date)
    sh001 = sh001[ind]
    n_dec = 2
    SumStat = pd.DataFrame(index=codes)
    SumStat['Geo Mean(Annu,%)'] = np.round(data.apply(ann_geo_mean) * 100, n_dec)
    SumStat['Volatility(Annu,%)'] = np.round(ann_std(data.std()) * 100, n_dec)
    SumStat['Sharpe Ratio (Annu)'] = np.round(data.apply(ann_sr, rf=0.0025), n_dec)
    SumStat['Max Drawdown(%)'] = np.round(data.apply(mdd) * 100, n_dec)
    show_data(SumStat)

    n_dec = 3

    mkt_ex_ret = sh001 - 0.00025
    ex_ret = data - 0.00025

    print(mkt_ex_ret)
    print("*" * 50)
    print(ex_ret)

    n = len(ex_ret.columns)
    beta = np.zeros(n)
    alpha = np.zeros(n)
    for i in range(n):
        beta[i], alpha[i] = LR(mkt_ex_ret.values, ex_ret[ex_ret.columns[i]].values)
    AlphaBeta = pd.DataFrame(index=data.columns)
    AlphaBeta['Alpha(Annu,%)'] = np.round(ann_ret(alpha) * 100, n_dec)
    AlphaBeta['Beta'] = np.round(beta, n_dec)
    print(AlphaBeta)

    # data_cov_mat = data.cov()
    # data_cov_mat_annu = data_cov_mat * Freq

    # 4 Estimating parameters of Markowitz Model

    data_train_mean = data.mean()
    data_train_cov_mat = data.cov()

    def EReturn(w):
        EReturn = w @ data_train_mean
        return EReturn

    def PVol(w):
        pvar = w @ data_train_cov_mat @ w
        return np.sqrt(pvar)

    def MarkPortOpt(r, silent=False):

        def constraint1(w):
            return np.sum(w) - 1.0

        def constraint2(w):
            return 1.0 - np.sum(w)

        def constraint3(w):
            return w

        def constraint4(w):
            diff = EReturn(w) - r
            return diff

        con1 = {'type': 'ineq', 'fun': constraint1}
        con2 = {'type': 'ineq', 'fun': constraint2}
        con3 = {'type': 'ineq', 'fun': constraint3}
        con4 = {'type': 'ineq', 'fun': constraint4}
        cons = ([con1, con2, con3, con4])

        w0 = np.ones(len(data.columns))
        sol = minimize(PVol, w0, method="SLSQP", constraints=cons)
        if (not silent):
            print("Solution to the Markowitz Problem with r= ", round(r * 100, 3), "%:")
            print(sol)
            print("")
        elif (not sol['success']):
            print("WARNING: the optimizer did not exit successfully!!")
            print(sol)
        return sol

    sol = MarkPortOpt(r=0.001, silent=False)
    numR = 14

    r_bar = np.array([i for i in range(numR - 3)]) * 0.0003 + 0.002
    r_bar = np.append(r_bar, np.array([0.00505, 0.0051, 0.00515]))

    if len(r_bar) != numR:
        numR = len(r_bar)
    print("Number of targeted returns (or r-bar) specified is: ", numR)

    port_vol = []
    port_return = []

    alloc_r = np.zeros((len(data.columns), numR))

    for i in range(numR):
        r = r_bar[i]
        print("* For the case r-bar = ", round(r * 100, 3), "%:")

        sol = MarkPortOpt(r, silent=True)

        if (not sol['success']):  # check if the optimizer exit successfully
            print("NOTE: solution to this r-bar will be dropped!")
        else:  # only keeping the r-bar that has sucessful optmization
            print(sol['message'])
            alloc_r[:, i] = sol['x']
            port_vol.append(sol['fun'])
            port_return.append(EReturn(sol['x']))
        print("")

    port_vol = np.asarray(port_vol)
    port_return = np.asarray(port_return)

    num_rbar = len(port_vol)  # update the number of r-bar recorded/kept
    print("The number of recoreded the efficient frontier points is:", num_rbar)

    DF_Alloc_R = pd.DataFrame(alloc_r)
    DF_Alloc_R.index = data.columns
    DF_Alloc_R.columns = [str(round(ann_ret(r) * 100, 1)) + "%" for r in r_bar]
    DF_Alloc_R = DF_Alloc_R.loc[:,
                 (DF_Alloc_R != 0).any(axis=0)]  # drop the r-bar solution(s) that failed the opt. problem

    print('Optimal allocation (in %) for specified (annualized) target return:')
    print(np.round(DF_Alloc_R * 100, 1))  # allocation in % and round (to the 1st decimal)

    # Drawing Full Efficient Frontier

    print(ann_std(port_vol))
    print(ann_ret(port_return))
    plt.rcParams['figure.figsize'] = (11, 6)
    plt.plot(ann_std(port_vol), ann_ret(port_return), 'ro--', label='efficient_frontier')
    plt.xlabel("Annualized Volatility")
    plt.ylabel("Annualized Return")

    plt.grid(True, linestyle='--')
    plt.legend()

    plt.title('Efficient Frontier (' + str(start_date) + ' to ' + str(end_date) + ')')
    plt.gca().yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.gca().xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    plt.show()

    def AnnSR(w, data=data, rf=0.0025):
        excess_ret = data @ w - rf
        annSR = ann_ret(excess_ret.mean()) / ann_std(PVol(w))

    def MaxSR(data=data, rf=0.0025, silent=False):
        def SR(w):
            excess_ret = data @ w - rf
            SR = (excess_ret.mean()) / (PVol(w))
            return -SR

        n = len(data.columns)

        bonds = tuple((0, 1) for i in range(n))

        def constraint1(w):
            return np.sum(w) - 1.0

        cons = {"type": "eq", "fun": constraint1}
        w0 = np.array(np.ones(n))

        sol = minimize(SR, w0, method="SLSQP", bounds=bonds, constraints=cons)

        if (not silent):
            print("Solution to the Max Sharpe Ratio Problem is:")
            print(sol)
            print("")
        elif (not sol['success']):  # check if the optimizer exist successfully
            print("WARNING:  the optimizer did NOT exit successfully!!")

        return sol

    sol = MaxSR()
    w_SR = sol['x']  # the portfolio weight with highest Sharpe Ratio

    # Calculate the volatility and expected return for the optimal portfolio
    opt_vol = PVol(sol['x'])
    opt_return = EReturn(sol['x'])
    sr = AnnSR(sol['x'])

    # Print (annualized) return, volatiltiy and Sharpe ratio information
    print("* The expected return (annualized) for the optimal portfolio is ", ann_ret(opt_return))
    print("* The volatility (annualized) for the optimal portfolio is ", ann_std(opt_vol))
    print("* The Sharpe ratio (annualized) for the optimal portfolio is ", sr)
    print()

    # Display the optimal allocation after adding in the allocation correspond to the max SR
    DF_Alloc_R['maxSR'] = w_SR
    print('Optimal allocation (in %) for specified target return:')
    print(np.round(DF_Alloc_R * 100, 1))  # allocation in % and round (to the 1st decimal)


if __name__ == '__main__':
    pass
