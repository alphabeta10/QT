import talib as ta
from utils.tool import get_data_from_mongo
from utils.actions import show_data
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def common_indictator_cal(data: pd.DataFrame, *args, **kwargs):
    mfi_timeperiod = 14
    if 'mfi_timeperiod' in kwargs.keys():
        mfi_timeperiod = kwargs['mfi_timeperiod']
    data['mfi'] = ta.MFI(data.high, data.low, data.close, data.volume, timeperiod=mfi_timeperiod)
    data['1年均线'] = ta.SMA(data.close, timeperiod=240)
    data['半年均线'] = ta.SMA(data.close, timeperiod=120)
    if 'ma_timeperiod' in kwargs.keys():
        ma_timeperiod = kwargs['ma_timeperiod']
        data[f'自定义{ma_timeperiod}日均线'] = ta.SMA(data.close, timeperiod=ma_timeperiod)
    macd, macdsignal, macdhist = ta.MACD(data.close, fastperiod=12, slowperiod=26, signalperiod=9)
    data['macd'] = macdhist
    data['rsi12'] = ta.RSI(data.close, timeperiod=12)
    data['rsi6'] = ta.RSI(data.close, timeperiod=6)
    data['K'], data['D'] = ta.STOCH(data.high, data.low, data.close, fastk_period=9, slowk_period=5, slowk_matype=1,
                                    slowd_period=5, slowd_matype=1)
    addSignal(data,sign_type='mfi')
    addSignal(data,sign_type='KDJ')
    return data


def addSignal(data: pd.DataFrame, sign_type=None):

    if sign_type is None:
        return
    if sign_type=='mfi':
        data['pre_mfi'] = data['mfi'].shift(1)
        data['buy_mfi'] = data.apply(lambda row:1 if row['pre_mfi']<20 and row['mfi']>20 else 0,axis=1)
        data['sell_mfi'] = data.apply(lambda row:1 if row['pre_mfi']>80 and row['mfi']<80 else 0,axis=1)
    if sign_type=='KDJ':
        data['pre_K'] = data['K'].shift(1)
        data['pre_D'] = data['D'].shift(1)
        data['buy_kdj'] = data.apply(lambda row:1 if row['D']<20 and row['K']<20 and row['K']>row['D'] and row['pre_K']<row['pre_D'] else 0,axis=1)
        data['sell_kdj'] = data.apply(lambda row:1 if row['D']>80 and row['K']>80 and row['K']<row['D'] and row['pre_K']>row['pre_D'] else 0,axis=1)


def MFI_indicator():
    """stock"""
    condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2020-01-01"}}
    condition = {"code": {"$in": ["000858"]}, "time": {"$gte": "2020-01-01"}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False}
    sort_key = "time"

    """index"""
    condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2019-01-01"}}
    database = 'stock'
    collection = 'index_data'
    projection = {'_id': False}
    sort_key = "date"
    """futures"""
    # condition = {"symbol": {"$in": ["B0"]}, "date": {"$gte": "2020-01-01"}}
    # database = 'futures'
    # collection = 'futures_daily'
    # projection = {'_id': False}
    # sort_key = "date"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.index = pd.to_datetime(data[sort_key])
    data.sort_index(inplace=True)
    data['ret'] = data['close'].pct_change(1)
    data.dropna(inplace=True)
    """
    TYP:=(HIGH+LOW+CLOSE)/3;
    V1:=SUM(IF(TYP＞REF(TYP,1),TYP*VOL,0),N)/SUM(IF(TYP＜REF(TYP,1),TYP*VOL,0),N)
    MFI:100-(100/(1+V1));
    """

    timeperiod = 20
    data['mfi'] = ta.MFI(data.high, data.low, data.close, data.volume, timeperiod=timeperiod)
    data['rsi12'] = ta.RSI(data.close, timeperiod=12)
    data['rsi6'] = ta.RSI(data.close, timeperiod=6)

    data['K'], data['D'] = ta.STOCH(data.high, data.low, data.close, fastk_period=9, slowk_period=5, slowk_matype=1,
                                    slowd_period=5, slowd_matype=1)

    """ADX 低于25时，震荡行情，不明确的行情 大于25是，趋势强，但不能判断上涨还是下降 要结合+DI和-DI以及OBV指标
        信号如下判断
        1.先判断ADX大于25
        2.判断+DM和-DM 
        3.判断obv 大于 avg100obv
        4.依据上面三个判断是否买入
    """
    data['ADX'] = ta.ADX(data.high,data.low,data.close,timeperiod=14)
    data['minus_di'] = ta.MINUS_DI(data.high,data.low,data.close,timeperiod=14)
    data['plus_di'] = ta.PLUS_DI(data.high,data.low,data.close,timeperiod=14)
    data['obv'] = ta.OBV(data.close,data.volume)
    data['avg100obv'] = ta.SMA(data.obv,timeperiod=100)

    # plt.figure(figsize=(16, 14))
    # plt.subplot(211)
    # data['close'].plot(color='r')
    # plt.xlabel("")
    # plt.title("上证走势", fontsize=15)

    fig, axes = plt.subplots(3, 1)
    data[['close']].loc['2022-01-01':].plot(ax=axes[0], grid=True, title='上证走势')
    data[['avg100obv','obv']].loc['2022-01-01':].plot(ax=axes[1], grid=True)
    data[['minus_di','plus_di','ADX']].loc['2022-01-01':].plot(ax=axes[2], grid=True)
    plt.legend(loc='best', shadow=True)

    # plt.subplot(212)
    # data['mfi'].plot()
    # plt.title('MFI指标', fontsize=15)
    # plt.xlabel('')
    plt.show()

    for i in range(15, len(data)):
        # if data['mfi'][i] > 20 and data['mfi'][i - 1] < 20:
        #     data.loc[data.index[i], '收盘信号'] = 1
        # if data['mfi'][i] < 80 and data['mfi'][i - 1] > 80:
        #     data.loc[data.index[i], '收盘信号'] = 0

        # if data['rsi6'][i] < 45 and data['rsi6'][i - 1] < data['rsi12'][i - 1] and data['rsi6'][i] > data['rsi12'][i]:
        #     data.loc[data.index[i], '收盘信号'] = 1
        # if data['rsi6'][i] > 65 and data['rsi6'][i - 1] > data['rsi12'][i - 1] and data['rsi6'][i] < data['rsi12'][i]:
        #     data.loc[data.index[i], '收盘信号'] = 0

        if data['K'][i] < 20 and data['D'][i] < 20 and data['K'][i - 1] <= data['D'][i - 1] and data['K'][i] > \
                data['D'][i]:
            data.loc[data.index[i], '收盘信号'] = 1
        if data['K'][i] > 80 and data['D'][i] > 80 and data['K'][i - 1] >= data['D'][i - 1] and data['K'][i] < \
                data['D'][i]:
            data.loc[data.index[i], '收盘信号'] = 0

    data.close.plot(figsize=(16, 7), grid=True)
    for i in range(len(data)):
        if data['收盘信号'][i] == 1:
            plt.annotate('买', xy=(data.index[i], data.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))
        if data['收盘信号'][i] == 0:
            plt.annotate('卖', xy=(data.index[i], data.close[i]), arrowprops=dict(facecolor='g', shrink=0.1))
    plt.title('上证买卖信号', size=15)
    plt.xlabel('')
    ax = plt.gca()
    ax.spines['right'].set_color('none')
    ax.spines['top'].set_color('none')
    show_data(data)
    plt.show()


def MA_indicator():
    """移动均线"""
    condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2019-01-01"}}
    database = 'stock'
    collection = 'index_data'
    projection = {'_id': False}
    sort_key = "date"

    condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2020-01-01"}}
    condition = {"code": {"$in": ["603019"]}, "time": {"$gte": "2020-01-01"}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False}
    sort_key = "time"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.index = pd.to_datetime(data.time)
    data.sort_index(inplace=True)
    data['ret'] = data['close'].pct_change(1)
    data.dropna(inplace=True)
    types = ['SMA', 'EMA', 'WMA', 'DEMA', 'TEMA', 'TRIMA', 'KAMA', 'MAMA', 'T3']
    type_dict = {k: i for i, k in enumerate(types)}
    print(type_dict)
    df_ma = pd.DataFrame(data.close)
    # for i in range(len(types)):
    #     df_ma[types[i]] = ta.MA(data.close,timeperiod=40,matype=i)
    df_ma['MA'] = ta.TEMA(data.close, timeperiod=100)
    H_line, M_line, L_line = ta.BBANDS(data.close, timeperiod=40, nbdevup=2, nbdevdn=2, matype=0)
    df_ma['H_line'] = H_line
    df_ma['M_line'] = M_line
    df_ma['L_line'] = L_line  # 'H_line', 'M_line', 'L_line',
    df_ma.loc['2023-01-01':][['close', 'H_line', 'M_line', 'L_line']].plot(figsize=(16, 6))
    for i in range(15, len(df_ma)):
        if df_ma['L_line'][i] > df_ma['close'][i]:
            df_ma.loc[df_ma.index[i], '收盘信号'] = 1
    for i in range(len(df_ma)):
        if df_ma['收盘信号'][i] == 1:
            plt.annotate('买', xy=(df_ma.index[i], df_ma.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))
    ax = plt.gca()
    ax.spines['right'].set_color("none")
    ax.spines['top'].set_color("none")
    plt.title("布林曲线", fontsize=15)
    plt.xlabel("")
    plt.show()


def Mo_indicator():
    """动量指标"""
    condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2019-01-01"}}
    database = 'stock'
    collection = 'index_data'
    projection = {'_id': False}
    sort_key = "date"

    condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2020-01-01"}}
    condition = {"code": {"$in": ["002409"]}, "time": {"$gte": "2020-01-01"}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False}
    sort_key = "time"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.index = pd.to_datetime(data[sort_key])
    data.sort_index(inplace=True)
    data['ret'] = data['close'].pct_change(1)
    data.dropna(inplace=True)
    df_ma = pd.DataFrame(data.close)
    df_ma['ADX'] = ta.ADX(data.high, data.low, data.close, timeperiod=14)
    df_ma['ADXR'] = ta.ADXR(data.high, data.low, data.close, timeperiod=14)
    df_ma['CCI'] = ta.CCI(data.high, data.low, data.close, timeperiod=14)
    df_ma['CMO'] = ta.CMO(data.close, timeperiod=14)
    df_ma['BOP'] = ta.BOP(data.open, data.high, data.low, data.close)
    df_ma['aroondown'], df_ma['aroonup'] = ta.AROON(data.high, data.low)
    macd, macdsignal, macdhist = ta.MACD(data.close, fastperiod=12, slowperiod=26, signalperiod=9)

    # df_ma.loc['2023-01-01':][['close']].plot(figsize=(16, 6))
    df_ma['diff'] = macd
    df_ma['dea'] = macdsignal
    df_ma['macd'] = macdhist
    df_ma['delta'] = df_ma['diff'] - df_ma['dea']
    show_data(df_ma)
    plt.figure(figsize=(16, 14))
    plt.subplot(211)
    df_ma.loc['2023-01-01':]['close'].plot(color='r')
    plt.xlabel("")
    plt.title("上证走势", fontsize=15)

    plt.subplot(212)
    df_ma.loc['2023-01-01':]['BOP'].plot()
    plt.title('CMO指标', fontsize=15)
    plt.xlabel('')
    plt.show()

    # for i in range(15,len(df_ma)):
    #     if df_ma['L_line'][i]>df_ma['close'][i]:
    #         df_ma.loc[df_ma.index[i],'收盘信号'] = 1
    df_ma.loc['2023-01-01':]['close'].plot(figsize=(16, 7))
    for i in range(len(df_ma)):
        if i > 0:
            if df_ma['macd'][i - 1] < 0 and df_ma['macd'][i] > 0:
                plt.annotate('买', xy=(df_ma.index[i], df_ma.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))
            if df_ma['macd'][i - 1] > 0 and df_ma['macd'][i] < 0:
                plt.annotate('卖', xy=(df_ma.index[i], df_ma.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))

        # if df_ma['diff'][i]>0 and df_ma['dea'][i]>0 and df_ma['diff'][i]>df_ma['dea'][i]:
        #     plt.annotate('买', xy=(df_ma.index[i], df_ma.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))
        # if df_ma['diff'][i]<0 and df_ma['dea'][i]<0 and df_ma['diff'][i]<df_ma['dea'][i]:
        #     plt.annotate('卖', xy=(df_ma.index[i], df_ma.close[i]), arrowprops=dict(facecolor='r', shrink=0.05))

    ax = plt.gca()
    ax.spines['right'].set_color("none")
    ax.spines['top'].set_color("none")
    plt.title("ADX平均趋向指数", fontsize=15)
    plt.xlabel("")
    plt.show()


if __name__ == '__main__':
    MFI_indicator()
