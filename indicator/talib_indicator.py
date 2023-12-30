import talib as ta
from utils.tool import get_data_from_mongo
from utils.actions import show_data
import pandas as pd
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def MFI_indicator():
    condition = {"code": {"$in": ["sh000001"]}, "date": {"$gte": "2020-01-01"}}
    database = 'stock'
    collection = 'index_data'
    projection = {'_id': False}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.index = pd.to_datetime(data.date)
    data.sort_index(inplace=True)
    data['ret'] = data['close'].pct_change(1)
    data.dropna(inplace=True)
    """
    TYP:=(HIGH+LOW+CLOSE)/3;
    V1:=SUM(IF(TYP＞REF(TYP,1),TYP*VOL,0),N)/SUM(IF(TYP＜REF(TYP,1),TYP*VOL,0),N)
    MFI:100-(100/(1+V1));
    """

    timeperiod = 10
    data['mfi'] = ta.MFI(data.high, data.low, data.close, data.volume, timeperiod=timeperiod)
    show_data(data)

    plt.figure(figsize=(16,14))
    plt.subplot(211)
    data['close'].plot(color='r')
    plt.xlabel("")
    plt.title("上证走势",fontsize=15)

    plt.subplot(212)
    data['mfi'].plot()
    plt.title('MFI指标', fontsize=15)
    plt.xlabel('')
    plt.show()

    for i in range(15,len(data)):
        if data['mfi'][i]>20 and data['mfi'][i-1]<20:
            data.loc[data.index[i],'收盘信号'] = 1
        if data['mfi'][i]<80 and data['mfi'][i-1]>80:
            data.loc[data.index[i],'收盘信号'] = 0
    data.close.plot(figsize=(16, 7))
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
    plt.show()


if __name__ == '__main__':
    MFI_indicator()
