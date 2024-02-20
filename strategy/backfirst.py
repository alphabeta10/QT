import backtrader as bt
from backtrader.feeds import DataBase
from utils.tool import get_data_from_mongo
import pandas as pd
import math
import backtrader.indicators as bi
from backtrader import date2num
import datetime as dt
# import pyfolio as pf
# from pyfolio.utils import to_utc, to_series

# 自定义数据类
class MyData(DataBase):
    lines = ('c1', 'c2', 'c3')  # 自定义参数

    # params = (('c1',-1),('c2',-1),('c3',-1))
    def __init__(self, database='stock', collection='ticker_daily', codes=None, start_date='2023-01-01',
                 col_mapping=None, projection=None):
        if codes is None:
            codes = ['000001']
        self.database = database
        self.collection = collection
        self.codes = codes
        self.start_date = start_date
        if projection is None:
            self.projection = {"_id": False, "code": True, "close": True, "high": True, "low": True, "open": True,
                               "volume": True, "time": True}
        else:
            self.projection = projection
        self.condition = {"code": {"$in": self.codes}, "time": {"$gte": self.start_date}}
        self.col_mapping = col_mapping

    def start(self):
        """
        data init
        :return:
        """
        data = get_data_from_mongo(database=self.database, collection=self.collection,
                                   condition=self.condition,
                                   projection=self.projection)
        data_list = []
        for index in data.index:
            ele = dict(data.loc[index])
            if self.col_mapping is not None:
                new_ele = {}
                for dk, bk in self.col_mapping.items():
                    if bk == 'datetime':
                        new_ele[bk] = dt.datetime.strptime(ele[dk], "%Y-%m-%d")
                    else:
                        new_ele[bk] = ele[dk]
                data_list.append(new_ele)
            else:
                ele['datetime'] = dt.datetime.strptime(ele['time'], "%Y-%m-%d")
                data_list.append(ele)
        self.result = iter(data_list)

    def stop(self):
        pass

    def _load(self):
        # 加载数据 数据迭代
        if self.result is None:
            return False
        try:
            one_row = next(self.result)
        except StopIteration:
            return False
        self.lines.datetime[0] = date2num(one_row.get("datetime"))
        self.lines.open[0] = float(one_row.get("open"))
        self.lines.close[0] = float(one_row.get("close"))
        self.lines.low[0] = float(one_row.get("low"))
        self.lines.high[0] = float(one_row.get("high"))
        self.lines.volume[0] = float(one_row.get("volume"))
        self.lines.openinterest[0] = -1
        self.lines.c1[0] = int(one_row.get("time").split("-")[1])
        return True

class TurtleStrategy(bt.Strategy):
    params = (
        ("long_period",20),
        ("short_period",10),
        ("printlog",True),
    )

    def __init__(self):
        self.order = None
        self.buyprice = 0
        self.comm = 0
        self.buy_size = 0
        self.buy_count = 0
        # macd子图模式显示
        #bt.indicators.MACDHisto(self.data[0], subplot=False)
        #指标
        self.H_line = bi.Highest(self.data.high(-1),period=self.p.long_period)
        self.L_line = bi.Lowest(self.data.low(-1),period=self.p.long_period)
        self.TR = bi.Max((self.data.high(0) - self.data.low(0)), abs(self.data.close(-1) - self.data.high(0)),
                         abs(self.data.close(-1) - self.data.low(0)))
        self.ATR = bi.SimpleMovingAverage(self.TR, period=14,plot=True,plotname='atr')


        self.buy_signal = bt.ind.CrossOver(self.data.close(0),self.H_line,plotname='buy_signal',plot=False)
        self.sell_signal = bt.ind.CrossOver(self.data.close(0),self.L_line,plotname='sell_signal',plot=False)
        for data in self.datas:
            print(data._name)


    def next(self):

        if self.order:
            return
        if self.buy_signal>0 and self.buy_count==0:
            self.buy_size = math.ceil((self.broker.getvalue() * 0.01 / self.ATR) / 100) * 100
            self.sizer.p.stake = self.buy_size
            self.buy_count = 1
            self.order = self.buy()
            self.log("入场")
            # 加仓: 价格上涨了买入价的0.5ATR且加仓次数少于3次(含)
        elif self.data.close > self.buyprice + 0.5 * self.ATR[0] and self.buy_count > 0 and self.buy_count <= 4:
            self.buy_size = math.ceil((self.broker.get_cash() * 0.01 / self.ATR) / 100) * 100
            self.sizer.p.stake = self.buy_size
            self.order = self.buy()
            self.buy_count += 1
            self.log("加仓")

        # 离场: 价格跌破下轨线且持仓时
        elif self.sell_signal < 0 and self.buy_count > 0:
            self.order = self.sell()
            self.buy_count = 0
            self.log("离场")

        # 止损: 价格跌破买入价的2个ATR且持仓时
        elif self.data.close < (self.buyprice - 2 * self.ATR[0]) and self.buy_count > 0:
            self.order = self.sell()
            self.buy_count = 0
            self.log("止损")


        # 输出交易记录
    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        # 有交易提交/被接受，啥也不做
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 交易完成，报告结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '执行买入, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))
                self.buyprice = order.executed.price
                self.comm += order.executed.comm
            else:
                self.log(
                    '执行卖出, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))
                self.comm += order.executed.comm
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")
        self.order = None

    # 输出手续费
    def stop(self):
        self.log("手续费:%.2f 成本比例:%.5f" % (self.comm, self.comm / self.broker.getvalue()))



class KDJStrategy(bt.Strategy):
    params = (
        ("long_period",20),
        ("short_period",10),
        ("printlog",True),
    )

    def __init__(self):
        self.order = None
        self.buyprice = 0
        self.comm = 0
        self.buy_size = 0
        self.buy_count = 0
        # macd子图模式显示
        #bt.indicators.MACDHisto(self.data[0], subplot=False)
        #指标
        self.H_line = bi.Highest(self.data.high(-1),period=self.p.long_period)
        self.L_line = bi.Lowest(self.data.low(-1),period=self.p.long_period)
        self.TR = bi.Max((self.data.high(0) - self.data.low(0)), abs(self.data.close(-1) - self.data.high(0)),
                         abs(self.data.close(-1) - self.data.low(0)))
        self.ATR = bi.SimpleMovingAverage(self.TR, period=14,plot=True,plotname='atr')


        self.buy_signal = bt.ind.CrossOver(self.data.close(0),self.H_line,plotname='buy_signal',plot=False)
        self.sell_signal = bt.ind.CrossOver(self.data.close(0),self.L_line,plotname='sell_signal',plot=False)

        self.res = bt.talib.STOCH(self.data.high, self.data.low, self.data.close, fastk_period=9, slowk_period=5, slowk_matype=1,
                                    slowd_period=5, slowd_matype=1)
        self.rsi12 = bt.talib.RSI(self.data.close, timeperiod=12)
        self.rsi6 = bt.talib.RSI(self.data.close, timeperiod=6)
        self.K,self.D = self.res.slowk,self.res.slowd

        self.buy_signal = bt.And(self.K>0,self.K<15,self.D>0,self.D<15)
        self.sell_signal = bt.Or(self.K>65,self.D>65)




    def next(self):
        print(self.K[0],self.D[0],self.sell_signal[0])
        if self.order:
            return
        if self.buy_signal>0 and self.buy_count==0:
            self.buy_size = math.ceil((self.broker.getvalue() * 0.01 / self.ATR[0]) / 100) * 100
            self.sizer.p.stake = self.buy_size
            self.buy_count = 1
            self.order = self.buy()
            self.log("入场")
            # 加仓: 价格上涨了买入价的0.5ATR且加仓次数少于3次(含)
        # elif self.data.close > self.buyprice + 0.5 * self.ATR[0] and self.buy_count > 0 and self.buy_count <= 4:
        #     self.buy_size = math.ceil((self.broker.get_cash() * 0.01 / self.ATR[0]) / 100) * 100
        #     self.sizer.p.stake = self.buy_size
        #     self.order = self.buy()
        #     self.buy_count += 1
        #     self.log("加仓")

        # 离场: 价格跌破下轨线且持仓时
        elif self.sell_signal > 0 and self.buy_count > 0:
            self.order = self.sell()
            self.buy_count = 0
            self.log("离场")

        # 止损: 价格跌破买入价的2个ATR且持仓时
        elif self.data.close < (self.buyprice - 2* self.ATR[0]) and self.buy_count > 0:
            self.order = self.sell()
            self.buy_count = 0
            self.log("止损")


        # 输出交易记录
    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def notify_order(self, order):
        # 有交易提交/被接受，啥也不做
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 交易完成，报告结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(
                    '执行买入, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))
                self.buyprice = order.executed.price
                self.comm += order.executed.comm
            else:
                self.log(
                    '执行卖出, 价格: %.2f, 成本: %.2f, 手续费 %.2f' %
                    (order.executed.price,
                     order.executed.value,
                     order.executed.comm))
                self.comm += order.executed.comm
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log("交易失败")
        self.order = None

    # 输出手续费
    def stop(self):
        self.log("手续费:%.2f 成本比例:%.5f" % (self.comm, self.comm / self.broker.getvalue()))

class TestStrategy(bt.Strategy):
    params = (('maperiod', 20),
              ('printlog', True),
              ('N1', 20),
              ('N2', 10),
              )

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print("%s,%s" % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close

        self.close = self.datas[0].close
        self.high = self.datas[0].high
        self.low = self.datas[0].low

        # 计算唐奇安通道上轨：过去20日的最高价
        self.DonchianH = bt.ind.Highest(self.high(-1), period=self.p.N1, subplot=True)
        # 计算唐奇安通道下轨：过去10日的最低价
        self.DonchianL = bt.ind.Lowest(self.low(-1), period=self.p.N2, subplot=True)
        # 生成唐奇安通道上轨突破：close>DonchianH，取值为1.0；反之为 -1.0
        self.CrossoverH = bt.ind.CrossOver(self.close(0), self.DonchianH, subplot=False)
        # 生成唐奇安通道下轨突破:
        self.CrossoverL = bt.ind.CrossOver(self.close(0), self.DonchianL, subplot=False)


        self.order = None
        self.buyprice = None
        self.buycomm = None
        # 移动平均
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0].close, period=self.params.maperiod
        )
        self.close_sma_diff = self.datas[0].close - self.sma
        self.atr = bt.talib.ATR(self.datas[0].high, self.datas[0].low, self.datas[0].close, timeperiod=14)
        self.last_buy_price = 0
        self.last_atr = 0
        self.buy_count = 0

    """
    1.入场 ，20均线
    2.持仓位多少 ，atr真实波幅
    """

    def next(self):
        if self.order:
            return

        buy_count = 100

        #仓位管理 准备开几手

        #入场优化
        if not self.position:
            if self.close_sma_diff > 0:
                self.log("BUY CREATE,%.2f" % self.dataclose[0])
                self.order = self.buy(size=buy_count)
                self.last_atr = self.atr[0]
                self.last_buy_price = self.position.price
                self.buy_count = 1
        else:
            if self.position.size > 0:
                if (self.datas[0].close > (self.last_buy_price + 0.5 * self.atr[0]) and self.buy_count <= 3):
                    self.order = self.buy(size=buy_count)
                    self.last_buy_price = self.position.price
                    self.last_atr = (self.last_atr + self.atr[0]) / 2
                    self.log("加仓",doprint=True)
                elif self.dataclose < (self.last_buy_price - self.last_atr * 2):
                    self.order = self.sell(size=self.position.size)
                    self.buy_count = 0
                    self.log("止损", doprint=True)
                if ((self.last_buy_price-self.close)/self.last_buy_price)>0.05:
                    self.order = self.sell(size=self.position.size)
                    self.buy_count = 0
                    self.log("止盈", doprint=True)

    def notify_order(self, order):
        # 如果order为submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入:\n价格:{order.executed.price},\
                成本:{order.executed.value},\
                手续费:{order.executed.comm}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(f'卖出:\n价格：{order.executed.price},\
                成本: {order.executed.value},\
                手续费{order.executed.comm}')
            self.bar_executed = len(self)

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('交易失败')
        self.order = None

    # 记录交易收益情况（可省略，默认不输出结果）
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}')

    # 回测结束后输出结果（可省略，默认输出结果）
    def stop(self):
        self.log('(MA均线： %2d日) 期末总资金 %.2f' %
                 (self.params.maperiod, self.broker.getvalue()), doprint=True)


class ADXStrategy(bt.Strategy):
    params = (('maperiod', 20),
              ('printlog', True),
              ('N1', 20),
              ('N2', 10),
              )

    def log(self, txt, dt=None, doprint=False):
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print("%s,%s" % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close

        self.close = self.datas[0].close
        self.high = self.datas[0].high
        self.low = self.datas[0].low


        self.obv = bt.talib.OBV(self.close, self.datas[0].volume)
        self.avg100obv = bt.talib.SMA(self.obv, timeperiod=100)
        self.adx = bt.talib.ADX(self.high,self.low,self.close,timeperiod=14)


        self.order = None
        self.buyprice = None
        self.buycomm = None
        # 移动平均
        self.sma = bt.indicators.SimpleMovingAverage(
            self.datas[0].close, period=self.params.maperiod
        )
        self.close_sma_diff = self.datas[0].close - self.sma
        self.atr = bt.talib.ATR(self.datas[0].high, self.datas[0].low, self.datas[0].close, timeperiod=14)
        self.last_buy_price = 0
        self.last_atr = 0
        self.buy_count = 0

    """
    1.入场 ，20均线
    2.持仓位多少 ，atr真实波幅
    """

    def next(self):
        if self.order:
            return

        buy_count = 100

        #仓位管理 准备开几手

    def notify_order(self, order):
        # 如果order为submitted/accepted,返回空
        if order.status in [order.Submitted, order.Accepted]:
            return
        # 如果order为buy/sell executed,报告价格结果
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'买入:\n价格:{order.executed.price},\
                成本:{order.executed.value},\
                手续费:{order.executed.comm}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            else:
                self.log(f'卖出:\n价格：{order.executed.price},\
                成本: {order.executed.value},\
                手续费{order.executed.comm}')
            self.bar_executed = len(self)

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('交易失败')
        self.order = None

    # 记录交易收益情况（可省略，默认不输出结果）
    def notify_trade(self, trade):
        if not trade.isclosed:
            return
        self.log(f'策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}')

    # 回测结束后输出结果（可省略，默认输出结果）
    def stop(self):
        self.log('(MA均线： %2d日) 期末总资金 %.2f' %
                 (self.params.maperiod, self.broker.getvalue()), doprint=True)



if __name__ == '__main__':
    cerebro = bt.Cerebro()  #
    print("Starting Portfolio value : %.2f" % cerebro.broker.get_value())
    cerebro.addstrategy(KDJStrategy)
    codes = ['300783']
    data = MyData(codes=codes, start_date='2023-01-01',dataname=codes[0])
    cerebro.adddata(data)
    cerebro.addanalyzer(bt.analyzers.SQN,_name='sqnAnz')
    cerebro.addanalyzer(bt.analyzers.SharpeRatio,_name='SharpeRatio',legacyannual=True)
    cerebro.addanalyzer(bt.analyzers.VWR, _name='VWR')
    cerebro.addanalyzer(bt.analyzers.AnnualReturn, _name='AnnualReturn')
    tframes = dict(days=bt.TimeFrame.Days, weeks=bt.TimeFrame.Weeks, months=bt.TimeFrame.Months,
                   years=bt.TimeFrame.Years)
    cerebro.addanalyzer(bt.analyzers.TimeReturn, timeframe=tframes['years'], _name='TimeAnz')
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='TradeAnalyzer')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
    # cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')

    results = cerebro.run()
    print("Final Portfolio value : %.2f" % cerebro.broker.get_value())
    print('\n#8')
    strat = results[0]
    anzs = strat.analyzers
    dsharp = anzs.SharpeRatio.get_analysis()['sharperatio']
    trade_info = anzs.TradeAnalyzer.get_analysis()
    #
    dw = anzs.DW.get_analysis()
    max_drowdown_len = dw['max']['len']
    max_drowdown = dw['max']['drawdown']
    max_drowdown_money = dw['max']['moneydown']
    #
    print('\n#8-1,基本BT量化分析数据')
    print('\t夏普指数SharpeRatio : ', dsharp)
    print('\t最大回撤周期 max_drowdown_len : ', max_drowdown_len)
    print('\t最大回撤 max_drowdown : ', max_drowdown)
    print('\t最大回撤(资金)max_drowdown_money : ', max_drowdown_money)

    # xpyf = anzs.getbyname('pyfolio')
    # xret, xpos, xtran, gross_lev = xpyf.get_pf_items()
    #
    # xret = to_utc(xret)
    # xpos = to_utc(xpos)
    # xtran = to_utc(xtran)
    # print('\n@xret', xret)
    # print('\n@xpos', xpos)
    # print('\n@xtran', xtran)



    cerebro.plot()

