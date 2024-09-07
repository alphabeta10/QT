from analysis.common_analysis import BasicAnalysis
from indicator.talib_indicator import adj_obv
from datetime import datetime, timedelta
import pandas as pd
from risk_manager.common_risk_utils import comm_down_or_up_risk
from pyecharts.charts import Tab
import os
import talib as ta


class CashFlowAnalysis(BasicAnalysis):

    def __init__(self, *args, **kwargs):

        if 'currency_start_date' in kwargs.keys():
            self.currency_start_date = kwargs['currency_start_date']
        else:
            self.currency_start_date = (datetime.now() - timedelta(days=30 * 8)).strftime("%Y-%m-%d")

        if "name" in kwargs.keys():
            self.name = kwargs['name']
        else:
            self.name = 'cash_flow_analysis'
        if not os.path.exists(self.name):
            os.mkdir(self.name)
        self._load_data()

    def _load_data(self):
        """
        加载数据
        :return:
        """
        # 加载货币数据
        database, collection, projection, condition, sort_key = 'stock', 'stock_seq_daily', {"_id": False}, {
            "metric_key": "global_currency", "time": {"$gt": self.currency_start_date}}, 'time'
        df = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)

        self.currency_flat_df = pd.pivot_table(df, values='target_currency_value', index='time', columns='sub_key')
        self.currency_flat_df.reset_index(inplace=True)
        self.currency_flat_df.set_index(keys='time', inplace=True)
        # 加载美股指数数据
        condition = {
            "metric_key": "us_stock_market_fun_flow", "time": {"$gt": self.currency_start_date}}
        self.us_index_df = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)

        # 加载A股指数数据

        code_dict = {
            "sh000001": "上证指数",
            "sz399001": "深证成指",
            "sh000852": "中证1000"
        }
        codes = list(code_dict.keys())
        condition = {"code": {"$in": codes}, "date": {"$gte": self.currency_start_date}}
        database = 'stock'
        collection = 'index_data'
        projection = {'_id': False}
        sort_key = "date"
        self.cn_index_df = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)

        # 加载中国指数资金流向
        database, collection, projection, condition, sort_key = 'stock', 'stock_seq_daily', {"_id": False}, {
            "metric_key": "cn_stock_market_fun_flow", "time": {"$gt": self.currency_start_date},
            "sub_key": "sh_sz_fun_flow"}, 'time'
        self.cn_index_cash_flow_df = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)

    def index_analysis_data(self):
        # 美股指数
        index_mappinng = {".IXIC": "纳斯达克综合指数", ".DJI": "道琼斯工业平均指数", ".INX": "标普500指数",
                          ".NDX": "纳斯达克100指数"}
        tab = Tab()
        for k in index_mappinng.values():
            data = self.us_index_df[self.us_index_df['sub_key'] == k]
            x_labels = list(data['time'].values)
            for col in ['high', 'low', 'close', 'volume']:
                data[col] = data[col].astype(float)

            data.index = pd.to_datetime(data['time'])
            data['adj_obv'] = adj_obv(data.high, data.low, data.close, data.volume)
            y_dict_data = {k: list(data['adj_obv'].values)}

            tab.add(self.line_chart(x_labels, y_dict_data), k)
        # A股指数
        code_dict = {
            "sh000001": "上证指数",
            "sz399001": "深证成指",
            "sh000852": "中证1000"
        }

        for code, name in code_dict.items():
            data = self.cn_index_df[self.cn_index_df['code'] == code]
            x_labels = list(data['date'].values)
            for col in ['high', 'low', 'close', 'volume']:
                data[col] = data[col].astype(float)

            data.index = pd.to_datetime(data['date'])
            data['adj_obv'] = adj_obv(data.high, data.low, data.close, data.volume)

            y_dict_data = {name: list(data['adj_obv'].values)}
            tab.add(self.line_chart(x_labels, y_dict_data), name)

        cn_index_cash_flow_config = {'上证和深证主力净流入净额(亿元)': 'main_force_flow_in',
                                     '上证和深证超大单净流入净额(亿元)': 'supper_order_flow_in',
                                     '上证和深证大单净流入净额(亿元)': 'big_order_flow_in',
                                     '上证和深证中单净流入净额(亿元)': 'mid_order_flow_in',
                                     '上证和深证小单净流入净额(亿元)': 'small_order_flow_in'}
        index_flow_xlabels = list(self.cn_index_cash_flow_df['time'].values)

        for cname,code in cn_index_cash_flow_config.items():
            self.cn_index_cash_flow_df[code] = self.cn_index_cash_flow_df[code].astype(float)
            self.cn_index_cash_flow_df[cname+"15均值"] = ta.SMA(self.cn_index_cash_flow_df[code],timeperiod=15)
            dv = self.cn_index_cash_flow_df[code]
            self.cn_index_cash_flow_df[cname+"adx14"] = ta.ADX(dv,dv,dv,timeperiod=14)
            avg_key = cname+"15均值"
            y_dict_data = {cname:[round(float(ele)/1e8,4) for ele in self.cn_index_cash_flow_df[code].values],avg_key:[round(ele/1e8,4) for ele in list(self.cn_index_cash_flow_df[avg_key].values)]}
            tab.add(self.line_chart(index_flow_xlabels,y_dict_data),cname)
        from utils.actions import show_data
        show_data(self.cn_index_cash_flow_df)
        tab.render(self.name + "/index_data.html")

    def common_analysis_currency_data(self, currencys=None):

        if currencys is None:
            currencys = ['USD_CNY']

        if self.currency_flat_df is not None:
            x_labels = None
            tab = Tab()
            for currency in currencys:
                y_dict_data = {}
                all_detail_risk, all_datas = comm_down_or_up_risk(self.currency_flat_df, [currency],
                                                                  [3, 7, 14, 30, 60, 90, 120],
                                                                  {currency: "up"}, "index")
                rs_df = pd.DataFrame(data=all_datas)
                rs_df.set_index(keys='time', inplace=True)
                if x_labels is None:
                    x_labels = list(rs_df.index.values)
                elif len(x_labels) < len(list(rs_df.index.values)):
                    x_labels = list(rs_df.index.values)
                y_dict_data[currency] = list(rs_df['total_risk'].values)[-100:]
                tab.add(self.line_chart(x_labels[-100:], y_dict_data), currency)
            tab.render(self.name + "/currency.html")


if __name__ == '__main__':
    cash_flow_analysis = CashFlowAnalysis()
    cash_flow_analysis.common_analysis_currency_data(['USD_CNY', 'USD_BTC'])
    cash_flow_analysis.index_analysis_data()
