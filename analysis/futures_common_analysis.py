import json

import pandas as pd
from pyecharts.charts import Tab

from analysis.common_analysis import BasicAnalysis
from analysis.analysis_tool import convert_pd_data_to_month_data
from datetime import datetime, timedelta
import os
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']

from indicator.talib_indicator import adj_obv, common_indictator_cal
from monitor.real_common import st_peak_data
from utils.actions import show_data


class FuturesCommonAnalysis(BasicAnalysis):
    """
    期货基础分析
    """

    def __init__(self, *args, **kwargs):

        self.code = kwargs['code']
        self.fin_start_date = kwargs.get('fin_start_date',
                                         (datetime.now() - timedelta(days=365 * 20)).strftime("%Y-%m-%d"))

        self.name = kwargs.get('name', self.code)
        self.dir = kwargs.get("dir", self.name)

        if self.dir and not os.path.exists(self.dir):
            os.mkdir(self.dir)

        self.import_condition = kwargs.get("import_condition", None)
        self.export_condition = kwargs.get("export_condition", None)
        self.long_short_code = kwargs.get("long_short_code", None)
        # 产业链配置
        self.chain_config = kwargs.get("chain_config", None)

        self.judge_future_config = [
            {"price": "up", "volume": "up", "hold": "up", "result": "坚挺"},
            {"price": "up", "volume": "down", "hold": "down", "result": "疲弱"},
            {"price": "down", "volume": "up", "hold": "up", "result": "疲弱"},
            {"price": "down", "volume": "down", "hold": "down", "result": "坚挺"},
        ]
        self.__load_data()

    def futures_indicator(self, data: pd.DataFrame):
        sort_key = 'date'
        data.index = pd.to_datetime(data[sort_key])
        st_peak_data(data, sort_key)
        data['adj_obv'] = adj_obv(data.high, data.low, data.close, data.volume)
        common_indictator_cal(data, ma_timeperiod=20)

        data['pre_volume'] = data['volume'].shift(1)
        data['ret_1'] = data['close'].pct_change(1)
        data['pre_hold'] = data['hold'].shift(1)
        data['market_env'] = data.apply(self.jude_current_market, axis=1)

        return data

    def jude_current_market(self, row: pd.Series):
        volume = row['volume']
        pre_volume = row['pre_volume']
        pct_change = row['ret_1']
        hold = row['hold']
        pre_hold = row['pre_hold']
        result_dict = {}
        if volume > pre_volume:
            result_dict['volume'] = 'up'
        else:
            result_dict['volume'] = 'down'

        if pct_change > 0:
            result_dict['price'] = 'up'
        else:
            result_dict['price'] = 'down'

        if hold > pre_hold:
            result_dict['hold'] = 'up'
        else:
            result_dict['hold'] = 'down'
        for config in self.judge_future_config:
            is_pass = True
            for k in ['price', 'volume', 'hold']:
                if config[k] != result_dict.get(k):
                    is_pass = False
                    break
            if is_pass:
                result_dict['result'] = config.get('result')
                break
        return result_dict.get('result', '无法判断市场行情')

    def __convert_price_data(self, data: pd.DataFrame):
        datas = []
        for index in data.index:
            ele = dict(data.loc[index])
            time = ele['time']
            value = ele['value']
            name = ele['name']
            year = time[0:4]
            new_name = f"{year}{name}"
            new_time = time[4:9]
            datas.append({"name": new_name, "value": value, "time": new_time, "raw_name": name})
        return pd.DataFrame(datas)

    def __convert_goods_line_plot(self, data: pd.DataFrame):
        datas = []
        line_char_dict = {}
        goods_name_list = set()
        for index in data.index:
            ele = dict(data.loc[index])
            time = ele['time']
            value = ele['value']
            name = ele['name']
            goods_name_list.add(name)
            year = time[0:4]
            new_name = f"{year}{name}"
            new_time = time[4:9]
            datas.append({"name": new_name, "value": value, "time": new_time, "raw_name": name})
        pd_data = pd.DataFrame(data=datas)
        for goods_name in goods_name_list:
            data = pd_data[pd_data['raw_name'] == goods_name]
            data[['value']] = data[['value']].astype(float)
            data = pd.pivot_table(data, values='value', columns='name', index='time')
            x_labels = list(data.index.values)
            y_dict_data = {}
            for col in data.columns:
                y_dict_data[col] = list(data[col].values)
            line_char_dict[goods_name] = self.line_chart(x_labels, y_dict_data)
        return line_char_dict

    def __load_data(self):
        """
        1. 加载价格数据，多空比数据，指标数据
        2. 基本面数据 供给分析 需求分析 库存分析 进出口分析 期现基差
        :return:
        """
        # 期货价格数据
        database = 'futures'
        collection = 'futures_daily'
        sort_key = "date"
        code_name = 'symbol'
        condition = {code_name: self.code, sort_key: {"$gte": self.fin_start_date}}
        projection = {"_id": False}
        self.futures_price_data = self.get_data_from_mongondb(database, collection, projection, condition, sort_key,
                                                              self.futures_indicator)
        # 多空比数据
        if self.long_short_code:
            database = 'futures'
            collection = 'futures_basic_info'
            sort_key = "date"
            projection = {'_id': False}
            condition = {"data_type": "futures_long_short_rate", "date": {"$gt": self.fin_start_date},
                         "code": self.long_short_code}
            self.futures_long_short_data = self.get_data_from_mongondb(database, collection, projection, condition,
                                                                       sort_key)
            show_data(self.futures_long_short_data)

        # 现货价格数据
        database = 'stock'
        collection = 'goods'
        sort_key = "time"
        code_name = 'name'
        condition = {code_name: self.name, sort_key: {"$gte": self.fin_start_date.replace("-", "")}}
        projection = {"_id": False}
        self.price_data = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)
        if len(self.price_data) > 0:
            self.price_data = self.__convert_price_data(self.price_data)
            self.price_data[['value']] = self.price_data[['value']].astype(float)
            data = pd.pivot_table(self.price_data, values='value', columns='name', index='time')
        #     data.plot(kind='line', title=self.name, rot=45, figsize=(15, 8), fontsize=10)
        #     plt.show()
        # show_data(self.price_data)

        # 进出口数据
        if self.import_condition is not None:
            self.import_data = self.get_data_from_board(condition=self.import_condition)

        if self.export_condition is not None:
            self.export_data = self.get_data_from_board(condition=self.export_condition)
        # 库存数据 没有同花顺期货APP查看
        # 需求数据 国家统计局数据
        # 产业链分析 上游，中游，下游 可视化分析

        if self.chain_config:
            chain_mapping = {"up_stream": "上游", "mid_stream": "中游", "down_stream": "下游"}

            for chain_key, chain_name in chain_mapping.items():
                combine_chain_dict = self.chain_config.get(chain_key, None)
                if combine_chain_dict:
                    tab = Tab()
                    for db_type, condition_list in combine_chain_dict.items():
                        if db_type == 'goods':
                            data, _ = self.get_data_from_goods(condition_list, self.fin_start_date.replace("-", ""))
                            line_char_dict = self.__convert_goods_line_plot(data)
                            for line_name, line_char in line_char_dict.items():
                                tab.add(line_char, line_name)
                        if db_type == 'st':
                            code_mapping = {}
                            for ele in condition_list:
                                for k, v in ele.items():
                                    code_mapping[k] = v
                            data = self.get_data_from_cn_st(code_mapping, '201501')
                            cols = data.columns
                            for code, name in code_mapping.items():
                                if code in cols:
                                    df_data = data[[code, 'time']]
                                    df_data['name'] = name
                                    gt_month = 2 if '累计' in name else 3
                                    df_data = self.tool_filter_month_data(df_data, gt_month=gt_month)
                                    df_data = convert_pd_data_to_month_data(df_data, 'time', code, 'name')
                                    x_labels = list(df_data.index.values)
                                    y_dict_data = {}
                                    for col in df_data.columns:
                                        y_dict_data[col] = list(df_data[col].values)
                                    line_char = self.line_chart(x_labels, y_dict_data)
                                    tab.add(line_char, name)
                        if db_type == 'board':
                            amount_col_dict = {
                                "acc_month_amount": "累计金额(亿元)",
                                "month_amount": "当期金额(亿元)",
                                "acc_month_amount_cyc": "累计金额同比",
                                "month_amount_cyc": "当期金额同比",
                            }
                            volume_col_dict = {
                                "acc_month_volume": "累计量",
                                "month_volume": "当月量",
                                "acc_month_volume_cyc": "累计量同比",
                                "acc_price": "累计均价",
                                "cur_price": "当月均价",
                            }

                            board_type_mapping = {"export_goods_detail":"出口","import_goods_detail":"进口"}

                            for board_condition in condition_list:
                                board_condition['date'] = {"$gt": self.fin_start_date}
                                board_df = self.get_data_from_board(condition=board_condition)
                                plot_config_dict = {}
                                for k,v in amount_col_dict.items():
                                    plot_config_dict[k] = v
                                if board_condition['unit'] != '-':
                                   for k,v in volume_col_dict.items():
                                       if k in ['acc_month_volume','month_volume']:
                                           name_unit = board_condition.get('unit','')
                                           v += f"({name_unit})"
                                       plot_config_dict[k] = v

                                for at_key, name in plot_config_dict.items():
                                    plot_df = board_df[[at_key, 'date']]
                                    plot_df[at_key] = plot_df[at_key].astype(float)
                                    if at_key in ['acc_month_amount','month_amount']:
                                        plot_df[at_key] = round(plot_df[at_key]/1e4,2)
                                    plot_df['name'] = name
                                    plot_df['date'] = plot_df['date'].apply(lambda ele:ele.replace("-",""))
                                    plot_df = convert_pd_data_to_month_data(plot_df,'date',at_key,'name')
                                    x_labels = list(plot_df.index.values)
                                    y_dict_data = {}
                                    for col in plot_df.columns:
                                        y_dict_data[col] = list(plot_df[col].values)
                                    line_char = self.line_chart(x_labels, y_dict_data)
                                    tab.add(line_char, board_type_mapping.get(board_condition["data_type"])+board_condition['name']+name)

                    tab.render(self.dir + "/" + chain_name + ".html")


if __name__ == '__main__':
    with open('fg.futures.json',encoding='utf8',mode='r') as f:
        chain_config = json.load(f)
        print(chain_config)
        fg_analysis = FuturesCommonAnalysis(code='FG0', name='玻璃', long_short_code='fg2505',
                                    import_condition=None, chain_config=chain_config)
