import pandas as pd
from pyecharts.charts import Tab
from analysis.common_analysis import BasicAnalysis
import wbgapi as wb
import os


class MacroHumanAnalysis(BasicAnalysis):
    """
    宏观分析中的人口统计分析
    分析人口数据
    """

    def __init__(self, **kwargs):
        self.total_metal_dict = {
            "SP.POP.TOTL": "总人口",
            "SM.POP.NETM": "净移民",
            "SP.POP.GROW": "人口增长率",
            "SP.POP.0014.TO.ZS": "0-14岁占比",
            "SP.POP.1564.TO.ZS": "15-64岁占比",
            "SP.POP.65UP.TO.ZS": "65岁以上占比",

            'SP.POP.TOTL.MA.IN': '男性人口',
            'SP.POP.TOTL.FE.IN': '女性人口',

            "SP.POP.TOTL.MA.ZS": "男性占比",
            "SP.POP.TOTL.FE.ZS": "女性占比",

            "SP.URB.TOTL.IN.ZS": "城镇人口(占总人口比例)",
            "SP.RUR.TOTL.ZS": "农村人口(占总人口比例)",

            "SP.POP.0014.TO": "0-14岁人口",
            "SP.POP.1564.TO": "15-64岁人口",
            "SP.POP.65UP.TO": "65岁以上人口",

            'SP.POP.0014.FE.IN': '0-14岁女性人口',
            'SP.POP.0014.MA.IN': '0-14岁男性人口',

            'SP.POP.1564.FE.IN': '15-64岁女性人口',
            'SP.POP.1564.MA.IN': '15-64岁男性人口',

            'SP.POP.65UP.FE.IN': '65岁以上女性人口',
            'SP.POP.65UP.MA.IN': '65岁以上男性人口',

        }  # 总人口的API KEY
        self.region = 'CHN'  # 地区，可以称为经济体
        self.mrv = 10  # 查询最近几年

        if "name" in kwargs.keys():
            self.name = kwargs['name']
        else:
            self.name = 'human'
        if not os.path.exists(self.name):
            os.mkdir(self.name)

        self.__load_data()  # 加载数据

    def __load_data(self):
        file_name = self.name + f"/{self.region}_total.csv"
        self.total_human_df = self.get_wd_data(list(self.total_metal_dict.keys()), self.region, self.mrv, file_name)

    def __convert_data(self, col_index):
        total_sr = self.total_human_df.loc[col_index]
        total_dict = dict(total_sr)

        cdf = pd.DataFrame([{'time': k, "value": v} for k, v in total_dict.items()])
        cdf.sort_values(by='time', inplace=True)
        return cdf, total_dict

    def __judge_data(self, value, config_list: list):
        for ele in config_list:
            gt_value = ele.get('gt', None)
            gte_value = ele.get('gte', None)
            final_gt_value = gte_value if gte_value else gt_value
            gt_type = ">=" if gte_value else ">"

            gt_pass = eval(f"True if final_gt_value and final_gt_value{gt_type}{value} else False")

            lt_value = ele.get('lt', None)
            lte_value = ele.get('lte', None)
            final_lt_value = lt_value if lte_value else lte_value
            lt_type = "<" if lte_value else "<="

            lt_pass = eval(f"True if final_lt_value and final_lt_value{lt_type}{value} else False")

            if final_lt_value and final_gt_value:
                if lt_pass and gt_pass:
                    return ele['name']
            if final_lt_value and not final_gt_value:
                if lt_pass:
                    return ele['name']
            if not final_lt_value and final_gt_value:
                if gt_pass:
                    return ele['name']

    def total_analysis(self):
        total_df, _ = self.__convert_data('SP.POP.TOTL')
        netm_df, _ = self.__convert_data('SM.POP.NETM')

        total_df['pct'] = round(total_df['value'].pct_change(1) * 100, 4)
        total_df['value'] = round(total_df['value'] / 1e4, 2)

        x_label = list(total_df['time'].values)
        bar_y_dict_data = {"总人口(万人)": list(total_df['value'].values)}
        line_y_dict_data = {"增长率(%)": list(total_df['pct'].values)}

        tab = Tab()
        chart = self.bar_line_overlap(x_label, bar_y_dict_data, line_y_dict_data)
        tab.add(chart, '总人口分析')

        netm_df['value'] = netm_df['value'] / 1e4
        chart = self.bar_chart(x_label, {"净移民(万人)": list(netm_df['value'].values)})
        tab.add(chart, '净移民')

        old_stand_config_dict = {
            "0_14_rate": [
                {"lt": 0.15, "name": "超少子化"},
                {"gte": 0.15, "lt": 0.18, "name": "严重少子化"},
                {"gte": 0.18, "lt": 0.20, "name": "少子化"},
                {"gte": 0.2, "lt": 0.23, "name": "正常"},
                {"gte": 0.23, "lt": 0.3, "name": "多子化"},
                {"gte": 0.3, "lt": 0.4, "name": "严重多子化"},
                {"gte": 0.4, "name": "超多子化"},
            ],
            "gt65_rate": [
                {"gte": 0.7, "name": "老龄化社会"}
            ]
        }

        pop14_df, zero_14_dict_rate = self.__convert_data('SP.POP.0014.TO.ZS')
        pop1564_df, pop_1565_dict_rete = self.__convert_data('SP.POP.1564.TO.ZS')
        pop65up_df, pop65up_dict_rate = self.__convert_data('SP.POP.65UP.TO.ZS')

        ma_rate_df, ma_dict_rate = self.__convert_data('SP.POP.TOTL.MA.ZS')
        fe_rate_df, fe_dict_rate = self.__convert_data('SP.POP.TOTL.FE.ZS')

        _, urb_dict_rate = self.__convert_data('SP.URB.TOTL.IN.ZS')
        _, rur_dict_rate = self.__convert_data('SP.RUR.TOTL.ZS')

        recent_key = list(zero_14_dict_rate.keys())[-1]

        pop_14_value = round(zero_14_dict_rate.get(recent_key), 2)
        pop_14_conclude = self.__judge_data(pop_14_value / 100, old_stand_config_dict['0_14_rate'])
        print(f"0-14 {pop_14_conclude} {pop_14_value}")

        pop_1565_value = round(pop_1565_dict_rete.get(recent_key), 2)
        pop_65up_value = pop65up_dict_rate.get(recent_key)
        pop_65up_value = round(pop_65up_value,2)
        pop65up_conclude = self.__judge_data(pop_65up_value / 100, old_stand_config_dict['gt65_rate'])
        print(f"gt65 {pop65up_conclude} {pop_65up_value}")

        pie_dict_data = {"人口年龄结构": [pop_14_value, pop_1565_value, pop_65up_value],
                         "性别占比": [round(ma_dict_rate.get(recent_key), 2), round(fe_dict_rate.get(recent_key), 2)],
                         "城乡占比": [round(urb_dict_rate.get(recent_key), 2), round(rur_dict_rate.get(recent_key), 2)],
                         }
        pie_attr_dict_data = {'人口年龄结构': [f'0-14岁 {pop_14_conclude}', '15-64岁', f'大于65岁{pop65up_conclude}'],
                              "性别占比": ['男性占比', '女性占比'],
                              "城乡占比": ['城市', '乡村'],
                              }
        chart = self.pie_chart('', pie_attr_dict_data, pie_dict_data)
        tab.add(chart, '人口占比分析')

        # 人口结构
        '''
        'SP.POP.TOTL.MA.IN': '男性人口',
        'SP.POP.TOTL.FE.IN': '女性人口',
        '''
        ma_df, _ = self.__convert_data('SP.POP.TOTL.MA.IN')
        fe_df, _ = self.__convert_data('SP.POP.TOTL.FE.IN')
        ma_df['pct'] = round(ma_df['value'].pct_change(1) * 100, 4)
        ma_df['value'] = round(ma_df['value'] / 1e4, 2)
        fe_df['value'] = round(fe_df['value'] / 1e4, 2)
        fe_df['pct'] = round(fe_df['value'].pct_change(1) * 100, 4)

        ma_rate_df['value'] = round(ma_rate_df['value'], 2)
        fe_rate_df['value'] = round(fe_rate_df['value'], 2)

        bar_y_dict_data = {"男性人口(万人)": list(ma_df['value'].values), "女性人口(万人)": list(fe_df['value'].values)}
        line_y_dict_data = {"男性占比(%)": list(ma_rate_df['value'].values),
                            "女性占比(%)": list(fe_rate_df['value'].values),
                            "男性增长(%)": list(ma_df['pct'].values),
                            "女性增长(%)": list(fe_df['pct'].values)}
        chart = self.bar_line_overlap(x_label, bar_y_dict_data, line_y_dict_data)
        tab.add(chart, "男性女性结构分析")
        # 年龄结构
        '''
        "SP.POP.0014.TO": "0-14岁人口",
        "SP.POP.1564.TO": "15-64岁人口",
        "SP.POP.65UP.TO": "65岁以上人口",
        '''
        year_value_dict = {
            "SP.POP.0014.TO": "0-14岁人口",
            "SP.POP.1564.TO": "15-64岁人口",
            "SP.POP.65UP.TO": "65岁以上人口",
        }
        bar_y_dict_data = {}
        line_y_dict_data = {}
        for k,name in year_value_dict.items():
            df, _ = self.__convert_data(k)
            df['value'] = round(df['value'] / 1e4, 2)
            df['pct'] = round(df['value'].pct_change(1),4)
            line_y_dict_data[name+"增长率(%)"] = list(df['pct'].values)
            bar_y_dict_data[name] = list(df['value'].values)
        pop14_df['value'] = round(pop14_df['value'],2)
        pop1564_df['value'] = round(pop1564_df['value'],2)
        pop65up_df['value'] = round(pop65up_df['value'],2)
        line_y_dict_data['0-14岁人口占比'] = list(pop14_df['value'].values)
        line_y_dict_data['15-64岁人口占比'] = list(pop1564_df['value'].values)
        line_y_dict_data['65岁以上人口占比'] = list(pop65up_df['value'].values)

        chart = self.bar_line_overlap(x_label, bar_y_dict_data, line_y_dict_data)
        tab.add(chart, "人口年龄结构分析")

        ma_fe_dict = {
            'SP.POP.0014.FE.IN': '0-14岁女性人口',
            'SP.POP.0014.MA.IN': '0-14岁男性人口',

            'SP.POP.1564.FE.IN': '15-64岁女性人口',
            'SP.POP.1564.MA.IN': '15-64岁男性人口',

            'SP.POP.65UP.FE.IN': '65岁以上女性人口',
            'SP.POP.65UP.MA.IN': '65岁以上男性人口',
        }

        bar_y_dict_data = {}
        line_y_dict_data = {}
        for k,name in ma_fe_dict.items():
            df, _ = self.__convert_data(k)
            df['value'] = round(df['value'] / 1e4, 2)
            df['pct'] = round(df['value'].pct_change(1), 4)
            line_y_dict_data[name + "增长率(%)"] = list(df['pct'].values)
            bar_y_dict_data[name] = list(df['value'].values)
        chart = self.bar_line_overlap(x_label, bar_y_dict_data, line_y_dict_data)
        tab.add(chart, "人口年龄结性别构分析")
        # 人口城乡结构

        # 劳动力人数及抚养比

        tab.render(self.name + f"/{self.region}人口分析.html")

    def get_wd_data(self, col_list, region='all', mrv=10, file_name=None, is_local=True):
        """
        获取世界银行的数据
        :param col_list:
        :param region:
        :param mrv:
        :return:
        """
        if file_name is not None and os.path.exists(file_name) and is_local:
            print(f"start local load data {file_name}")
            df = pd.read_csv(file_name)
            df.set_index(keys='series', inplace=True)
            print(f"end local load data {file_name}")
            return df
        print("start online load data ")
        df = wb.data.DataFrame(col_list, region, mrv=mrv)
        print("end online load data ")
        if file_name is not None:
            df.to_csv(file_name)
        return df


if __name__ == '__main__':
    analysis = MacroHumanAnalysis()
    analysis.total_analysis()
