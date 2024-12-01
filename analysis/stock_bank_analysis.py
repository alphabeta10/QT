from analysis.common_analysis import BasicAnalysis
import pandas as pd
from datetime import datetime,timedelta
from utils.tool import sort_dict_data_by
from pyecharts.charts import Page, Tab
from utils.actions import show_data

class BankAnalysis(BasicAnalysis):

    def __init__(self):
        self.code_mapping = {'SH601825': '沪农商行', 'SH601229': '上海银行', 'SH600919': '江苏银行',
                             'SH601860': '紫金银行',
                             'SH601838': '成都银行', 'SH600926': '杭州银行', 'SH601169': '北京银行',
                             'SZ002142': '宁波银行',
                             'SH600908': '无锡银行', 'SZ002807': '江阴银行', 'SH601166': '兴业银行',
                             'SH600016': '民生银行',
                             'SH601288': '农业银行', 'SH601916': '浙商银行', 'SZ002948': '青岛银行',
                             'SH601077': '渝农商行',
                             'SH601988': '中国银行', 'SH601658': '邮储银行', 'SH601939': '建设银行',
                             'SH601963': '重庆银行',
                             'SH601528': '瑞丰银行', 'SH601818': '光大银行', 'SH601328': '交通银行',
                             'SH601398': '工商银行',
                             'SH600036': '招商银行', 'SH601128': '常熟银行', 'SH601998': '中信银行',
                             'SZ000001': '平安银行'}
        self.name = "银行分析"

    def stock_score(self, pd_data: pd.DataFrame, metric, sort_type=False):
        data = pd.pivot_table(pd_data, values=metric, index=['date'], columns=['code'])
        dict_list = []
        for index in data.index:
            dict_data = dict(data.loc[index])
            sort_dict_data = sort_dict_data_by(dict_data, by='value', reverse=sort_type)
            num = len(sort_dict_data)
            before_rank, before_ele = 0, 0
            for i, combine in enumerate(sort_dict_data.items()):
                k, v = combine
                if i == 0:
                    before_rank, before_ele = i + 1, v
                else:
                    if before_ele != v:
                        before_rank += 1
                    before_ele = v
                score = round((before_rank / num) * 100, 4)
                dict_list.append({"code": k, f"{metric}_score": score, "date": index})
        score_df = pd.DataFrame(data=dict_list)
        return score_df

    def generator_analysis_html(self):
        col_name_mp = {'LTDRR': '存贷款比例',
                       'NONPERLOAN': '不良贷款率(%)',
                       'BLDKBBL': '不良贷款拨备覆盖率(%)',
                       'TOTALOPERATEREVETZ': '营业总收入同比增长(%)',
                       'PARENTNETPROFITTZ': '归属净利润同比增长(%)',
                       'ZCFZL': '资产负债率(%)',
                       }
        metric_sort_type = {
            'LTDRR': True,
            'NEWCAPITALADER': False,
            'HXYJBCZL': False, 'NONPERLOAN': True,
            'BLDKBBL': True,
            'ZCFZL': True
        }

        def handle_score(row, col_list):
            total_score = 0
            for col in col_list:
                total_score += row[col]
            return total_score

        code_list = list(self.code_mapping.keys())
        code_list = [ele[2:] for ele in code_list]
        code_name_mp = {k[2:]: v for k, v in self.code_mapping.items()}
        time = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        condition = {"code": {"$in": code_list}, "date": {"$gte": time}, "data_type": "stock_em_metric"}
        sort_key = 'date'
        database = 'stock'
        collection = 'fin'
        projection = {"_id": False}
        pd_data = self.get_data_from_mongondb(database,collection,projection,condition,sort_key)

        score_df_list = []
        metric_core_col = []
        tab = Tab()
        for metric, metric_name in col_name_mp.items():
            pd_data[metric] = pd_data[metric].astype(float)
            metric_core_col.append(f"{metric}_score")
            sort_type = False
            if metric in metric_sort_type.keys():
                sort_type = metric_sort_type.get(metric)
            score_df = self.stock_score(pd_data, metric, sort_type=sort_type)
            score_df_list.append(score_df)
            data = pd.pivot_table(pd_data, values=metric, index=['date'], columns=['code'])
            data = data.rename(columns=code_name_mp)

            temp_chart =[]
            self.df_to_chart(data,temp_chart)

            tab.add(temp_chart[0],metric_name)



        score_df = score_df_list[0]
        for ele in score_df_list[1:]:
            score_df = pd.merge(score_df, ele, left_on=['code', 'date'], right_on=['code', 'date'])
        score_df['total_score'] = score_df.apply(handle_score, axis=1, args=(metric_core_col,))

        data = pd.pivot_table(score_df, values='total_score', index=['date'], columns=['code'])
        data = data.rename(columns=code_name_mp)
        temp_chart = []
        self.df_to_chart(data, temp_chart)
        tab.add(temp_chart[0], '分数')
        show_data(data)
        tab.render(f"{self.name}.html")


if __name__ == '__main__':
    bank = BankAnalysis()
    bank.generator_analysis_html()