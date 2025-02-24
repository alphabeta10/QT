import os.path

from analysis.common_analysis import BasicAnalysis
from datetime import datetime, timedelta
from big_models.big_model_api import *
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
from google.genai import types
import pandas as pd

class StockNewsAnalysis(BasicAnalysis):

    def __init__(self, code_list: list, start_time_str=None, model_type="gemini"):
        self.code_list = code_list
        self.stock_day_news_dict = {}

        for code in self.code_list:
            self.stock_day_news_dict[code] = {}

        if start_time_str is None:
            start_time_str = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d")
        database = 'stock'
        collection = 'news'
        projection = {"_id": False}
        condition = {"data_type": {"$in": self.code_list}, "pub_time": {"$gt": start_time_str}}
        self.sort_key = 'pub_time'
        self.news_data = self.get_data_from_mongondb(database, collection, projection, condition, self.sort_key)
        self.__convert_data_to_dict_data()

        if model_type == 'gemini':
            self.client, self.version = google_model_client()
            self.template = '''给定多条新闻数据数据用xml new标签分割的，整体分析这些新闻对股价的影响[利好，利空，中性]，以及原因，新闻数据{news},
                            用json格式返回：
                            Result = {'effect_price':str,' reason':str}
                            Return:Result'''
        else:
            raise Exception("no support model")
        self.to_db = get_mongo_table(collection='big_model')

    def __convert_data_to_dict_data(self):
        self.stock_day_news_dict = {}
        for index in self.news_data.index:
            dict_data = dict(self.news_data.iloc[index])
            new_pub_time = dict_data[self.sort_key]
            day_key = str(new_pub_time)[0:10]
            code = dict_data['data_type']
            self.stock_day_news_dict.setdefault(code, {})
            if day_key not in self.stock_day_news_dict[code].keys():
                self.stock_day_news_dict[code][day_key] = []
            self.stock_day_news_dict[code][day_key].append(dict_data)

    def analysis_news_day_data_to_db(self):
        update_request = []
        for code, day_dict_data in self.stock_day_news_dict.items():
            for day, new_list in day_dict_data.items():
                xml_news = ""
                for dict_data in new_list:
                    content = dict_data['content']
                    xml_news += f"<new>{content}</new>"
                prompt = self.template.replace("news", xml_news)
                ret_json_data = google_simple_model_fn(self.client, prompt, is_ret_json=True)
                print(ret_json_data)

                in_db_data = {"data_type": "stock_day_news_analysis", "time": day, "code": code,
                              "abstract": ret_json_data['reason'],
                              "effect_price": ret_json_data['effect_price']}

                update_request.append(
                    UpdateOne(
                        {"code": in_db_data['code'], 'time': in_db_data['time'], "data_type": in_db_data['data_type']},
                        {"$set": in_db_data},
                        upsert=True)
                )
        if len(update_request) > 0:
            mongo_bulk_write_data(self.to_db, update_request)
            update_request.clear()


class StockFinAnaysis(BasicAnalysis):

    def __init__(self, code_list: list, start_time_str=None):
        self.code_list = code_list
        database = 'stock'
        collection = 'fin_simple'
        projection = {"_id": False}
        if start_time_str is None:
            start_time_str = (datetime.now() - timedelta(days=365 * 10)).strftime("%Y%m%d")

        condition = {"code": {"$in": self.code_list}, 'date': {'$gt': start_time_str}}
        sort_key = 'date'
        self.fin_data = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)

        self.lr_df = self.fin_data[self.fin_data['data_type'] == 'lrb']
        self.xjll_df = self.fin_data[self.fin_data['data_type'] == 'xjll']
        self.zcfz_df = self.fin_data[self.fin_data['data_type'] == 'zcfz']

        date_str = max([str(ele) for ele in self.fin_data['date'].to_list()])


        self.lr_dict_mapping = {
            "code": '股票代码',
            "name": '股票名称',
            'income': '净利润',
            'income_cycle': '净利润同比',
            'total_revenue': '营业总收入',
            'total_revenue_cycle': '营业总收入同比',
            'oper_exp': '营业支出',
            'sell_exp': '销售费用',
            'admin_exp': '管理费用',
            'fin_exp': '财务费用',
            'total_oper_exp': '营业总支出',
            'operate_profit': '营业利润',
            'total_profit': '利润总额',
            "ann_date": '公告日期',
            "date": '财报日期',
            "data_type": '类型'}

        self.xjll_dict_mapping = {
            "code": '股票代码',
            "name": '股票名称',
            'net_cash_flow': '净现金流',
            'net_cash_flow_cycle': '净现金流-同比增长',
            'net_oper_cash_flow': '经营性现金流-现金流量净额',
            'net_oper_cash_flow_rate': '经营性现金流-净现金流占比',
            'invest_cash_flow': '投资性现金流-现金流量净额',
            'invest_cash_flow_rate': '资性现金流-净现金流占比',
            'fa_cash_flow': '融资性现金流-现金流量净额',
            'fa_cash_flow_rate': '融资性现金流-净现金流占比',
            "ann_date": '公告日期',
            "date": '财报日期',
            "data_type": '类型'}

        self.zcfc_dict_mapping = {
            "code": '股票代码',
            "name": '股票名称',
            "money_cap": '货币资金',
            "accounts_receiv": '应收账款',
            "inventories": '存货',
            "total_assets": '总资产',
            "assets_cycle": '总资产同比',
            "lia_acct_payable": '应付账款',
            "lia_acct_receiv": '预收账款',
            "lia_assets": '总负债',
            "lia_assets_cycle": '总负债同比',
            "lia_assets_rate": '资产负债率',
            "total_hldr_eqy_exc_min_int": '股东权益合计',
            "ann_date": '公告日期',
            "date": '财报时间'}

        client, version = google_model_client()
        template = '''该文件是股票利润表，现金流量表，资产负债表三张财报数据，结合三张财报数据，分析该股票投资前景如何以及给出理由'''
        update_request = []
        self.to_db = get_mongo_table(collection='big_model')

        for code in code_list:
            date_str = max([str(ele) for ele in self.lr_df['date'].to_list()])
            pd_data = self.lr_df[self.lr_df['code']==code]
            pd_data = pd_data.rename(columns=self.lr_dict_mapping)
            temp_file_name = f'fn{code}.csv'
            if os.path.exists(temp_file_name) is True:
                os.remove(temp_file_name)

            pd_data.to_csv(temp_file_name, index=False, mode='a')

            pd_data = self.xjll_df[self.xjll_df['code'] == code]
            pd_data = pd_data.rename(columns=self.xjll_dict_mapping)
            pd_data.to_csv(temp_file_name, index=False, mode='a')

            pd_data = self.zcfz_df[self.zcfz_df['code'] == code]
            pd_data = pd_data.rename(columns=self.zcfc_dict_mapping)
            pd_data.to_csv(temp_file_name, index=False, mode='a')

            pd_data = pd.read_csv(temp_file_name, dtype={"股票代码": str})
            new_bytes_obj = pd_data.to_csv(path_or_buf=None, index=False).encode('utf8')
            response = client.models.generate_content(
                model=version,
                contents=[
                    types.Part.from_bytes(
                        data=new_bytes_obj,
                        mime_type='text/csv',
                    ),
                    template
                ]
            )

            ret_llm_conclude = response.text

            in_db_data = {"data_type": "stock_fin_analysis", "time": date_str, "code": code,
                          "abstract": ret_llm_conclude,
                          }

            update_request.append(
                UpdateOne(
                    {"code": in_db_data['code'], 'time': in_db_data['time'], "data_type": in_db_data['data_type']},
                    {"$set": in_db_data},
                    upsert=True)
            )

            if len(update_request) > 0:
                mongo_bulk_write_data(self.to_db, update_request)
                update_request.clear()







if __name__ == '__main__':
    stock_list = ['300308','002352']
    stock_fin = StockFinAnaysis(stock_list)
