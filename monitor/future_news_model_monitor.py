from analysis.common_analysis import BasicAnalysis
from datetime import datetime, timedelta
import os
from utils.actions import show_data
import akshare as ak
from google import genai
from google.genai.types import CreateCachedContentConfig, Content, Part
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from langchain_community.document_loaders import DataFrameLoader
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate


from big_models.big_model_api import *
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


class NewsAnalysis(BasicAnalysis):
    def __init__(self,goods_list:list[str]=None):
        start_date = (datetime.now() - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")
        if goods_list is None:
            goods_list = ['玻璃','螺纹钢','轻质纯碱']
        self.goods_list = goods_list
        database = 'futures'
        collection = 'futures_news'
        condition = {'time': {"$gte": start_date},"data_type":"goods_price_news","metric_code":{"$in":self.goods_list}}
        projection = {"_id": False}
        sort_key = 'time'
        #new_df
        self.df = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)
        # #国家统计局平板玻璃产量
        # fg_code_dict = {"A02090Z02_yd":"玻璃累计产量","A02090Z01_yd":"玻璃产量"}
        # self.fg_acc_volume = self.get_data_from_cn_st(fg_code_dict)
        # show_data(self.fg_acc_volume)






    def google_gemini_model_anlaysis(self):
        new_df = self.df[['content', 'time','metric_code']]
        new_df['day'] = new_df['time'].apply(lambda v: v[0:10])
        days = new_df['day'].unique()
        client, version = google_model_client()
        for m in client.models.list():
            print(m.name)
        for goods_name in self.goods_list:
            for day in days:
                day_df = new_df[(new_df['day'] == day) & (new_df['metric_code']==goods_name)]
                show_data(day_df)
                if not day_df.empty:
                    new_bytes_obj = day_df.to_csv(path_or_buf=None).encode('utf8')
                    template = f'给你的是新闻文本文件content,time分别是内容，发布时间，请你分析对{goods_name}期货影响、风险分析、风险总体评分[评分范围0-100]、情绪总体评分[评分范围0-100]，并给出{goods_name}期货投资建议'
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
                    print(day, response.text)




if __name__ == '__main__':
    news = NewsAnalysis()
    news.google_gemini_model_anlaysis()

