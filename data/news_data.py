import akshare as ak
import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import time
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import schedule

def all_platform_news_data():
    news = get_mongo_table(database='stock', collection='all_news')
    col_mapping = {"标题": "title", "内容": "content", "发布时间": "publish_time","发布日期":"publish_day"}
    #"标题", "内容", "发布日期", "发布时间"
    datas = []
    stock_info_global_cls_df = ak.stock_info_global_cls(symbol="全部")
    stock_info_global_cls_df.rename(columns=col_mapping, inplace=True)
    for index in stock_info_global_cls_df.index:
        dict_data = dict(stock_info_global_cls_df.loc[index])
        dict_data['time'] = str(dict_data.get('publish_day'))+" "+str(dict_data.get("publish_time"))
        dict_data['data_type'] = 'cls'
        dict_data['publish_time'] = str(dict_data['publish_time'])
        dict_data['publish_day'] = str(dict_data['publish_day'])
        datas.append(UpdateOne(
            {"title": dict_data['title'], "data_type": dict_data['data_type'], "content": dict_data['content']},
            {"$set": dict_data},
            upsert=True))

    # 东方财富新闻
    col_mapping = {"标题": "title", "摘要": "content", "发布时间": "time",'链接':'url'}
    stock_info_global_em_df = ak.stock_info_global_em()
    stock_info_global_em_df.rename(columns=col_mapping, inplace=True)
    for index in stock_info_global_em_df.index:
        dict_data = dict(stock_info_global_em_df.loc[index])
        dict_data['time'] = str(dict_data.get("time"))
        dict_data['data_type'] = 'em'
        datas.append(UpdateOne(
            {"title": dict_data['title'], "data_type": dict_data['data_type'], "url": dict_data['url']},
            {"$set": dict_data},
            upsert=True))


    #新浪
    col_mapping = {"内容": "content", "时间": "time"}
    stock_info_global_sina_df = ak.stock_info_global_sina()
    stock_info_global_sina_df.rename(columns=col_mapping, inplace=True)
    for index in stock_info_global_sina_df.index:
        dict_data = dict(stock_info_global_sina_df.loc[index])
        dict_data['time'] = str(dict_data.get("time"))
        dict_data['data_type'] = 'sina'
        datas.append(UpdateOne(
            {"data_type": dict_data['data_type'], "content": dict_data['content']},
            {"$set": dict_data},
            upsert=True))



    # 富途牛牛
    col_mapping = {"标题": "title", "内容": "content", "发布时间": "time",'链接':'url'}
    stock_info_global_futu_df = ak.stock_info_global_futu()

    stock_info_global_futu_df.rename(columns=col_mapping, inplace=True)
    for index in stock_info_global_futu_df.index:
        dict_data = dict(stock_info_global_futu_df.loc[index])
        dict_data['time'] = str(dict_data.get("time"))
        dict_data['data_type'] = 'futu'
        datas.append(UpdateOne(
            {"title": dict_data['title'], "data_type": dict_data['data_type'], "url": dict_data['url']},
            {"$set": dict_data},
            upsert=True))

    # 同花顺
    col_mapping = {"标题": "title", "内容": "content", "发布时间": "time",'链接':'url'}
    stock_info_global_ths_df = ak.stock_info_global_ths()
    stock_info_global_ths_df.rename(columns=col_mapping, inplace=True)
    for index in stock_info_global_ths_df.index:
        dict_data = dict(stock_info_global_ths_df.loc[index])
        dict_data['time'] = str(dict_data.get("time"))
        dict_data['data_type'] = 'ths'
        datas.append(UpdateOne(
            {"title": dict_data['title'], "data_type": dict_data['data_type'], "url": dict_data['url']},
            {"$set": dict_data},
            upsert=True))
    if len(datas) > 0:
        mongo_bulk_write_data(news, datas)



if __name__ == '__main__':
    all_platform_news_data()
    schedule.every(30).minutes.do(all_platform_news_data)
    while True:
        schedule.run_pending()
        time.sleep(10)