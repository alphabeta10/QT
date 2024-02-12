import akshare as ak
from datetime import datetime, timedelta
import google.generativeai as genai
from big_models.google_api import *
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from tqdm import tqdm
from utils.tool import load_json_data
from utils.tool import mongo_bulk_write_data
from utils.actions import try_get_action


def big_model_stock_news_data(big_model_col,model,symbol='002527'):
    stock_news_em_df = try_get_action(ak.stock_news_em, try_count=3, symbol=symbol)
    if stock_news_em_df is not None and len(stock_news_em_df) > 0:
        data = stock_news_em_df[['发布时间', '新闻内容']]
        before_day_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        data = data[data['发布时间'] > before_day_str]

        temp_list = []
        update_request = []
        contain_keys = ['情感类别','摘要','时间']
        for index in data.index:
            dict_ele = dict(data.loc[index])
            temp_list.append(dict_ele)
            if len(temp_list) == 10:
                temp_df = pd.DataFrame(temp_list)
                ret = try_get_action(comm_google_big_gen_model, try_count=3, data_df=temp_df, model=model,contain_keys=contain_keys)
                if ret is not None:
                    for ret_ele in ret:
                        kys = ret_ele.keys()
                        if '情感类别' in kys and '摘要' in kys and '时间' in kys:
                            new_dict = {"data_type": "news", "abstract": ret_ele['摘要'], "sentiment": ret_ele['情感类别'],
                                        "time": ret_ele['时间'], "code": symbol}
                            update_request.append(
                                UpdateOne({"code": symbol, 'time': new_dict['time'], "data_type": new_dict['data_type'],
                                           "abstract": new_dict['abstract']},
                                          {"$set": new_dict},
                                          upsert=True)
                            )
                        else:
                            print("返回的json数据有问题:",ret)
                mongo_bulk_write_data(big_model_col, update_request)
                update_request.clear()
                temp_list.clear()
        if len(temp_list) > 0:
            temp_df = pd.DataFrame(temp_list)
            ret = try_get_action(comm_google_big_gen_model, try_count=3, data_df=temp_df, model=model,contain_keys=contain_keys)
            if ret is not None:
                for ret_ele in ret:
                    kys = ret_ele.keys()
                    if '情感类别' in kys and '摘要' in kys and '时间' in kys:
                        new_dict = {"data_type": "news", "abstract": ret_ele['摘要'], "sentiment": ret_ele['情感类别'],
                                    "time": ret_ele['时间'], "code": symbol}
                        update_request.append(
                            UpdateOne({"code": symbol, 'time': new_dict['time'], "data_type": new_dict['data_type']},
                                      {"$set": new_dict},
                                      upsert=True)
                        )
                    else:
                        print("返回的json数据有问题:", ret)
                mongo_bulk_write_data(big_model_col, update_request)
                update_request.clear()

def handle_stock_news_abstract_sentiment(code_dict=None):
    if code_dict is None:
        code_dict = {
            # 半导体
            "002409": "雅克科技",
            # 电力
            "002015": "协鑫能科",
            # 游戏
            "002555": "三七互娱",
            "002602": "世纪华通",
            "603444": "吉比特",
            # 通讯
            "000063": "中兴通讯",
            "600522": "中天科技",
            # 白酒
            "000858": "五粮液",
            "600519": "贵州茅台",
            # 机器人
            "002472": "双环传动",
            "002527": "新时达",
            # 银行
            "600036": "招商银行",
            "600919": "江苏银行",
            # AI相关
            "300474": "景嘉微",
            "002230": "科大讯飞",
            "603019": "中科曙光",
            "000977": "浪潮信息",
            # 新能源
            "300750": "宁德时代",
            "002594": "比亚迪",
            # 零食
            "300783": "三只松鼠",
            "603719": "良品铺子",
            # 啤酒
            "600132": "重庆啤酒",
            "600600": "青岛啤酒",

        }
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    genai.configure(api_key=api_key, transport='rest')
    for m in genai.list_models():
        if "generateContent" in m.supported_generation_methods:
            print(m.name)
    model = genai.GenerativeModel('gemini-pro')

    for code, name in tqdm(code_dict.items()):
        print(f"handle code={code},name={name}")
        big_model_stock_news_data(big_model_col,model,code)





if __name__ == '__main__':
    handle_stock_news_abstract_sentiment()
