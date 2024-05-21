import akshare as ak
from datetime import datetime, timedelta
import google.generativeai as genai
from big_models.google_api import *
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from tqdm import tqdm
from utils.tool import load_json_data, comm_read_stock
from utils.tool import mongo_bulk_write_data
from utils.actions import try_get_action
from openai import OpenAI
from big_models.kimi_model import get_result_from_kimi_model


def big_model_stock_news_data(big_model_col, model, symbol='002527'):
    stock_news_em_df = try_get_action(ak.stock_news_em, try_count=3, symbol=symbol)
    if stock_news_em_df is not None and len(stock_news_em_df) > 0:
        data = stock_news_em_df[['发布时间', '新闻内容']]
        before_day_str = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
        data = data[data['发布时间'] > before_day_str]

        temp_list = []
        update_request = []
        contain_keys = ['情感类别', '摘要', '时间']
        for index in data.index:
            dict_ele = dict(data.loc[index])
            temp_list.append(dict_ele)
            if len(temp_list) > 5:
                temp_df = pd.DataFrame(temp_list)
                ret = try_get_action(comm_google_big_gen_model, try_count=3, data_df=temp_df, model=model,
                                     contain_keys=contain_keys)
                if ret is not None:
                    for ret_ele in ret:
                        kys = ret_ele.keys()
                        if '情感类别' in kys and '摘要' in kys and '时间' in kys:
                            new_dict = {"data_type": "news", "abstract": ret_ele['摘要'],
                                        "sentiment": ret_ele['情感类别'],
                                        "time": ret_ele['时间'], "code": symbol}
                            update_request.append(
                                UpdateOne({"code": symbol, 'time': new_dict['time'], "data_type": new_dict['data_type'],
                                           "abstract": new_dict['abstract']},
                                          {"$set": new_dict},
                                          upsert=True)
                            )
                        else:
                            print("返回的json数据有问题:", ret)
                else:
                    print(f"no batch data to db {symbol}")
                mongo_bulk_write_data(big_model_col, update_request)
                update_request.clear()
                temp_list.clear()
        if len(temp_list) > 0:
            temp_df = pd.DataFrame(temp_list)
            ret = try_get_action(comm_google_big_gen_model, try_count=3, data_df=temp_df, model=model,
                                 contain_keys=contain_keys)
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
            else:
                print(f"no batch data to db {symbol}")


def big_model_detail_stock_news_data(big_model_col, model, symbol='002527', before_day_str=None):
    stock_news_em_df = try_get_action(ak.stock_news_em, try_count=3, symbol=symbol)
    update_request = []
    temp_input_str = '给定新闻内容，分析新闻分类以及该新闻对公司股价情感分类[悲观，中性，积极]。\n输入：天眼查经营风险信息显示，近日，济南爱尔眼科医院有限公司因未取得医疗机构执业许可证擅自执业，违反了基本医疗卫生与健康促进法，被济南市市中区卫生健康局罚款5万元。\n输出：```{"新闻分类":"医疗卫生","情感分类":"悲观"}```\n输入：'
    if stock_news_em_df is not None and len(stock_news_em_df) > 0:
        data = stock_news_em_df[['发布时间', '新闻内容', '新闻标题']]
        if before_day_str is not None:
            data = data[data['发布时间'] > before_day_str]
        temp_list = []
        for index in data.index:
            dict_ele = dict(data.loc[index])
            news_content = dict_ele['新闻内容']
            request_txt = temp_input_str + news_content + "\n输出："
            ret_data = try_get_action(simple_big_gen_model_fn, try_count=3,delay=70, model=model, request_txt=request_txt,
                                      is_ret_json=True)
            if ret_data is not None and ('新闻分类' not in ret_data.keys() or '情感分类' not in ret_data.keys()):
                print(f"ret data is illegal try again for {symbol}")
                ret_data = try_get_action(simple_big_gen_model_fn, try_count=3,delay=70, model=model, request_txt=request_txt,
                                          is_ret_json=True)
            if ret_data is not None and '新闻分类' in ret_data.keys() and '情感分类' in ret_data.keys():
                new_dict = {"sentiment": ret_data['情感分类']}
                new_dict['code'] = symbol
                new_dict['data_type'] = 'detail_news'
                new_dict['time'] = str(dict_ele['发布时间'])
                new_dict['content'] = news_content
                new_dict['header'] = dict_ele['新闻标题']
                new_dict['news_type'] = ret_data['新闻分类']
                update_request.append(
                    UpdateOne({"code": symbol, 'time': new_dict['time'], "data_type": new_dict['data_type']},
                              {"$set": new_dict},
                              upsert=True)
                )
            else:
                print(f"处理{symbol}数据出错:\n{news_content} \nret-data:{ret_data}")
            if len(update_request) > 10:
                mongo_bulk_write_data(big_model_col, update_request)
                update_request.clear()
                temp_list.clear()
    if len(update_request) > 0:
        mongo_bulk_write_data(big_model_col, update_request)
        update_request.clear()


def handle_stock_news_abstract_sentiment(code_dict=None):
    if code_dict is None:
        code_dict = comm_read_stock('../stock.txt')
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    version = api_key_json['version']
    genai.configure(api_key=api_key, transport='rest')
    for m in genai.list_models():
        if "generateContent" in m.supported_generation_methods:
            print(m.name)
    model = genai.GenerativeModel(version)

    for code, name in tqdm(code_dict.items()):
        print(f"handle code={code},name={name}")
        big_model_stock_news_data(big_model_col, model, code)


def handle_detail_stock_news_abstract_sentiment(code_dict=None):
    if code_dict is None:
        code_dict = comm_read_stock('../stock.txt')
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    version = api_key_json['version']
    genai.configure(api_key=api_key, transport='rest')
    for m in genai.list_models():
        if "generateContent" in m.supported_generation_methods:
            print(m.name)
    model = genai.GenerativeModel(version)
    before_day_str = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")

    for code, name in tqdm(code_dict.items()):
        print(f"handle code={code},name={name}")
        big_model_detail_stock_news_data(big_model_col, model, code, before_day_str=before_day_str)


def big_kimi_model_detail_stock_news_data(big_model_col, client, model, symbol='002527', before_day_str=None):
    stock_news_em_df = try_get_action(ak.stock_news_em, try_count=3, symbol=symbol)
    update_request = []
    if stock_news_em_df is not None and len(stock_news_em_df) > 0:
        data = stock_news_em_df[['发布时间', '新闻内容', '新闻标题']]
        if before_day_str is not None:
            data = data[data['发布时间'] > before_day_str]
        temp_list = []
        for index in data.index:
            dict_ele = dict(data.loc[index])
            news_content = dict_ele['新闻内容']
            history = [{"role": "system",
                        "content": "你将收到一段新闻用XML标签分割。首先概括新闻的摘要，给出情感分类为[积极，中性，悲观]，给出新闻类别,返回数据格式是{\"摘要\":\"新闻的摘要\",\"情感类别\":\"新闻的情感类别\",\"新闻类别\":\"新闻的类别\"}"}]
            history += [{"role": "user", "content": f"<article>{news_content}</article>"}]

            ret_data = try_get_action(get_result_from_kimi_model, try_count=3, client=client, history=history,
                                      model=model,
                                      is_ret_json=True)
            if ret_data is not None and (
                    '新闻类别' not in ret_data.keys() or '情感类别' not in ret_data.keys() or '摘要' not in ret_data.keys()):
                print(f"ret data is illegal try again for {symbol}")
            if ret_data is not None and '情感类别' in ret_data.keys() and '新闻类别' in ret_data.keys() and '摘要' in ret_data.keys():
                new_dict = {"sentiment": ret_data['情感类别']}
                new_dict['code'] = symbol
                new_dict['data_type'] = 'detail_news'
                new_dict['time'] = str(dict_ele['发布时间'])
                new_dict['content'] = news_content
                new_dict['header'] = ret_data['摘要']
                new_dict['news_type'] = ret_data['新闻类别']
                update_request.append(
                    UpdateOne({"code": symbol, 'time': new_dict['time'], "data_type": new_dict['data_type']},
                              {"$set": new_dict},
                              upsert=True)
                )
            else:
                print(f"处理{symbol}数据出错:\n{news_content} \nret-data:{ret_data}")
            if len(update_request) > 10:
                mongo_bulk_write_data(big_model_col, update_request)
                update_request.clear()
                temp_list.clear()
    if len(update_request) > 0:
        mongo_bulk_write_data(big_model_col, update_request)
        update_request.clear()


def handle_detail_stock_news_abstract_sentiment_by_kimi_model(code_dict=None):
    if code_dict is None:
        code_dict = comm_read_stock('../stock.txt')
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    model_config = load_json_data('kimi_api_key.json')
    api_key = model_config['api_key']
    base_url = model_config['base_url']
    model = model_config['model']

    client = OpenAI(
        api_key=api_key,
        base_url=base_url,
    )

    model_list = client.models.list()
    model_data = model_list.data
    for i, mode in enumerate(model_data):
        print(f"model[{i}]:", mode.id)

    before_day_str = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")

    for code, name in tqdm(code_dict.items()):
        print(f"handle code={code},name={name}")
        big_kimi_model_detail_stock_news_data(big_model_col, client, model, code, before_day_str=before_day_str)


if __name__ == '__main__':
    handle_detail_stock_news_abstract_sentiment()
