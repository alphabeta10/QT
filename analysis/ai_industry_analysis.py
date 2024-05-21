from datetime import datetime, timedelta
import google.generativeai as genai
from big_models.google_api import *
from data.mongodb import get_mongo_table
import akshare as ak
from pymongo import UpdateOne
from tqdm import tqdm
from utils.tool import load_json_data
from utils.tool import mongo_bulk_write_data
from utils.actions import try_get_action
from openai import OpenAI
from big_models.kimi_model import get_result_from_kimi_model
import time


def get_data_from_mongo_db(dict_key_words=None, before_day=1):
    goods = get_mongo_table(database='stock', collection='news')

    date_time = datetime.now() - timedelta(days=before_day)
    today = date_time.strftime("%Y-%m-%d")
    if dict_key_words is None:
        dict_key_words = {"人工智能": 0, "5G通信": 0, "算力": 0, "半导体": 0}
    datas = []
    for ele in goods.find({"data_type": "cls_telegraph", "time": {"$gt": f"{today}"}}, projection={'_id': False}).sort(
            "time"):
        sentence = ele['content']
        time = ele['time']
        title = ele['title']
        for key in dict_key_words.keys():
            if key in sentence or key in title:
                datas.append({"发布时间": time, "新闻内容": sentence})
    if len(datas) > 0:
        pd_data = pd.DataFrame(datas)
        return pd_data
    return None


def get_data_from_real_data(dict_key_words=None):
    stock_telegraph_cls_df = try_get_action(ak.stock_telegraph_cls, try_count=3)
    datas = []
    if dict_key_words is None:
        dict_key_words = {"人工智能": 0, "5G通信": 0, "算力": 0, "半导体": 0}
    for index in stock_telegraph_cls_df.index:
        data = dict(stock_telegraph_cls_df.loc[index])
        pub_time = str(data['发布时间'])
        pub_day = str(data['发布日期'])
        content = data['内容']
        title = data['标题']
        for key in dict_key_words.keys():
            if key in content or key in title:
                datas.append({"发布时间": f"{pub_day} {pub_time}", "新闻内容": content})
    if len(datas) > 0:
        pd_data = pd.DataFrame(datas)
        return pd_data
    return None


def result_to_db(datas: list, model_indicator_col):
    update_request = []
    time_dict = {}
    for data in datas:
        kys = data.keys()
        content = data['content']
        if '主题' in kys:
            theme = data['主题']
        else:
            print(data)
            continue
        if '情感分类' in kys:
            sentiment = data['情感分类']
        else:
            print(data)
            continue

        if '涉及的国家' in kys:
            if isinstance(data['涉及的国家'], str):
                region = data['涉及的国家']
            elif isinstance(data['涉及的国家'], list):
                region = ",".join(data['涉及的国家'])
            else:
                print(f"涉及的国家出错，{content}")
                region = ''
        else:
            print(data)
            continue

        if '摘要' in kys:
            abstract = data['摘要']
        else:
            print(data)
            continue
        time = data['time']
        try:
            time_dict.setdefault(theme, {time})
        except Exception as e:
            print(e)
            print(theme, time)
        in_db_data = {"data_type": "big_model_sentiment", "time": time, "code": theme, "abstract": abstract,
                      "content": content, "sentiment": sentiment, "region": region}

        update_request.append(
            UpdateOne({"code": in_db_data['code'], 'time': in_db_data['time'], "content": in_db_data['content']},
                      {"$set": in_db_data},
                      upsert=True)
        )
    mongo_bulk_write_data(model_indicator_col, update_request)
    update_request.clear()


def big_model_handle_ai_news(dict_key_words=None, is_real=False, before_day=1, is_in_db=False):
    if dict_key_words is None:
        dict_key_words = {"人工智能": 0, "5G通信": 0, "算力": 0, "半导体": 0, "算法": 0}
    if is_real:
        pd_data = get_data_from_real_data(dict_key_words=dict_key_words)
    else:
        pd_data = get_data_from_mongo_db(dict_key_words=dict_key_words, before_day=before_day)
    common_ai_new_analysis(pd_data, is_in_db)


def common_kimi_ai_new_analysis(pd_data, is_in_db, pub_time_key='发布时间', pub_content_key='新闻内容', ret_key=None,
                                client=None, model=None, name=None, request_count_dict: dict = None):
    global model_indicator_col
    if name is None:
        name = 'default'
    if request_count_dict is None:
        print("请求统计为空！！！")
        request_count_dict = {"rc": 0}
    if pd_data is not None and len(pd_data) > 0:
        if client is None or model is None:
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
        result_data = []
        batch = 0
        batch_data = []
        contents = []
        if is_in_db:
            model_indicator_col = get_mongo_table(database='stock', collection="model_new_indicator")
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            content = dict_data[pub_content_key]
            batch_data.append(dict_data)
            contents.append(f"<article>{content}</article>")
            batch += 1
            if batch == 5:
                batch_content = "".join(contents)
                batch = 0
                history = [{"role": "system",
                            "content": "你将收到多段新闻用XML标签分割。首先概括新闻的摘要，给出情感分类为[积极，中性，悲观]，给出新闻涉及的国家，返回数据格式是json数组[{\"摘要\":\"新闻的摘要\",\"情感分类\":\"新闻的情感类别\",\"涉及的国家\":\"新闻涉及的国家\"}]"}]
                history += [{"role": "user", "content": batch_content}]
                ret_data = try_get_action(get_result_from_kimi_model, try_count=3, delay=65, client=client,
                                          history=history,
                                          model=model,
                                          is_ret_json=True)
                if ret_data is not None and len(ret_data) > 0:
                    for i, ret in enumerate(ret_data):
                        ret['time'] = batch_data[i][pub_time_key]
                        ret['content'] = batch_data[i][pub_content_key]
                        ret['主题'] = name
                        if ret_key is not None:
                            for key in ret_key:
                                ret[key] = batch_data[i][key]
                        result_data.append(ret)
                    if is_in_db and len(result_data) > 0:
                        result_to_db(result_data, model_indicator_col)
                        result_data.clear()
                batch_data.clear()
                contents.clear()
                request_count_dict['rc'] += 1
                if request_count_dict['rc'] >= 3:
                    print("等待下一次调度kimi模型")
                    time.sleep(65)
                    request_count_dict['rc'] = 0
        if batch > 0:
            batch_content = "".join(contents)
            history = [{"role": "system",
                        "content": "你将收到多段新闻用XML标签分割。首先概括新闻的摘要，给出情感分类为[积极，中性，悲观]，给出新闻涉及的国家，返回数据格式是json数组[{\"摘要\":\"新闻的摘要\",\"情感分类\":\"新闻的情感类别\",\"涉及的国家\":\"新闻涉及的国家\"}]"}]
            history += [{"role": "user", "content": batch_content}]
            ret_data = try_get_action(get_result_from_kimi_model, try_count=3, delay=65, client=client,
                                      history=history,
                                      model=model,
                                      is_ret_json=True)
            if isinstance(ret_data,dict):
                print("ret error ",ret_data)
                ret_data = [ret_data]
            if ret_data is not None and len(ret_data) > 0:
                for i, ret in enumerate(ret_data):
                    ret['time'] = batch_data[i][pub_time_key]
                    ret['content'] = batch_data[i][pub_content_key]
                    ret['主题'] = name
                    if ret_key is not None:
                        for key in ret_key:
                            ret[key] = batch_data[i][key]
                    result_data.append(ret)
                if is_in_db and len(result_data) > 0:
                    result_to_db(result_data, model_indicator_col)
            request_count_dict['rc'] += 1
            if request_count_dict['rc'] >= 3:
                print("等待下一次调度kimi模型")
                time.sleep(65)
                request_count_dict['rc'] = 0
        return result_data
    return None


def common_ai_new_analysis(pd_data, is_in_db, pub_time_key='发布时间', pub_content_key='新闻内容', ret_key=None,
                           model=None, themes=None):
    if pd_data is not None and len(pd_data) > 0:
        if model is None:
            api_key_json = load_json_data("google_api.json")
            api_key = api_key_json['api_key']
            version = api_key_json['version']
            genai.configure(api_key=api_key, transport='rest')
            for m in genai.list_models():
                if "generateContent" in m.supported_generation_methods:
                    print(m.name)
            model = genai.GenerativeModel(version)
        if themes is None:
            input_str_temp = """给定文本分析出该内容主题分类[人工智能，5G通信，半导体，算力，算法，人形机器人,其他]，分析其情感分类[积极，中性，悲观]，摘要提取，以及涉及的国家。文本：财联社2月3日电，据外媒报道，谷歌表示，Gemini Pro是谷歌最大的人工智能(AI)模型之一，作为巴德(Bard)的升级版，现已向欧洲用户开放。该模型是一个多模态大模型，这意味着它可以理解和组合不同类型的信息，如文本、代码、音频、图像和视频。通过Gemini，谷歌希望能与OpenAI的热门聊天机器人ChatGPT进行竞争。 谷歌最新人工智能模型Gemini Pro已在欧洲上市 将与ChatGPT竞争。\n输出：{"主题":"人工智能","情感分类":"积极","涉及的国家":["欧洲"],"摘要":"谷歌最新人工智能模型Gemini Pro已在欧洲上市，将与ChatGPT竞争。Gemini Pro是谷歌最大的人工智能模型之一，作为巴德(Bard)的升级版，现已向欧洲用户开放。该模型是一个多模态大模型，这意味着它可以理解和组合不同类型的信息，如文本、代码、音频、图像和视频。"}\n"""
        else:
            themes = "，".join(themes)
            input_str_temp = """给定文本分析出该内容主题分类[{themes}]，分析其情感分类[积极，中性，悲观]，摘要提取，以及涉及的国家。文本：财联社2月3日电，据外媒报道，谷歌表示，Gemini Pro是谷歌最大的人工智能(AI)模型之一，作为巴德(Bard)的升级版，现已向欧洲用户开放。该模型是一个多模态大模型，这意味着它可以理解和组合不同类型的信息，如文本、代码、音频、图像和视频。通过Gemini，谷歌希望能与OpenAI的热门聊天机器人ChatGPT进行竞争。 谷歌最新人工智能模型Gemini Pro已在欧洲上市 将与ChatGPT竞争。\n输出：{"主题":"人工智能","情感分类":"积极","涉及的国家":["欧洲"],"摘要":"谷歌最新人工智能模型Gemini Pro已在欧洲上市，将与ChatGPT竞争。Gemini Pro是谷歌最大的人工智能模型之一，作为巴德(Bard)的升级版，现已向欧洲用户开放。该模型是一个多模态大模型，这意味着它可以理解和组合不同类型的信息，如文本、代码、音频、图像和视频。"}\n"""
            input_str_temp = input_str_temp.replace("{themes}", themes)
        result_data = []
        for index in pd_data.index:
            dict_data = dict(pd_data.loc[index])
            time = dict_data[pub_time_key]
            content = dict_data[pub_content_key]
            request_txt = input_str_temp + "文本：" + content + "\n 输出："
            ret = try_get_action(simple_big_gen_model_fn, try_count=3, delay=50, model=model, request_txt=request_txt,
                                 is_ret_json=True)
            if ret is not None:
                ret['time'] = time
                ret['content'] = content
                if ret_key is not None:
                    for key in ret_key:
                        ret[key] = dict_data[key]
                result_data.append(ret)
        if is_in_db and len(result_data) > 0:
            model_indicator_col = get_mongo_table(database='stock', collection="model_new_indicator")
            result_to_db(result_data, model_indicator_col)
        return result_data
    return None
