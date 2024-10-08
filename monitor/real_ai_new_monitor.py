import os
import sys

# 可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from datetime import datetime, timedelta
import google.generativeai as genai

import pandas as pd
import schedule
import time
from data.comm_real_news_data import stock_telegraph_cls_news
from analysis.ai_industry_analysis import common_ai_new_analysis, common_kimi_ai_new_analysis
from utils.send_msg import MailSender
from utils.tool import load_json_data
from openai import OpenAI
from data.cn_grand_data import post_or_get_data


def lian_rmb_fx():
    html_str = ''
    try:
        wl_url = 'https://api-ddc-wscn.awtmt.com/market/kline?prod_code=USDCNH.OTC&tick_count=1&period_type=2592000&adjust_price_type=forward&fields=tick_at%2Copen_px%2Cclose_px%2Chigh_px%2Clow_px%2Cturnover_volume%2Cturnover_value%2Caverage_px%2Cpx_change%2Cpx_change_rate%2Cavg_px%2Cma2'
        x = post_or_get_data(wl_url, method='get')
        data = x['data']
        lines = data['candle']['USDCNH.OTC']['lines']
        fields = data['fields']
        kv_data = dict(zip(fields, lines[0]))
        kv_data['date'] = datetime.now().strftime("%Y-%m-%d")
        title = '人民币离岸汇率'
        html_str = f"<p>{title}</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr>"
        for k in kv_data.keys():
            html_str += f"<th>{k}</th>"
        html_str += "</tr>"

        html_str += "<tr>"
        for v in kv_data.values():
            html_str += f"<td>{v}</td>"
        html_str += "</tr>"
        html_str += "</table>"
    except Exception as e:
        print(e)
    return html_str


def unique_key_to_file(file_name: str, data_list: set):
    with open(file_name, mode='w') as f:
        for ele in data_list:
            f.write(ele + "\n")


def unique_key_load(file_name: str):
    with open(file_name, mode='r') as f:
        lines = f.readlines()
        keys = set([line.replace("\n", "") for line in lines if line.replace("\n", "") != ''])
        return keys


def filter_shenyishe_new_data(data_dict: dict, file_name, new_key, before_day=3):
    before_day_str = (datetime.now() - timedelta(days=before_day)).strftime("%Y%m%d")
    before_day = int(before_day_str)
    filter_datas = {}
    if os.path.exists(file_name) is False:
        unique_keys = set()
        for name, list_news in data_dict.items():
            new_list_news = []
            for new in list_news:
                day_str = new['time'][0:10].replace("-", "")
                day_int = int(day_str)
                if day_int >= before_day:
                    new_list_news.append(new)
                    key = name + new['time'] + new[new_key]
                    unique_keys.add(key)
            if len(new_list_news) > 0:
                filter_datas[name] = new_list_news
        if len(unique_keys) > 0:
            unique_key_to_file(file_name, unique_keys)
        return filter_datas
    else:
        unique_keys = unique_key_load(file_name)
        new_keys = set()
        is_has_new = False
        for name, list_news in data_dict.items():
            new_list_news = []
            for new in list_news:
                day_str = new['time'][0:10].replace("-", "")
                day_int = int(day_str)
                key = name + new['time'] + new[new_key]

                if day_int >= before_day:
                    new_list_news.append(new)
                    new_keys.add(key)
                    if key not in unique_keys:
                        is_has_new = True
            if len(new_list_news) > 0:
                filter_datas[name] = new_list_news
        if is_has_new:
            unique_key_to_file(file_name, new_keys)
            return filter_datas
        else:
            return {}


def load_names(filename):
    with open(filename, mode='r') as f:
        lines = f.readlines()
        keys = set([line.replace("\n", "") for line in lines if line.replace("\n", "") != ''])
        return list(keys)


def sort_by_last_time(dict_data: dict):
    list_data = []
    for k, v in dict_data.items():
        for ele in v:
            ele['data_type'] = k
            list_data.append(ele)
    list_data = sorted(list_data, key=lambda ele: ele['time'], reverse=True)
    new_dict_data = {}
    for ele in list_data:
        data_type = ele['data_type']
        new_dict_data.setdefault(data_type, [])
        new_dict_data[data_type].append(ele)
    return new_dict_data


def get_real_future_news_data():
    mail_msg = ""
    names = load_names("tele_keys.txt")
    tele_news = stock_telegraph_cls_news()
    filter_dict_data = {}
    for dict_data in tele_news:
        content = dict_data['content']
        for name in names:
            if name in content:
                if name not in filter_dict_data.keys():
                    filter_dict_data[name] = []
                filter_dict_data[name].append(dict_data)

    filter_dict_data = filter_shenyishe_new_data(filter_dict_data, 'tele_ai_new_data.txt', 'title')
    filter_dict_data = sort_by_last_time(filter_dict_data)
    if len(filter_dict_data.keys()) > 0:
        api_key_json = load_json_data("google_api.json")
        api_key = api_key_json['api_key']
        version = api_key_json['version']
        genai.configure(api_key=api_key, transport='rest')
        for m in genai.list_models():
            if "generateContent" in m.supported_generation_methods:
                print(m.name)
        model = genai.GenerativeModel(version)

        for name, list_new in filter_dict_data.items():
            mail_msg += f"<p>{name}财联社最新消息如下</p>"
            mail_msg += f"<table border=\"1\">"
            mail_msg += f"<tr> <th>时间</th> <th>标题</th><th>详细内容</th> <th>地区</th> <th>情感类别</th> </tr>"
            list_new.sort(key=lambda ele: ele['time'], reverse=True)
            pd_data = pd.DataFrame(list_new)
            ret_list = common_ai_new_analysis(pd_data, is_in_db=True, pub_content_key='content', pub_time_key='time',
                                              ret_key=['title'], model=model, themes=names, name=name)
            if ret_list is not None and len(ret_list) != 0:
                for new in ret_list:
                    kys = new.keys()
                    if '情感分类' in kys:
                        sentiment = new['情感分类']
                        if isinstance(sentiment, list):
                            sentiment = ",".join(sentiment)
                    else:
                        print(new, '解析出错')
                        sentiment = '解析出错'

                    if '涉及的国家' in kys:
                        region = ",".join(new['涉及的国家'])
                    else:
                        print(new, '解析出错')
                        region = '解析出错'

                    mail_msg += f"<tr> <td>{new['time']}</td> <td>{new['title']}</td> <td>{new['content']}</td>  <td>{region}</td> <td>{sentiment}</td></tr>"
                mail_msg += "</table>"
            else:
                for new in list_new:
                    kys = new.keys()
                    if '情感分类' in kys:
                        sentiment = new['情感分类']
                    else:
                        print(new, '解析出错')
                        sentiment = '解析出错'

                    if '涉及的国家' in kys:
                        region = ",".join(new['涉及的国家'])
                    else:
                        print(new, '解析出错')
                        region = '解析出错'

                    mail_msg += f"<tr> <td>{new['time']}</td> <td>{new['title']}</td> <td>{new['content']}</td>  <td>{region}</td> <td>{sentiment}</td></tr>"
                mail_msg += "</table>"
    return mail_msg


def get_real_future_news_data_kimi_model():
    mail_msg = ""
    names = load_names("tele_keys.txt")
    tele_news = stock_telegraph_cls_news()
    filter_dict_data = {}
    for dict_data in tele_news:
        content = dict_data['content']
        for name in names:
            if name in content:
                if name not in filter_dict_data.keys():
                    filter_dict_data[name] = []
                filter_dict_data[name].append(dict_data)

    filter_dict_data = filter_shenyishe_new_data(filter_dict_data, 'tele_ai_new_data.txt', 'title')
    filter_dict_data = sort_by_last_time(filter_dict_data)
    if len(filter_dict_data.keys()) > 0:
        request_count_dict = {"rc": 0}
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
        for name, list_new in filter_dict_data.items():
            mail_msg += f"<p>{name}财联社最新消息如下</p>"
            mail_msg += f"<table border=\"1\">"
            mail_msg += f"<tr> <th>时间</th> <th>标题</th><th>详细内容</th> <th>地区</th> <th>情感类别</th> </tr>"
            list_new.sort(key=lambda ele: ele['time'], reverse=True)
            pd_data = pd.DataFrame(list_new)
            ret_list = common_kimi_ai_new_analysis(pd_data, is_in_db=True, pub_content_key='content',
                                                   pub_time_key='time',
                                                   ret_key=['title'], client=client, model=model, name=name,
                                                   request_count_dict=request_count_dict)
            if ret_list is not None and len(ret_list) != 0:
                for new in ret_list:
                    kys = new.keys()
                    if '情感分类' in kys:
                        sentiment = new['情感分类']
                    else:
                        print(new, '解析出错')
                        sentiment = '解析出错'

                    if '涉及的国家' in kys:
                        if isinstance(new['涉及的国家'], str):
                            region = new['涉及的国家']
                        elif isinstance(new['涉及的国家'], list):
                            region = ",".join(new['涉及的国家'])
                        else:
                            print(new, '解析出错')
                            region = ''
                    else:
                        print(new, '解析出错')
                        region = '解析出错'

                    mail_msg += f"<tr> <td>{new['time']}</td> <td>{new['title']}</td> <td>{new['content']}</td>  <td>{region}</td> <td>{sentiment}</td></tr>"
                mail_msg += "</table>"
            else:
                for new in list_new:
                    kys = new.keys()
                    if '情感分类' in kys:
                        sentiment = new['情感分类']
                    else:
                        print(new, '解析出错')
                        sentiment = '解析出错'

                    if '涉及的国家' in kys:
                        region = ",".join(new['涉及的国家'])
                    else:
                        print(new, '解析出错')
                        region = '解析出错'

                    mail_msg += f"<tr> <td>{new['time']}</td> <td>{new['title']}</td> <td>{new['content']}</td>  <td>{region}</td> <td>{sentiment}</td></tr>"
                mail_msg += "</table>"
    return mail_msg


def main_sender():
    mail_msg = get_real_future_news_data()
    sender = MailSender()
    mail_msg += lian_rmb_fx()
    if mail_msg != '':
        try:
            sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], "AI行业数据监控",
                                  mail_msg)
            sender.close()
        except Exception as e:
            print(e)
            print(mail_msg)
    else:
        print("没有数据可发")


if __name__ == '__main__':
    main_sender()
    schedule.every(30).minutes.do(main_sender)
    while True:
        schedule.run_pending()
        time.sleep(10)
