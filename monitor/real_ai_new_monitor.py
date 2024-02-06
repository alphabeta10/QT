import os
from datetime import datetime, timedelta

import pandas as pd
import schedule
import time
from data.comm_real_news_data import stock_telegraph_cls_news
from analysis.ai_industry_analysis import common_ai_new_analysis
from utils.send_msg import MailSender


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

    for name, list_new in filter_dict_data.items():
        mail_msg += f"<p>{name}财联社最新消息如下</p>"
        mail_msg += f"<table>"
        mail_msg += f"<tr> <th>时间</th> <th>标题</th><th>详细内容</th> <th>地区</th> <th>情感类别</th> </tr>"
        list_new.sort(key=lambda ele:ele['time'],reverse=True)
        pd_data = pd.DataFrame(list_new)
        ret_list = common_ai_new_analysis(pd_data,is_in_db=True,pub_content_key='content',pub_time_key='time',ret_key=['title'])
        if ret_list is not None:
            for new in ret_list:
                kys = new.keys()
                if '情感分类' in kys:
                    sentiment = new['情感分类']
                else:
                    print(new, '解析出错')
                    sentiment = ''

                if '涉及的国家' in kys:
                    region = ",".join(new['涉及的国家'])
                else:
                    print(new,'解析出错')
                    region = ''

                mail_msg += f"<tr> <td>{new['time']}</td> <td>{new['title']}</td> <td>{new['content']}</td>  <td>{region}</td> <td>{sentiment}</td></tr>"
            mail_msg += "</table>"
        else:
            mail_msg += "无数据分析</table>"
    return mail_msg


def main_sender():
    mail_msg = get_real_future_news_data()
    sender = MailSender()
    if mail_msg != '':
        sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], "AI行业数据监控", mail_msg)
        sender.close()
    else:
        print("没有数据可发")



if __name__ == '__main__':
    main_sender()
    schedule.every(30).minutes.do(main_sender)
    while True:
        schedule.run_pending()
        time.sleep(10)
