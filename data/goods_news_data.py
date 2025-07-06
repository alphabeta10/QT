import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import requests
from bs4 import BeautifulSoup
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from datetime import datetime
from utils.tool import mongo_bulk_write_data
import warnings
warnings.filterwarnings('ignore')
import schedule
import time

global_url_dict = {}



def get_goods_new_detail_url():
    """
    获取商品价格数据
    :return:
    """
    url = 'http://www.100ppi.com/monitor/'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    list01_div = soup.find_all("div", 'list01')
    list02_div = soup.find_all("div", "list02")
    name_url_dict = {}
    for resource_div in list01_div:
        a_s = resource_div.find_all("a")
        for a in a_s:
            href = a['href']
            name = a.text.replace(' ', '').replace("\n", '')
            url = f'https://www.100ppi.com{href}'
            name_url_dict.setdefault(name, set())
            name_url_dict[name].add(url)
    for resource_div in list02_div:
        a_s = resource_div.find_all("a")
        for a in a_s:
            href = a['href']
            name = a.text.replace(' ', '').replace("\n", '')
            url = f'https://www.100ppi.com{href}'
            name_url_dict.setdefault(name, set())
            name_url_dict[name].add(url)

    for name, set_ in name_url_dict.items():
        if len(list(set_)) > 1:
            print(name, list(set_))

    news_url_dict = {}
    for name, url_set in name_url_dict.items():
        url = list(url_set)[0]
        respond = requests.get(url, headers={
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
        html = respond.content
        html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
        soup = BeautifulSoup(html_doc, 'html.parser')
        dives = soup.find_all("div", "zxbj-h")
        if dives and len(dives)>0:
            a_ = dives[0].find("a")
            url = 'https://www.100ppi.com'+a_['href']
            print(name,url)
            news_url_dict[name] = url
            global_url_dict[name] = url
    return news_url_dict

def get_new_detail_data(url):
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    divs = soup.find_all("div","m-left-box")
    year = datetime.now().year
    now_str = datetime.now().strftime("%Y-%m-%d")
    datas = []
    if divs and len(divs)>0:
        div = divs[0]
        detail_divs = div.find_all("div","dis-flex w-qb")
        for detail_div in detail_divs:
            ele_details = detail_div.find_all("div","mb-6 ml-2")
            if ele_details and len(ele_details)>0:
                time_divs = ele_details[0].find_all("span",'mr-2 qb-time')
                time_str = time_divs[0].text
                ps = ele_details[0].find_all("p")
                detail_text = ps[0].text
                splits = time_str.split(" ")
                if len(splits)==2:
                    minute_time,day = splits
                    day_time = f"{year}-{day} {minute_time}:00"
                else:
                    if len(time_str)==5:
                        day_time = f"{now_str} {time_str}:00"
                    else:
                        print(f"error in time {time_str}")
                        continue
                datas.append([day_time,detail_text,time_str])
    return datas


def main_goods_price_info():
    futures_news = get_mongo_table(database='futures', collection='futures_news')
    if len(list(global_url_dict.keys()))==0:
        print("enter get url")
        get_goods_new_detail_url()
    else:
        print("global url is not null")
    news_url_dict = global_url_dict
    all_datas = []
    for name,url in news_url_dict.items():
        datas = get_new_detail_data(url)
        for news in datas:

            dict_data = {"content": news[1], "time": news[0], 'data_type': 'goods_price_news', "metric_code": name}
            if news[2] is not None:
                dict_data['follow_news'] = news[2]
            all_datas.append(UpdateOne(
                {"content": dict_data['content'], "metric_code": dict_data['metric_code'],
                 "data_type": dict_data["data_type"]},
                {"$set": dict_data},
                upsert=True))
    if len(all_datas) > 0:
        mongo_bulk_write_data(futures_news, all_datas)
        all_datas.clear()



if __name__ == '__main__':
    main_goods_price_info()
    schedule.every(30).minutes.do(main_goods_price_info)
    while True:
        schedule.run_pending()
        time.sleep(10)

