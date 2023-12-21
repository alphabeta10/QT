import re
import akshare as ak
import requests
import pandas as pd
from datetime import datetime, timedelta
from utils.actions import try_get_action
from bs4 import BeautifulSoup
from selenium import webdriver
import time
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import jieba.analyse
import schedule

global_hour_before = "小时前"
en_global_hour_before = "hoursago"

global_day_before = "天前"
global_week_before = "周前"
global_month_before = "个月前"

google_search_url = "https://www.google.com/search?q={}&newwindow=1&biw=1306&bih=602&tbm=nws&oq={}&sclient=gws-wiz-news"


def get_stock_news(stock='000001'):
    stock_news_em_df = try_get_action(ak.stock_news_em, try_count=3, stock=stock)
    return stock_news_em_df


def get_js_stock_news(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S')):
    js_news_df = try_get_action(ak.js_news, try_count=3, timestamp=timestamp)
    return js_news_df


def get_stock_google_search_news(stock='云海金属'):
    url = google_search_url.format(stock, stock)
    option = webdriver.ChromeOptions()
    option.add_argument('headless')
    driver = webdriver.Chrome(executable_path='../chromedriver', options=option)
    driver.get(url)
    time.sleep(10)
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    search_div = soup.find("div", id='search')
    a_list = search_div.find_all("div")[0].find_all("a")
    data_list = []

    for a in a_list:
        divs = a.find_all("div")
        datas = []
        for index, div in enumerate(divs[-4:]):
            ele = div.text.replace("\n", "").replace(" ", "")
            if index == 3:
                timestr = None
                print(ele)
                if global_hour_before in ele or "hour" in ele:
                    hour = int(re.findall(r"\d+", ele)[0])
                    # hour = int(ele.replace(global_hour_before,""))
                    timestr = (datetime.now() - timedelta(hours=hour)).strftime("%Y-%m-%d %H:%M:%S")
                if global_day_before in ele or "day" in ele:
                    day = int(re.findall(r"\d+", ele)[0])
                    timestr = (datetime.now() - timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S")
                if global_week_before in ele or "week" in ele:
                    week = int(re.findall(r"\d+", ele)[0])
                    timestr = (datetime.now() - timedelta(weeks=week)).strftime("%Y-%m-%d %H:%M:%S")
                if global_month_before in ele or "month" in ele:
                    month = int(re.findall(r"\d+", ele)[0]) * 30
                    timestr = (datetime.now() - timedelta(days=month)).strftime("%Y-%m-%d %H:%M:%S")
                if timestr is None:
                    timestr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                datas.append(timestr)
            else:
                datas.append(ele)
        datas.append(stock)
        data_list.append(datas)

    driver.close()
    return data_list


def get_data_from_google(key_words=None):
    if key_words is None:
        key_words = ['全球经济', '美国经济', '中国经济', '中央银行政策', '赣锋锂业', '中矿资源', '华友钴业', '天赐材料', '杉杉股份', '隆基绿能', '晶澳科技', '五矿稀土',
                     '北方稀土', '中国核电', '比亚迪', '潍柴动力', '科大讯飞', '平安银行', '双塔食品', '湖南发展', '阳光能源', '中天科技']
    news = get_mongo_table(database='stock', collection='news')
    all_datas = []
    for tic in key_words:
        datas = try_get_action(get_stock_google_search_news,try_count=3,stock=tic)
        for data in datas:
            dict_data = {"source":data[0],"title":data[1],"content":data[2],"time":data[3],"symbol":data[4],"data_type":"stock_news"}
            all_datas.append(UpdateOne(
                {"title": dict_data['title'], "data_type": dict_data['data_type']},
                {"$set": dict_data},
                upsert=True))
    if len(all_datas) > 0:
        mongo_bulk_write_data(news, all_datas)


def get_html_xitu():
    day = datetime.now().strftime("%Y-%m-%d")
    url = "http://www.cre-ol.com/zhxx"
    head_url = "http://www.cre-ol.com"
    response = requests.get(url)
    response.encoding = 'utf-8'
    soup = BeautifulSoup(response.text, 'html.parser')
    div = soup.find("div",id="view_list_10018_271033775") #view_list_10018_271033775
    uls = div.find_all("ul")
    new_data_list = []
    if uls is not None:
        lis = uls[0].find_all("li")
        for li in lis:
            spans = li.find_all("span")
            if spans is not None and len(spans)>0:
                date = spans[0].text.replace("\n","").replace(" ","").replace("\r","")
                if len(date)>=10:
                    a_s = li.find_all("a")
                    if a_s is not None and len(a_s)>0:
                        a = a_s[0]
                        href = a.get("href")
                        header = a.text.replace("\n","").replace(" ","")
                        print(date,href,header)
                        new_data_list.append([date,f"{head_url}{href}",header])
    if len(new_data_list)>0:
        pd.DataFrame(data=new_data_list,columns=['time','href','header']).to_csv(f"{day}_xitu.csv")



def stock_telegraph_cls_news():
    stock_telegraph_cls_df = try_get_action(ak.stock_telegraph_cls,try_count=3)
    datas = []
    news = get_mongo_table(database='stock', collection='news')
    for index in stock_telegraph_cls_df.index:
        data = dict(stock_telegraph_cls_df.loc[index])
        pub_time = str(data['发布时间'])
        pub_day = str(data['发布日期'])
        title = data['标题']
        content = data['内容']
        dict_data = {"time":f"{pub_day} {pub_time}","day":pub_day,"title":title,"content":content,"data_type":"cls_telegraph"}
        datas.append(UpdateOne(
            {"title": dict_data['title'], "data_type": dict_data['data_type'],"content":content},
            {"$set": dict_data},
            upsert=True))
    if len(datas) > 0:
        mongo_bulk_write_data(news, datas)



def find_data():
    goods = get_mongo_table(database='stock', collection='news')
    datas = {}
    today = datetime.now().strftime("%Y-%m-%d")
    key_words = ['机器人','算力','人工智能','通信']
    for ele in goods.find({"data_type":"cls_telegraph","time":{"$gt":f"{today}"}},projection={'_id': False}).sort("time"):
        sentence  = ele['content']
        if '营业收入' not in sentence:
            time = ele['time']
            print("*" * 50)
            print(sentence)

            split_sentence = sentence.split("】")
            if len(split_sentence)>1:
                sentence = split_sentence[1]
            key_words_top = jieba.analyse.textrank(sentence, topK=10, withWeight=False)
            print(time+"="+"/".join(key_words_top))
            for word in key_words_top:
                if word not in datas.keys():
                    datas[word] = 0
                datas[word] += 1
            print("*"*50)
    print(sorted(datas.items(),key=lambda x:(x[1],x[0]),reverse=True))





if __name__ == '__main__':
    stock_telegraph_cls_news()
    schedule.every().hour.do(stock_telegraph_cls_news)
    while True:
        schedule.run_pending()
        time.sleep(10)