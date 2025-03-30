import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime,timedelta
import time
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import jieba.analyse
import schedule


def get_futures_new_data():
    url = 'https://goodsfu.10jqka.com.cn/'
    respond = requests.get(url=url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})

    year = datetime.now().year
    before_year = year - 1
    month = datetime.now().month
    futures_news = get_mongo_table(database='futures', collection='futures_news')


    soup = BeautifulSoup(respond.text, 'html.parser')
    cl_items = soup.find_all('div', 'cl-item')
    all_datas = []
    for cl_item in cl_items:
        a_s = cl_item.find_all("a")
        for a in a_s:
            title = a['title']
            url = a['href']
            if title=='':
                if a.text is not None:
                    title = a.text.replace(" ","").replace("\n","")
            news_list = get_detail_news(url,year,before_year,month)
            print(f"handle {title} url={url}")
            if len(news_list)==0:
                print(f"handle {title} is no data {url}")
            for news in news_list:
                dict_data = {"content":news[1],"time":news[0],'data_type':'goodsfu',"metric_code":title}
                if news[2] is not None:
                    dict_data['follow_news'] = news[2]
                all_datas.append(UpdateOne(
                    {"content":dict_data['content'], "metric_code": dict_data['metric_code'],"data_type":dict_data["data_type"]},
                    {"$set": dict_data},
                    upsert=True))
            if len(all_datas) > 0:
                mongo_bulk_write_data(futures_news, all_datas)
                all_datas.clear()


def get_detail_news(url,cur_year,before_year,month):
    respond = requests.get(url=url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    soup = BeautifulSoup(respond.text, 'html.parser')
    list_con = soup.find_all('div', 'list-con')
    news_list = []
    if list_con is not None and len(list_con)>0:
        con = list_con[0]
        lis = con.find_all("li")
        for li in lis:
            a_s = li.find_all("a")
            spans = li.find_all("span")
            time = None
            if spans is not None and len(spans)>0:
                time = spans[0].find_all("span")[0].text
            main_news = None
            if len(a_s)>0:
                main_news = a_s[0].text.replace(" ","").replace("\n","")

            follow_news = None
            if len(a_s)==2:
                tmp_text = a_s[1].text.replace(" ", "").replace("\n", "")
                if tmp_text != '...':
                    follow_news = tmp_text

            if main_news is not None:
                final_time = datetime.now().strftime("%Y-%m-%d 00:00:00")
                if time is not None:
                    re_month = re.findall("(\d+)月(\d+)日 (\d+):(\d+)", time)
                    if len(re_month[0])==4:
                        data_month = re_month[0][0]
                        day = re_month[0][1]
                        day = day if len(day)==2 else "0"+str(day)
                        data_month = data_month if len(data_month)==2 else "0"+str(data_month)
                        minute = f"{re_month[0][2]}:{re_month[0][3]}:00"
                        if int(data_month)>month:
                            final_time = f"{before_year}-{data_month}-{day} {minute}"
                        else:
                            final_time = f"{cur_year}-{data_month}-{day} {minute}"
                news_list.append([final_time,main_news,follow_news])
    return news_list

def special_handle():
    url_dict = {"玉米淀粉":'http://stock.10jqka.com.cn/getListPage.php?listid=cl_008002036','焦煤':'http://stock.10jqka.com.cn/getListPage.php?listid=cl_008002019'}
    year = datetime.now().year
    before_year = year - 1
    month = datetime.now().month
    futures_news = get_mongo_table(database='futures', collection='futures_news')
    all_datas = []
    for title,url in url_dict.items():
        news_list = get_detail_news(url, year, before_year, month)
        title = ''
        print(f"handle {title} url={url}")
        if len(news_list) == 0:
            print(f"handle {title} is no data {url}")
        for news in news_list:
            dict_data = {"content": news[1], "time": news[0], 'data_type': 'goodsfu', "metric_code": title}
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

def enter_news_main():
    special_handle()
    get_futures_new_data()

if __name__ == '__main__':
    enter_news_main()
    schedule.every().hour.do(enter_news_main)
    while True:
        schedule.run_pending()
        time.sleep(10)
