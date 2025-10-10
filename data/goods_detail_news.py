import os
import sys
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from bs4 import BeautifulSoup
import requests
from datetime import datetime
from utils.actions import try_get_action
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data



def get_news_data(names =None):
    # 从数据库中获取新闻数据
    # 这里简单返回一个字符串列表
    url = 'https://www.100ppi.com/monitor/'
    root_url = 'https://www.100ppi.com'
    name_url_mapping = {}
    futures_news = get_mongo_table(database='futures', collection='futures_news')

    response = try_get_action(requests.get,try_count=3,url=url,headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'})
    if response is not None and response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        item_list = soup.find_all('div', class_='list01')
        if item_list:
            for item in item_list:
                a_s = item.find_all('a')
                if a_s:
                    for a_ in a_s:
                        name_url_mapping[a_.text] = root_url + a_['href']
        
        item_list = soup.find_all('div', class_='list02')
        if item_list:
            for item in item_list:
                a_s = item.find_all('a')
                if a_s:
                    for a_ in a_s:
                        name_url_mapping[a_.text] = root_url + a_['href']
    news_data = []
    for name,url in name_url_mapping.items():
        if names and name not in names:
            continue
        news = try_get_action(get_detail_data,try_count=3,url=url,name=name)
        if len(news) > 0:
            mongo_bulk_write_data(futures_news, news)



def get_detail_data(url,name):
    root_url = 'https://www.100ppi.com'
    cur_month = datetime.now().month
    cur_year = datetime.now().year
    cur_day = datetime.now().strftime("%Y-%m-%d")
    before_year = cur_year - 1
    news = []
    response = requests.get(url,headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'})
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        item = soup.find('a', class_='price-new_more')
        if item:
            detail_url = root_url + item['href']
            response = requests.get(detail_url,headers={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'})
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                mb_6_divs = soup.find_all('div', class_='mb-6 ml-2')
                if mb_6_divs:
                    for ele in mb_6_divs:
                        text = ele.find('p').text
                        time = ele.find('span').text.replace("\u2002"," ")
                        split_time = time.split(" ")
                        if len(split_time) == 2:
                            month_day = split_time[1]
                            data_month = month_day.split("-")[0]
                            if int(data_month) > cur_month:
                                year = before_year
                            else:
                                year = cur_year
                            dataday = month_day.split("-")[1]
                            date_str = f"{year}-{data_month}-{dataday} {split_time[0][0:5]}:00"
                            dict_data = {"content": text, "time": date_str, 'data_type': 'goods_intelligence_news', "metric_code": name}

                        else:
                            if ":" in time and '-' not in time:
                                date_str = f"{cur_day} {time[0:5]}:00"
                            else:
                                print("时间格式错误",time)
                                date_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            dict_data = {"content": text, "time": date_str, 'data_type': 'goods_intelligence_news', "metric_code": name}
                        print(dict_data)
                        news.append(UpdateOne(
                            {"content": dict_data['content'], "metric_code": dict_data['metric_code'],
                             "data_type": dict_data["data_type"]},
                            {"$set": dict_data},
                            upsert=True))
                else:
                    print(f'{name}未找到 {url}')
            else:
                print(f'{name}未找到 {url}')
        else:
            print(f'{name}未找到 {url}')
    else:
        print(f'{name}未找到 {url}')
    return news


    
if __name__ == '__main__':
    names = ['炼焦煤', '焦炭']
    get_news_data()
