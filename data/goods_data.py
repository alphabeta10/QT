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
import pandas as pd
import matplotlib.pyplot as plt
#设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')


def handle_day(date_str,c_month,c_year,before_year):
    month = date_str.split("月")[0]
    day = date_str.split("月")[1].replace("日", "")
    if int(c_month)>=int(month):
        date1 = f"{c_year}{month}{day}"
    if int(c_month)<int(month):
        date1 = f"{before_year}{month}{day}"
    return date1


def get_all_monitor_price_data():
    """
    获取商品价格数据
    :return:
    """
    goods = get_mongo_table(database='stock', collection='goods')
    url = 'https://www.100ppi.com/monitor2/'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    search_div = soup.find_all("div", 'right fl')

    headers = search_div[0]
    tables = headers.find_all('table')
    year = datetime.now().strftime("%Y")
    before_year = str(int(year)-1)
    month = datetime.now().strftime("%m")
    datas = []
    goods_name_dict = {}
    for table in tables:
        trs = table.find_all('tr')
        headtds = trs[0].find_all('td')
        date1 = headtds[2].text.replace(' ','')
        date1 = handle_day(date1,month,year,before_year)

        date2 = headtds[3].text.replace(' ','')
        date2 = handle_day(date2, month, year, before_year)

        date3 = headtds[4].text.replace(' ','')
        date3 = handle_day(date3, month, year, before_year)
        goods_name = None
        for tr in trs[1:]:

            tds = tr.find_all("td")
            if len(tds)==5:
                name = tds[0].text.replace(' ','').replace("\n",'')
                metric = tds[1].text.replace(' ','').replace("\n",'')
                va1 = tds[2].text.replace(' ','').replace("\n",'')
                va2 = tds[3].text.replace(' ','').replace("\n",'')
                va3 = tds[4].text.replace(' ','').replace("\n",'')
                dict_data = {"name":name,"metric":metric,"time":date1,"value":va1,"data_type":"goods_price"}
                datas.append(UpdateOne(
                    {"name": dict_data['name'], "time": dict_data['time'], "data_type": dict_data['data_type']},
                    {"$set": dict_data},
                    upsert=True))
                dict_data = {"name": name, "metric": metric, "time": date2, "value": va2,"data_type":"goods_price"}
                datas.append(UpdateOne(
                    {"name": dict_data['name'], "time": dict_data['time'], "data_type": dict_data['data_type']},
                    {"$set": dict_data},
                    upsert=True))
                dict_data = {"name": name, "metric": metric, "time": date3, "value": va3,"data_type":"goods_price"}
                datas.append(UpdateOne(
                {"name": dict_data['name'],"time":dict_data['time'],"data_type":dict_data['data_type']},
                {"$set": dict_data},
                upsert=True))
                if goods_name is None:
                    goods_name = ''
                goods_name_dict[goods_name].append(name)
            elif len(tds)==1:
                goods_name = tds[0].text.replace(' ', '').replace("\n", '')
                goods_name_dict[goods_name] = []
    goods_name_dict['name'] = "goods_meta"
    goods_name_dict['time'] = "29990101"
    goods_name_dict['data_type'] = "goods_class"
    datas.append(UpdateOne(
        {"name": goods_name_dict['name'], "time": goods_name_dict['time'], "data_type": goods_name_dict['data_type']},
        {"$set": goods_name_dict},
        upsert=True))
    if len(datas) > 0:
        mongo_bulk_write_data(goods, datas)


def get_all_monitor_price_data02():
    """
    获取商品价格数据
    :return:
    """
    goods = get_mongo_table(database='stock', collection='goods')
    url = 'http://www.100ppi.com/monitor/'
    respond = requests.get(url, headers={
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
        "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
    html = respond.content
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    list01_div = soup.find_all("div", 'list01')
    titbk_div = soup.find_all("div", "titbk")
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

    datas = []
    year = datetime.now().strftime("%Y")
    for name, url_set in name_url_dict.items():
        url = list(url_set)[0]
        respond = requests.get(url, headers={
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
        html = respond.content
        html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
        soup = BeautifulSoup(html_doc, 'html.parser')
        price_spans = soup.find_all("span", "price-fb01_1")
        time_li = soup.find_all("li", "post_date_li")
        if price_spans and time_li and len(price_spans)>0 and len(time_li)>0:
            price = price_spans[0].text.replace(' ', '').replace("\n", '')
            time = time_li[0].text.strip().replace("\n", '').replace("更新时间：", "").replace("-", "")[0:4]
            date1 = f"{year}{time}"
            print(name + "," + price + "," + date1)

            dict_data = {"name": name, "time": date1, "value": price, "data_type": "goods_price"}
            datas.append(UpdateOne(
                {"name": dict_data['name'], "time": dict_data['time'], "data_type": dict_data['data_type']},
                {"$set": dict_data},
                upsert=True))
        else:
            print(f'----{name} not found data----')
            print(url)
            print(f'----{name} not found data---')
    if len(datas) > 0:
        mongo_bulk_write_data(goods, datas)

def find_data():
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    goods_name = '轻质纯碱'
    for ele in goods.find({"name":goods_name,"data_type":"goods_price"},projection={'_id': False}).sort("time"):
        datas.append(ele)
        print(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data[['time','value']]
    data[['value']] = data[['value']].astype(float)
    data.set_index(keys=['time'],inplace=True)
    data[['value']].plot(kind='line', title=goods_name, rot=45, figsize=(15, 8), fontsize=10)
    plt.show()

def back_data():
    """
    备份数据
    :return:
    """
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    for ele in goods.find({"data_type": "goods_price"}, projection={'_id': False}).sort("time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    now_str = datetime.now().strftime("%Y%m%d")
    pd_data.to_csv(f"goods_data_{now_str}.csv",index=False)
def create_index():
    goods = get_mongo_table(database='stock', collection='goods')
    goods.create_index([("name", 1), ("time", 1),("data_type",1)],unique=True,background=True)


if __name__ == '__main__':
    get_all_monitor_price_data()
    find_data()
    back_data()