import pandas as pd
import os
import re

import requests
from bs4 import BeautifulSoup
from utils.actions import show_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def extract_data(name, raw_line):
    data, same_data = None, None
    result = re.findall(name + "(\d+\.?\d?)亿元，同比增长(\d+\.?\d?)%", raw_line)
    if len(result) > 0:
        data, same_data = result[0]
        same_data = float(same_data)
    else:
        result = re.findall(name + "(\d+\.?\d?)亿元，与上年同期持平", raw_line)
        if len(result) > 0:
            data = result[0]
            same_data = 0.0
        result = re.findall(name + "(\d+\.?\d?)亿元，同比下降(\d+\.?\d?)%", raw_line)
        if len(result) > 0:
            data, same_data = result[0]
            same_data = float(same_data) * -1
    if data is None:
        result = re.findall(name + "(\d+\.?\d?)亿元", raw_line)
        if len(result) > 0:
            data = result[0]
            same_data = 0.0
    if data is not None:
        return data, same_data
    return None


def handel_date(txt):
    result = re.findall("(\d+)-(\d+)-(\d+)", txt)
    if len(result) > 0:
        return result[0][0] + result[0][1] + result[0][2]


def handle_content(txt, type, url,date_str):
    txt = txt.replace("\n", "").replace("  ", "")
    if type == 'in':
        f000k_index_price = re.findall("5000K0.8S指数为(\d+\.?\d+?)元/吨", txt)
        ff00k_index_price = re.findall("5500K0.8S指数为(\d+\.?\d+?)/吨", txt)
        if len(ff00k_index_price)==0:
            ff00k_index_price = re.findall("5500K0.8S指数为(\d+\.?\d+?)元/吨", txt)
        if len(ff00k_index_price) == 0 or len(f000k_index_price) == 0:
            print(url)
            print(txt, type,date_str)
            return None

        return {"5000K08s": ff00k_index_price[0], "5500K08s": f000k_index_price[0]}
    if type == 'out':
        ff00k_index_price = re.findall("5500K华东和华南到岸价均为(\d+\.?\d+?)美元/吨", txt)
        fof00k_index_price = re.findall("4500K华东和华南到岸价均为(\d+\.?\d+?)美元/吨", txt)
        tf00k_index_price = re.findall("3800K华东和华南到岸价为(\d+\.?\d+?)美元/吨", txt)
        if len(ff00k_index_price) == 0 or len(fof00k_index_price) == 0 or len(tf00k_index_price) == 0:
            print(url)
            print(txt, type,date_str)
            return
        return {f"{type}_5500K": ff00k_index_price[0], f"{type}_4500K": fof00k_index_price[0],
                f"{type}_3800K": tf00k_index_price[0]}
    pass


def craw_coal_data():
    update_request = []
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    for i in range(1):
        url = f'http://www.jgjcndrc.org.cn/list2.aspx?clmId=801&page={i}'
        print(f"{url}")
        respond = requests.get(url)
        html = respond.content
        html_doc = str(html, 'utf-8')
        soup = BeautifulSoup(html_doc, 'html.parser')
        uls = soup.find_all("ul", "list_02 clearfix")
        if uls is not None and len(uls) > 0:
            ul = uls[0]
            a_s = ul.find_all("a")
            if a_s is not None:
                for a_ in a_s:
                    detail_url = None
                    name = None
                    if '长江口动力煤' in a_.text:
                        detail_url = 'http://www.jgjcndrc.org.cn/' + a_['href']
                        name = 'in'
                    if '动力煤进口到岸' in a_.text:
                        detail_url = 'http://www.jgjcndrc.org.cn/' + a_['href']
                        name = 'out'
                    if detail_url is not None:
                        respond = requests.get(detail_url)
                        html = respond.content
                        html_doc = str(html, 'utf-8')
                        soup = BeautifulSoup(html_doc, 'html.parser')
                        divs = soup.find_all('div', id='zoom')
                        date_divs = soup.find_all("div", 'txt_subtitle1 tcenter')
                        if date_divs is not None and len(date_divs) > 0:
                            date_str = handel_date(date_divs[0].text)
                        else:
                            print("没有时间")
                            return
                        if divs is not None and len(divs) > 0:
                            dict_data = handle_content(divs[0].text, name, detail_url,date_str)
                            if dict_data is not None:
                                data_type = 'energy_data'
                                if name == 'in':
                                    metric_code = 'coal_index_price'
                                elif name == 'out':
                                    metric_code = 'coal_import_price'
                                else:
                                    return
                                dict_data['data_type'] = data_type
                                dict_data['metric_code'] = metric_code
                                dict_data['time'] = date_str
                                update_request.append(
                                    UpdateOne(
                                        {"data_type": dict_data['data_type'], "time": dict_data['time'],
                                         "metric_code": dict_data['metric_code']},
                                        {"$set": dict_data},
                                        upsert=True))
        else:
            print("no craw data")
    if len(update_request) > 0:
        mongo_bulk_write_data(stock_common, update_request)

def find_data():
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    for ele in news.find({"data_type": "energy_data", "metric_code": "coal_import_price"},
                         projection={"_id":False}).sort(
        "time"):
        datas.append(ele)
    data = pd.DataFrame(data=datas)
    show_data(data)
    data.set_index(keys=['time'], inplace=True)
    data['out_5500K'] = data['out_5500K'].astype(float)
    data['out_5500K'].plot(kind='bar', title='out_5500K', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


if __name__ == '__main__':
    craw_coal_data()
    find_data()
