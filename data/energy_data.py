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
from datetime import datetime

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


def handle_content(txt, type, url, date_str):
    txt = txt.replace("\n", "").replace("  ", "")
    if type == 'in':
        f000k_index_price = re.findall("5000K0.8S指数为(\d+\.?\d+?)元/吨", txt)
        ff00k_index_price = re.findall("5500K0.8S指数为(\d+\.?\d+?)/吨", txt)
        if len(ff00k_index_price) == 0:
            ff00k_index_price = re.findall("5500K0.8S指数为(\d+\.?\d+?)元/吨", txt)
        if len(ff00k_index_price) == 0 or len(f000k_index_price) == 0:
            print(url)
            print(txt, type, date_str)
            return None

        return {"5000K08s": ff00k_index_price[0], "5500K08s": f000k_index_price[0]}
    if type == 'out':
        ff00k_index_price = re.findall("5500K华东和华南到岸价均为(\d+\.?\d+?)美元/吨", txt)
        fof00k_index_price = re.findall("4500K华东和华南到岸价均为(\d+\.?\d+?)美元/吨", txt)
        tf00k_index_price = re.findall("3800K华东和华南到岸价为(\d+\.?\d+?)美元/吨", txt)
        if len(ff00k_index_price) == 0 or len(fof00k_index_price) == 0 or len(tf00k_index_price) == 0:
            print(url)
            print(txt, type, date_str)
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
                            dict_data = handle_content(divs[0].text, name, detail_url, date_str)
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


def handle_electricity_consume(year=None):
    """
    中国每月用电量数据
    :param year:
    :return:
    """
    def inner_extract_data(re_list, line_data):
        for i, key in enumerate(re_list):
            ex_re = re.findall(key, line_data)
            if len(ex_re) > 0:
                return i, ex_re
            else:
                print(f"{key} extract is None please check {line_data}")
        return None

    def inner_handle_extract_data(ext_data):
        if ext_data and len(ext_data)>0:
            consume, up_or_down, same = ext_data[1][0]
            if up_or_down == '增长':
                same = float(same) / 100
            else:
                same = -float(same) / 100
            return float(consume), same
        return None, None

    if not year:
        year = datetime.now().year

    update_request = []
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')

    with open(f'energy/{year}用电量.txt', mode='r') as f:
        lines = [line.strip() for line in f.readlines()]
        all_ele_consume_acc_re_list = ['全社会用电量累计(\d+\.?\d+?)亿千瓦时，同比(增长|下降)(\d+\\.?\d+)%']
        all_ele_consume_re_list = ['全社会用电量(\d+\.?\d+?)亿千瓦时，同比(增长|下降)(\d+\\.?\d+)%']
        first_industry_consume_re_list = ['第一产业用电量(\d+\.?\d+?)亿千瓦时，同比(增长|下降)(\d+\\.?\d+)%']
        second_industry_consume_re_list = ['第二产业用电量(\d+\.?\d+?)亿千瓦时，同比(增长|下降)(\d+\\.?\d+)%']
        third_industry_consume_re_list = ['第三产业用电量(\d+\.?\d+?)亿千瓦时，同比(增长|下降)(\d+\\.?\d+)%']
        urban_and_rural_consume_re_list = ['城乡居民生活用电量(\d+\.?\d+?)亿千瓦时，同比(增长|下降)(\d+\\.?\d+)%']
        for line in lines:
            if line != '':
                print(line)
                acc_month_ex = re.findall("(\d)～(\d+)月", line)
                cur_month_ex = re.findall("^(\d+)月", line)

                ext_data = inner_extract_data(first_industry_consume_re_list, line)
                ret = inner_handle_extract_data(ext_data)
                first_industry_consume, first_industry_consume_same = ret

                ext_data = inner_extract_data(second_industry_consume_re_list, line)
                ret = inner_handle_extract_data(ext_data)
                second_industry_consume, second_industry_consume_same = ret

                ext_data = inner_extract_data(third_industry_consume_re_list, line)
                ret = inner_handle_extract_data(ext_data)
                third_industry_consume, third_industry_consume_same = ret

                ext_data = inner_extract_data(urban_and_rural_consume_re_list, line)
                ret = inner_handle_extract_data(ext_data)
                urban_and_rural_consume, urban_and_rural_consume_same = ret
                dict_data = {}

                dict_data['first_industry_consume'] = first_industry_consume
                dict_data['first_industry_consume_same'] = first_industry_consume_same

                dict_data['second_industry_consume'] = second_industry_consume
                dict_data['second_industry_consume_same'] = second_industry_consume_same

                dict_data['third_industry_consume'] = third_industry_consume
                dict_data['third_industry_consume_same'] = third_industry_consume_same

                dict_data['urban_and_rural_consume'] = urban_and_rural_consume
                dict_data['urban_and_rural_consume_same'] = urban_and_rural_consume_same

                if len(cur_month_ex) > 0:
                    int_month = int(cur_month_ex[0])
                    month = f"0{int_month}" if int_month < 10 else str(int_month)
                    cur_month = f"{year}{month}01"

                    ext_data = inner_extract_data(all_ele_consume_re_list, line)
                    ret = inner_handle_extract_data(ext_data)
                    all_consume, all_consume_same = ret
                    if all_consume:
                        dict_data["all_consume"] = all_consume
                        dict_data["all_consume_same"] = all_consume_same
                        print(dict_data)
                        dict_data['time'] = cur_month
                        dict_data['data_type'] = "cn_electric_consume"
                        dict_data['metric_code'] = "cur_month"
                        update_request.append(
                            UpdateOne(
                                {"data_type": dict_data['data_type'], "time": dict_data['time'],
                                 "metric_code": dict_data['metric_code']},
                                {"$set": dict_data},
                                upsert=True))

                elif len(acc_month_ex) > 0:
                    int_month = int(acc_month_ex[0][1])
                    month = f"0{int_month}" if int_month < 10 else str(int_month)
                    acc_month = f"{year}{month}01"

                    ext_data = inner_extract_data(all_ele_consume_acc_re_list, line)
                    ret = inner_handle_extract_data(ext_data)
                    all_consume_acc, all_consume_acc_same = ret

                    if all_consume_acc:
                        dict_data["all_consume_acc"] = all_consume_acc
                        dict_data["all_consume_acc_same"] = all_consume_acc_same
                        dict_data['time'] = acc_month
                        dict_data['data_type'] = "cn_electric_consume"
                        dict_data['metric_code'] = "acc_month"
                        update_request.append(
                            UpdateOne(
                                {"data_type": dict_data['data_type'], "time": dict_data['time'],
                                 "metric_code": dict_data['metric_code']},
                                {"$set": dict_data},
                                upsert=True))
    mongo_bulk_write_data(stock_common,update_request)


def find_data():
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    for ele in news.find({"data_type": "cn_electric_consume", "metric_code": "cur_month"},
                         projection={"_id": False}).sort(
        "time"):
        datas.append(ele)
    data = pd.DataFrame(data=datas)
    show_data(data)
    data.set_index(keys=['time'], inplace=True)
    data['second_industry_consume_same'].plot(kind='bar', title='second_industry_consume_same', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


if __name__ == '__main__':
    handle_electricity_consume()
    find_data()
