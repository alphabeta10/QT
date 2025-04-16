import sys
import os

# 可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import requests
from bs4 import BeautifulSoup
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from datetime import datetime
from utils.tool import mongo_bulk_write_data
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')
import re
import schedule
import time
from data.comm_real_news_data import get_all_detail_data


def handle_goods_price_data():
    names = ['玻璃','沥青','轻质纯碱']
    _,sell_url_dict = get_all_detail_data(names=names)
    sell_buy_order = get_mongo_table(database='futures', collection='sell_buy_order')
    for name,url in sell_url_dict.items():
        print(f'handle url {url}')
        respond = requests.get(url, headers={
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
            "accept-language": "an,zh-CN;q=0.9,zh;q=0.8,en;q=0.7"})
        html = respond.content
        html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
        soup = BeautifulSoup(html_doc, 'html.parser')
        main_div = soup.find('div', id='main')

        year = datetime.now().strftime("%Y")
        before_year = str(int(year) - 1)
        month = datetime.now().strftime("%m")

        if main_div is not None:
            tables = main_div.find_all("table")
            if tables is not None and len(tables) > 0:
                table = tables[0]
                trs = table.find_all("tr")
                mapping = {
                    "卖盘商品": "sell_name",
                    "规格": "specifications",
                    "单价": "price",
                    "数量": "quantity",
                    "现货类型": "spot_type",
                    "交收期": "delivery_period",
                    "交货地": "delivery_location",
                    "发布时间": "release_time",
                    "有效时间": "validity_period",
                    "联系": "contact"
                }
                upsert_datas = []
                for tr in trs[1:]:
                    tds = tr.find_all("td")
                    datas = []
                    day = None
                    for i, td in enumerate(tds):
                        if i == 7:
                            text_value = td.text.strip().replace("\n", "").replace("\t", "")
                            dm = int(text_value.split("-")[0])
                            if dm > int(month):
                                day = f"{before_year}-{text_value}"
                            else:
                                day = f"{year}-{text_value}"
                        else:
                            text_value = td.text.replace(" ", "").strip().replace("\n", "").replace("\t", "")
                        datas.append(text_value)
                    if len(datas) > 0:
                        dict_data = {}
                        for k, v in zip(list(mapping.values()), datas):
                            dict_data[k] = v
                        dict_data['time'] = day
                        upsert_datas.append(UpdateOne(
                            {"sell_name": dict_data['sell_name'], "time": dict_data['time'],
                             "price": dict_data['price'],"delivery_location":dict_data['delivery_location'],'specifications':dict_data['specifications']},
                            {"$set": dict_data},
                            upsert=True))
                mongo_bulk_write_data(sell_buy_order, upsert_datas)



if __name__ == '__main__':
    handle_goods_price_data()
    schedule.every(30).minutes.do(handle_goods_price_data)
    while True:
        schedule.run_pending()
        time.sleep(10)
