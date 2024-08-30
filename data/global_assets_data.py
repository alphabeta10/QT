import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import schedule
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import time
import akshare as ak
from utils.actions import try_get_action

def handle_html_bit_position_data():
    response = requests.get(url='https://bitcointreasuries.com/')
    html = response.content
    now_day = datetime.now().strftime("%Y%m%d")
    html_doc = str(html, 'utf-8')  # html_doc=html.decode("utf-8","ignore")
    soup = BeautifulSoup(html_doc, 'html.parser')
    treasuries = soup.find_all("table", 'treasuries-table')
    h2s = soup.find_all("h2", 'center-on-mobile')
    data_type_list = []
    update_request = []
    for h2 in h2s:
        if h2.get('id', None) is not None:
            data_type = h2.attrs['id']
        else:
            data_type = h2.text
        data_type_list.append("bit_" + data_type)
    print(data_type_list)
    if treasuries is not None and len(treasuries) > 0:
        for i, treasurie in enumerate(treasuries[1:]):
            col_meta = None
            theads = treasurie.find_all("thead")
            if theads is not None and len(theads) > 0:
                thead = theads[0]
                ths = thead.find_all("th")
                if ths is not None and len(ths) > 0:
                    col_meta = [th.attrs['class'][0].replace("th-", "").replace("-", "_") for th in ths]
            trs = treasurie.find_all("tr")
            if trs is not None and len(trs) > 0:
                for tr in trs:
                    tds = tr.find_all("td")
                    if tds is not None and len(tds) > 0:
                        values = [td.text.replace("\n", '').replace("\t", "") for td in tds]
                        data = {}
                        index = 0
                        for k, v in zip(col_meta, values):
                            if k == 'location':
                                spans = tds[index].find_all("span")
                                if spans is not None and len(spans) > 0:
                                    span = spans[0]
                                    v = span.attrs['data-tooltip']
                            index += 1
                            data[k] = v
                            data['data_type'] = data_type_list[i]
                            if data.get('company', '') == '':
                                data['company'] = 'Totals'
                            data['metric_code'] = data['company']
                            data['time'] = now_day
                        update_request.append(
                            UpdateOne(
                                {"data_type": data['data_type'], "time": data['time'],
                                 "metric_code": data['metric_code']},
                                {"$set": data},
                                upsert=True))
    return update_request
def enter_main_bit_position_data():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    update_request = handle_html_bit_position_data()
    mongo_bulk_write_data(stock_common, update_request)

def handle_macro_cons_gold():
    macro_cons_gold_df = try_get_action(ak.macro_cons_gold,try_count=3)
    update_request = []
    if macro_cons_gold_df is not None:
        for index in macro_cons_gold_df.index:
            ele = dict(macro_cons_gold_df.loc[index])
            time = str(ele['日期'])
            position = ele['总库存']
            reduce_or_hold = ele['增持/减持']
            total_value = ele['总价值']
            dict_data = {"time":time,"position":position,"reduce_or_hold":reduce_or_hold,"total_value":total_value,"data_type":"gold","metric_code":"ETF_SPDR_Gold_Position"}
            update_request.append(
                UpdateOne(
                    {"data_type": dict_data['data_type'], "time": dict_data['time'],
                     "metric_code": dict_data['metric_code']},
                    {"$set": dict_data},
                    upsert=True))
    return update_request




def enter_main_gold_position_data():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    update_request = handle_macro_cons_gold()
    mongo_bulk_write_data(stock_common, update_request)


if __name__ == '__main__':
    enter_main_bit_position_data()
    # schedule.every().hour.do(enter_main_bit_position_data)
    # while True:
    #     schedule.run_pending()
    #     time.sleep(10)
