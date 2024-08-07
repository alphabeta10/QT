import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import akshare as ak
import copy
from utils.actions import try_get_action
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data
from datetime import datetime,timedelta
import requests
import pandas as pd
import json
import xml.etree.ElementTree as ET
import matplotlib.pyplot as plt
from data.cn_grand_data import post_or_get_data
#设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')
def global_micro_data(arg_start_date_str = None):
    if arg_start_date_str is None:
        arg_start_date_str = (datetime.now()-timedelta(days=30)).strftime("%Y%m01")

    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    start_date_str = copy.deepcopy(arg_start_date_str)
    now_date_str = datetime.now().strftime("%Y%m%d")
    start_date = datetime.strptime(start_date_str,"%Y%m%d")
    while int(start_date_str)<=int(now_date_str):
        print(f"start handle date={start_date_str}")
        news_economic_baidu_df = try_get_action(ak.news_economic_baidu,try_count=3,date=start_date_str)
        update_request = []
        for index in news_economic_baidu_df.index:
            ele = dict(news_economic_baidu_df.loc[index])
            dict_data = {
                "data_type":"global_micro_data",
                "metric_code":ele['事件'],
                "time":str(ele['日期']),
                "country":ele['地区'],
                "pub_value":str(ele['公布']),
                "predict_value":str(ele['预期']),
                "pre_value":str(ele['前值']),
                "weight":str(ele['重要性']),
            }

            update_request.append(
                UpdateOne(
                    {"data_type": dict_data['data_type'], "time": dict_data['time'],
                     "metric_code": dict_data['metric_code']},
                    {"$set": dict_data},
                    upsert=True)
            )
        if len(update_request)>0:
            mongo_bulk_write_data(stock_common, update_request)
            update_request.clear()

        start_date = start_date + timedelta(days=1)
        start_date_str = start_date.strftime("%Y%m%d")



def get_series_data(category='bank', name='M1', series_id='M1REAL', unit="Billions of Dollars"):
    """
    fred data from api and save data to mongodb
    """
    url = f'https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key=22b1b3ca291181caa2db6092d3cfbdbb&file_type=json'
    ret = try_get_action(requests.get, try_count=3, url=url)
    if ret is not None:
        datas = json.loads(ret.text)
        observations = datas['observations']
        data_list = []
        for observation in observations:
            date = observation['date']
            value = observation['value']
            try:
                value = float(str(value))
            except Exception as e:
                print(f"e={e},value={value}")
                continue
            data = {"category": category, "data_name": name, "data_time": date, "value": value, "series_id": series_id, "unit": unit}
            data_list.append(UpdateOne(
                {"data_name": data['data_name'], "data_time": data['data_time']},
                {"$set": data},
                upsert=True))
        return data_list

def us_monetary_data_to_mongo():
    micro = get_mongo_table(database='stock', collection='micro')
    series_id_list = [{"name": "M0", "category": "finance", "series_id": "BOGMBASE", "unit": "Millions of Dollars"},
                      {"name": "M1", "category": "finance", "series_id": "M1SL",
                       "unit": "Billions of Dollars"},
                      {"name": "M2", "category": "finance", "series_id": "M2SL",
                       "unit": "Billions of Dollars"},
                      {"name": "EFFR", "category": "finance", "series_id": "EFFR",
                       "unit": "Percent"},
                      {"name": "PCEPI", "category": "finance", "series_id": "PCEPI",
                       "unit": "Index 2017=100,Seasonally Adjusted"},
                      ]
    for series_id_dict in series_id_list:
        name, category, series_id, unit = series_id_dict['name'], series_id_dict['category'], series_id_dict[
            'series_id'], series_id_dict['unit']
        data_list = get_series_data(category, name, series_id, unit)
        if data_list is not None and len(data_list)>0:
            mongo_bulk_write_data(micro,data_list)

def jp_cross_boarder_data_to_db():
    micro = get_mongo_table(database='stock', collection='micro')
    data_list = try_get_action(get_jp_cross_border_data_from_bis,start_year=None)
    if data_list is not None and len(data_list) > 0:
        mongo_bulk_write_data(micro, data_list)

def get_jp_cross_border_data_from_bis(start_year=None):
    if start_year is None:
        start_year = datetime.now().strftime("%Y")
    url = f'https://stats.bis.org/api/v1/data/BIS%2CWS_CBS_PUB%2C1.0/Q.S.JP.4R.U.D.A.A.TO1.A.5J/all?startPeriod={start_year}&detail=dataonly'
    ret = requests.get(url)
    tree = ET.fromstring(ret.text)
    obs = tree.iter('Obs')
    quarterly_mapping = {"Q1":"0301","Q2":"0601","Q3":"0901","Q4":"1201"}
    name = 'jp_cross_border_data'
    unit = 'US dollar (Millions)'
    data_list = []
    for ele in obs:
        attrib_dict = ele.attrib
        time,value = attrib_dict['TIME_PERIOD'],float(attrib_dict['OBS_VALUE'])
        year,simple_q = time.split("-")
        date = f"{year}{quarterly_mapping.get(simple_q)}"
        data = {"data_name": name, "data_time": date, "value": value,
                "unit": unit}
        data_list.append(UpdateOne(
            {"data_name": data['data_name'], "data_time": data['data_time']},
            {"$set": data},
            upsert=True))
    return data_list

def get_us_treasury_debt(start_time=None):
    if start_time is None:
        start_time = (datetime.now()-timedelta(days=60)).strftime("%Y-%m-%d")
    url = f'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/debt/mspd/mspd_table_4?filter=record_date:gte:{start_time}&page[number]=1&page[size]=10000'
    ret = requests.get(url)
    json_data = ret.json()
    datas = json_data['data']
    data_list = []
    micro = get_mongo_table(database='stock', collection='micro')
    for data in datas:
        record_date = data['record_date']
        security_class_desc = data['security_class_desc']
        curr_mth_mil_amt = data['curr_mth_mil_amt']
        data = {"data_name": security_class_desc, "data_time": record_date, "value": curr_mth_mil_amt,
                "unit": 'mil_amt'}
        data_list.append(UpdateOne(
            {"data_name": data['data_name'], "data_time": data['data_time']},
            {"$set": data},
            upsert=True))
    mongo_bulk_write_data(micro,data_list)

def craw_lian_rmb():
    micro = get_mongo_table(database='stock', collection='micro')
    wl_url = 'https://api-ddc-wscn.awtmt.com/market/kline?prod_code=USDCNH.OTC&tick_count=1&period_type=2592000&adjust_price_type=forward&fields=tick_at%2Copen_px%2Cclose_px%2Chigh_px%2Clow_px%2Cturnover_volume%2Cturnover_value%2Caverage_px%2Cpx_change%2Cpx_change_rate%2Cavg_px%2Cma2'
    x = post_or_get_data(wl_url, method='get')
    data = x['data']
    lines = data['candle']['USDCNH.OTC']['lines']
    fields = data['fields']
    dict_data = dict(zip(fields,lines[0]))
    dict_data['data_name'] = 'cn_lian_rmb_fx'
    dict_data['data_time'] = datetime.now().strftime("%Y%m%d")
    mongo_bulk_write_data(micro,[UpdateOne(
            {"data_name": dict_data['data_name'], "data_time": dict_data['data_time']},
            {"$set": dict_data},
            upsert=True)])



def find_data():
    series_id_list = [{"name": "M0", "category": "finance", "series_id": "BOGMBASE", "unit": "Millions of Dollars"},
                      {"name": "M1", "category": "finance", "series_id": "M1SL",
                       "unit": "Billions of Dollars"},
                      {"name": "M2", "category": "finance", "series_id": "M2SL",
                       "unit": "Billions of Dollars"}]
    mirco = get_mongo_table(database='stock', collection='micro')
    for series_id_dict in series_id_list:
        unit = series_id_dict['unit']
        datas = []
        for ele in mirco.find({"data_name": series_id_dict['name'],"data_time":{"$gte":"2020-01-01"}}, projection={'_id': False}).sort("data_time"):
            print(ele)
            datas.append(ele)
        pd_data = pd.DataFrame(datas)
        pd_data.set_index(keys='data_time',inplace=True)
        pd_data[['value']].plot(kind='line', title="美国"+series_id_dict['name']+f"货币供应量 单位{unit}", rot=45, figsize=(15, 8), fontsize=10)
        plt.show()



if __name__ == '__main__':
    craw_lian_rmb()
    global_micro_data()
    us_monetary_data_to_mongo()
    jp_cross_boarder_data_to_db()
    get_us_treasury_debt()