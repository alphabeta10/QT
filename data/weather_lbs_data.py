import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from utils.tool import load_json_data
from utils.actions import try_get_action
import requests
from tqdm import tqdm
from utils.actions import show_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import pandas as pd


def handle_all_weather():
    city_pd = pd.read_excel("AMap_adcode_citycode.xlsx", dtype=str)
    adcode_values = city_pd['adcode'].values
    weather = get_mongo_table(database='base', collection='weather')
    update_request = []
    for adcode in tqdm(adcode_values):
        lbs_weather_api(adcode, update_request)
        if len(update_request) > 100:
            mongo_bulk_write_data(weather, update_request)
            update_request.clear()


def lbs_weather_api(city_code, update_request):
    json_key = load_json_data('weather_key.json')
    key = json_key['api_key']
    extensions = 'all'
    url = f'https://restapi.amap.com/v3/weather/weatherInfo?city={city_code}&key={key}&extensions={extensions}&output=json'
    response = try_get_action(requests.get, try_count=3, url=url)
    if response is not None:
        json_data = response.json()
        handle_json_data(json_data, update_request)


def handle_json_data(ret_json, update_request):
    forecasts = ret_json.get('forecasts', None)
    if forecasts is not None and len(forecasts) > 0:
        casts = forecasts[0].get('casts', None)
        if casts is not None:
            city = ''
            if 'city' in forecasts[0].keys():
                city = forecasts[0]['city']
            if 'adcode' not in forecasts[0].keys():
                print(ret_json)
                return
            adcode = forecasts[0]['adcode']
            province = ''
            if 'province' in forecasts[0].keys():
                province = forecasts[0]['province']
            for cast in casts:
                cast['city'] = city
                cast['province'] = province
                cast['data_type'] = 'weather'
                cast['time'] = cast['date']
                cast['metric_code'] = adcode

                update_request.append(
                    UpdateOne(
                        {"data_type": cast['data_type'], "time": cast['time'],
                         "metric_code": cast['metric_code']},
                        {"$set": cast},
                        upsert=True))
def create_index():
    weather = get_mongo_table(database='base', collection='weather')
    weather.create_index([("data_type",1),("metric_code",1),('time',1)],unique=True,background=True)


if __name__ == '__main__':
    handle_all_weather()
