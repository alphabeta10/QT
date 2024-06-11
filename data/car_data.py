import requests
import json
import re
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import pandas as pd
import matplotlib.pyplot as plt


def handle_total():
    url = 'http://data.cpcadata.com/api/chartlist?charttype=1'
    data = requests.get(url)
    total_market = {
        "narrow": 0,
        "broad": 1,
    }
    total_type = {
        "product": 0,
        "wholesale": 1,
        "retail": 2,
        "exit": 3
    }
    update_datas = []
    data_json = json.loads(data.text)
    if len(data_json) > 1:
        for market_type, index in total_market.items():
            datas = data_json[index]['dataList']
            for data in datas:
                month = data['month'].replace("月", "")
                if int(month) < 10:
                    month = f"0{month}"
                cycle = None
                if "同比" in data.keys():
                    cycle = data['同比']
                for key in data.keys():
                    if key not in ('month', '同比'):
                        year = key.replace("年", "")
                        for type, t_index in total_type.items():
                            time = f"{year}{month}01"
                            if cycle is not None:
                                cyc_data = cycle[t_index]
                            else:
                                cyc_data = None
                            cur_data = data[key][t_index]
                            metric = f"{market_type}_{type}"
                            dict_data = {"data_type": "car_total_market", "name": metric, "time": time, "type": type,
                                         "value": cur_data}
                            if cyc_data is not None:
                                dict_data['cyc_data'] = cyc_data
                            # print(dict_data)
                            update_datas.append(UpdateOne(
                                {"name": dict_data['name'], "time": dict_data['time'],
                                 "data_type": dict_data['data_type']},
                                {"$set": dict_data},
                                upsert=True))
    return update_datas


def handle_man_market():
    url = 'http://data.cpcadata.com/api/chartlist?charttype=2'
    data = requests.get(url)
    total_market = {
        "narrow_acc": 0,
        "narrow_cur": 1,
        "broad_acc": 2,
        "broad_cur": 3,
    }
    total_type = {
        "wholesale": 0,
        "retail": 1
    }
    data_json = json.loads(data.text)
    update_datas = []
    if len(data_json) > 1:
        for market_type, index in total_market.items():
            datas = data_json[index]['dataList']
            for data in datas:
                cycle = data['同比']
                for key in data.keys():
                    man = data['厂商']
                    if key not in ('厂商', '同比'):
                        time = handle_year_and_month(key)
                        for type, t_index in total_type.items():
                            cyc_data = cycle[t_index]
                            cur_data = data[key][t_index]
                            # print(f"{man},{market_type},{time},{type},{cur_data},{cyc_data}")
                            metric = f"{man}_{market_type}_{type}"
                            dict_data = {"data_type": "car_man_market", "name": metric, "time": time, "type": type,
                                         "value": cur_data, "cyc_data": cyc_data}
                            # print(dict_data)
                            update_datas.append(UpdateOne(
                                {"name": dict_data['name'], "time": dict_data['time'],
                                 "data_type": dict_data['data_type']},
                                {"$set": dict_data},
                                upsert=True))
    return update_datas


def handle_year_and_month(key_date):
    result = re.findall(r"\d+", key_date)
    year = result[0]
    month = result[-1]
    if int(month) < 10:
        month = int(month)
        month = f"0{month}"
    time = f"{year}{month}01"
    return time


def handle_car_type():
    url = 'http://data.cpcadata.com/api/chartlist?charttype=3'
    data = requests.get(url)
    total_type = {
        "wholesale": 1,
        "retail": 2
    }

    rate_type = {
        "wholesale": 2,
        "retail": 3
    }
    data_json = json.loads(data.text)
    update_datas = []
    if len(data_json) > 1:
        for datas in data_json:
            category = datas['category']
            data_list = datas['dataList']
            if category != '占比':
                for data in data_list:
                    month = data['month'].replace("月", "")
                    if int(month) < 10:
                        month = int(month)
                        month = f"0{month}"
                    cycle = None
                    if "同比" in data.keys():
                        cycle = data['同比']
                    for key in data.keys():
                        if key not in ('month', '同比'):
                            year = key.replace("年", "")
                            for type, t_index in total_type.items():
                                time = f"{year}{month}01"
                                if cycle is not None:
                                    cyc_data = cycle[t_index]
                                else:
                                    cyc_data = None
                                cur_data = data[key][t_index]
                                # print(f"{category},{time},{type},{cur_data},{cyc_data}")
                                metric = f"{category}_{type}"
                                dict_data = {"data_type": "car_type_market", "name": metric, "time": time, "type": type,
                                             "value": cur_data}
                                if cyc_data is not None:
                                    dict_data["cyc_data"] = cyc_data
                                # print(dict_data)
                                update_datas.append(UpdateOne(
                                    {"name": dict_data['name'], "time": dict_data['time'],
                                     "data_type": dict_data['data_type']},
                                    {"$set": dict_data},
                                    upsert=True))


            else:
                for data in data_list:
                    time = handle_year_and_month(data['月份'])
                    mpvs = data['MPV']
                    suvs = data['SUV']
                    cars = data['轿车']
                    for type, t_index in rate_type.items():
                        mpv = mpvs[t_index]
                        suv = suvs[t_index]
                        car = cars[t_index]
                        # print(f"{category},{time},{type},{mpv},{suv},{car}")
                        metric = f"{type}"
                        dict_data = {"data_type": "car_type_market_rate", "name": metric, "time": time, "type": type,
                                     "mpv": mpv, "suv": suv, "car": car}
                        # print(dict_data)
                        update_datas.append(UpdateOne(
                            {"name": dict_data['name'], "time": dict_data['time'],
                             "data_type": dict_data['data_type']},
                            {"$set": dict_data},
                            upsert=True))
    return update_datas


def handle_new_ene_car():
    url = 'http://data.cpcadata.com/api/chartlist?charttype=6'
    data = requests.get(url)
    category_dict = {
        "total": 0,
        "rate_1": 1,
        "rate_2": 2,
    }
    total_type = {
        "num": 2
    }
    update_datas = []
    data_json = json.loads(data.text)
    if len(data_json) > 1:
        for market_type, index in category_dict.items():
            datas = data_json[index]['dataList']
            if market_type == 'total':
                for data in datas:
                    month = data['month'].replace("月", "")
                    if int(month) < 10:
                        month = f"0{month}"
                    for key in data.keys():
                        if key not in ('month', '同比'):
                            year = key.replace("年", "")
                            for type, t_index in total_type.items():
                                time = f"{year}{month}01"
                                cur_data = data[key][t_index]
                                # print(f"{market_type},{time},{type},{cur_data},{cyc_data}")
                                dict_data = {"data_type": "car_new_energy_detail", "name": market_type, "time": time,
                                             "type": type,
                                             "value": cur_data}
                                # print(dict_data)
                                update_datas.append(UpdateOne(
                                    {"name": dict_data['name'], "time": dict_data['time'],
                                     "data_type": dict_data['data_type']},
                                    {"$set": dict_data},
                                    upsert=True))
            else:
                for data in datas:
                    type = "占比"
                    time = handle_year_and_month(data['月份'])
                    for key, value in data.items():
                        if key not in ['月份']:
                            val = value[2]
                            # print(f"{key},{time},{type},{val}")

                            dict_data = {"data_type": "car_new_energy_detail_rate", "name": key, "time": time,
                                         "type": type,
                                         "value": val}
                            # print(dict_data)
                            update_datas.append(UpdateOne(
                                {"name": dict_data['name'], "time": dict_data['time'],
                                 "data_type": dict_data['data_type']},
                                {"$set": dict_data},
                                upsert=True))
    return update_datas


def handle_level_car():
    url = 'http://data.cpcadata.com/api/chartlist?charttype=5'
    data = requests.get(url)
    data_json = json.loads(data.text)
    update_datas = []
    if len(data_json) > 1:
        for data_list in data_json:
            category = data_list['category']
            for data in data_list['dataList']:
                time = handle_year_and_month(data['月份'])
                type = '占比'
                for key, value in data.items():
                    if key not in ['月份']:
                        val = value[2]
                        # print(f"{category},{key},{time},{type},{val}")
                        metric = f"{category}_{key}"
                        dict_data = {"data_type": "car_level_detail_rate", "name": metric, "time": time, "type": type,
                                     "value": val}
                        # print(dict_data)
                        update_datas.append(UpdateOne(
                            {"name": dict_data['name'], "time": dict_data['time'],
                             "data_type": dict_data['data_type']},
                            {"$set": dict_data},
                            upsert=True))
    return update_datas


def handle_country_car():
    url = 'http://data.cpcadata.com/api/chartlist?charttype=4'
    data = requests.get(url)
    data_json = json.loads(data.text)
    update_datas = []
    if len(data_json) > 0:

        for data_list in data_json:
            category = data_list['category'].replace(" ", '')
            if category == '':
                category = '国别'
            for data in data_list['dataList']:
                time = handle_year_and_month(data['月份'])
                type = '占比'
                for key, value in data.items():
                    if key not in ['月份']:
                        val = value[2]
                        # print(f"{category},{key},{time},{type},{val}")
                        dict_data = {"data_type": "car_country_detail_rate", "name": key, "time": time, "type": type,
                                     "value": val}
                        # print(dict_data)
                        update_datas.append(UpdateOne(
                            {"name": dict_data['name'], "time": dict_data['time'],
                             "data_type": dict_data['data_type']},
                            {"$set": dict_data},
                            upsert=True))
    return update_datas


def chenlianshe_car_data_to_db():
    goods = get_mongo_table(database='stock', collection='goods')
    all_datas = []
    country_car_list = handle_country_car()
    total_car_list = handle_total()
    man_cart = handle_man_market()
    car_type_list = handle_car_type()
    new_energy_list = handle_new_ene_car()
    level_car_list = handle_level_car()

    if len(country_car_list) > 0:
        print("country car len ", len(country_car_list))
        all_datas.extend(country_car_list)
    if len(total_car_list) > 0:
        print("total car len ", len(total_car_list))
        all_datas.extend(total_car_list)
    if len(man_cart) > 0:
        print("man car len ", len(man_cart))
        all_datas.extend(man_cart)
    if len(car_type_list) > 0:
        print("car type len ", len(car_type_list))
        all_datas.extend(car_type_list)
    if len(new_energy_list) > 0:
        print("new energy car len ", len(new_energy_list))
        all_datas.extend(new_energy_list)
    if len(level_car_list) > 0:
        print("level car len ", len(level_car_list))
        all_datas.extend(level_car_list)
    if len(all_datas) > 0:
        mongo_bulk_write_data(goods, all_datas)


def find_data():
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    for ele in goods.find({"data_type": "car_total_market", "name": "narrow_product"}, projection={'_id': False}).sort(
            "time"):
        datas.append(ele)
        print(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data[['time', 'value']]
    data[['value']] = data[['value']].astype(float)
    data.set_index(keys=['time'], inplace=True)
    data[['value']].plot(kind='bar', title='total car market')
    plt.show()


if __name__ == '__main__':
    chenlianshe_car_data_to_db()
    find_data()
