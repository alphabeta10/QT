import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
from utils.actions import show_data
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def handle_battery_data():
    with open("battery/2024电池数据.txt", mode='r') as f:
        lines = f.readlines()
        dict_data = {}
        for line in lines:
            power_battery_production = -1
            power_battery_production_same = -1

            power_battery_loading = -1
            power_battery_loading_same = -1

            power_battery_loading_acc = -1
            power_battery_loading_acc_same = -1

            power_battery_export = -1
            storage_power_battery_export = -1

            power_battery_acc_export = -1
            storage_power_battery_acc_export = -1

            power_battery_production_acc = -1
            power_battery_production_acc_same = -1

            storage_power_battery_production = -1
            storage_power_battery_production_acc = -1

            re_month = re.findall("(\d+)年(\d+)月\\，", line)
            t_day = -1
            if len(re_month) > 0:
                year, month = int(re_month[0][0]), int(re_month[0][1])
                if month < 10:
                    t_month = f"0{month}"
                else:
                    t_month = month
                t_day = f"{year}{t_month}01"

            re_month = re.findall("(\d+)年1-(\d+)月\\，", line)
            if len(re_month) > 0:
                year, month = int(re_month[0][0]), int(re_month[0][1])
                if month < 10:
                    t_month = f"0{month}"
                else:
                    t_month = month
                t_day = f"{year}{t_month}01"
            if t_day not in dict_data.keys() and t_day != -1:
                dict_data[t_day] = {}

            if "我国动力电池产量共计" in line:
                get_res = re.findall("我国动力电池产量共计(\d+\\.?\d+)GWh，同比下降(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_production = float(get_res[0][0])
                    power_battery_production_same = -float(get_res[0][1])
                get_res = re.findall("我国动力电池产量共计(\d+\\.?\d+)GWh，同比增长(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_production = float(get_res[0][0])
                    power_battery_production_same = float(get_res[0][1])
            if "我国动力和储能电池合计产量为" in line:
                get_res = re.findall("我国动力和储能电池合计产量为(\d+\\.?\d+)GWh", line)
                if len(get_res) > 0:
                    storage_power_battery_production = float(get_res[0])
                    get_res = re.findall("同比增长(\d+\\.?\d+)%", line)
                    if len(get_res) > 0:
                        power_battery_production_same = float(get_res[0])
                    else:
                        get_res = re.findall("同比下降(\d+\\.?\d+)%", line)
                        power_battery_production_same = -float(get_res[0])
                    get_res = re.findall("其中动力电池产量占比约为(\d+\\.?\d+)%", line)
                    if len(get_res) > 0:
                        power_battery_production = round((float(get_res[0]) / 100) * storage_power_battery_production,
                                                         1)
                    get_res = re.findall("其中动力电池产量占比为(\d+\\.?\d+)%", line)
                    if len(get_res) > 0:
                        power_battery_production = round((float(get_res[0]) / 100) * storage_power_battery_production,
                                                         1)

            if "我国动力电池累计产量" in line:
                get_res = re.findall("我国动力电池累计产量(\d+\\.?\d+)GWh，累计同比下降(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_production_acc = float(get_res[0][0])
                    power_battery_production_acc_same = -float(get_res[0][1])
                get_res = re.findall("我国动力电池累计产量(\d+\\.?\d+)GWh，累计同比增长(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_production_acc = float(get_res[0][0])
                    power_battery_production_acc_same = float(get_res[0][1])
            if "我国动力和储能电池合计累计产量为" in line:
                get_res = re.findall("我国动力和储能电池合计累计产量为(\d+\\.?\d+)GWh，产量累计同比下降(\d+\\.?\d+)%",
                                     line)
                if len(get_res) > 0:
                    storage_power_battery_production_acc = float(get_res[0][0])
                    power_battery_production_acc_same = -float(get_res[0][1])
                get_res = re.findall("我国动力和储能电池合计累计产量为(\d+\\.?\d+)GWh，产量累计同比增长(\d+\\.?\d+)%",
                                     line)
                if len(get_res) > 0:
                    storage_power_battery_production_acc = float(get_res[0][0])
                    power_battery_production_acc_same = float(get_res[0][1])
                get_res = re.findall("其中动力电池产量占比约为(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_production_acc = round(
                        (float(get_res[0]) / 100) * storage_power_battery_production_acc, 1)
                get_res = re.findall("其中动力电池产量占比为(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_production_acc = round(
                        (float(get_res[0]) / 100) * storage_power_battery_production_acc, 1)

            if "我国动力电池装车量" in line:
                get_res = re.findall("我国动力电池装车量(\d+\\.?\d+)GWh，同比下降(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_loading = float(get_res[0][0])
                    power_battery_loading_same = -float(get_res[0][1])
                get_res = re.findall("我国动力电池装车量(\d+\\.?\d+)GWh，同比增长(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_loading = float(get_res[0][0])
                    power_battery_loading_same = float(get_res[0][1])
            if "我国动力电池累计装车量" in line:
                get_res = re.findall("我国动力电池累计装车量(\d+\\.?\d+)GWh, 累计同比下降(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_loading_acc = float(get_res[0][0])
                    power_battery_loading_acc_same = -float(get_res[0][1])
                get_res = re.findall("我国动力电池累计装车量(\d+\\.?\d+)GWh, 累计同比增长(\d+\\.?\d+)%", line)
                if len(get_res) > 0:
                    power_battery_loading_acc = float(get_res[0][0])
                    power_battery_loading_acc_same = float(get_res[0][1])

            if "我国动力电池企业电池出口共计" in line:
                get_res = re.findall("我国动力电池企业电池出口共计(\d+\\.?\d+)GWh", line)
                if len(get_res) > 0:
                    power_battery_export = float(get_res[0])

            if "我国动力和储能电池合计出口" in line:
                get_res = re.findall("我国动力和储能电池合计出口(\d+\\.?\d+)GWh", line)
                if len(get_res) > 0:
                    storage_power_battery_export = float(get_res[0])
                    power_battery_export = storage_power_battery_export

            if "我国动力和储能电池合计累计出口达" in line:
                get_res = re.findall("我国动力和储能电池合计累计出口达(\d+\\.?\d+)GWh", line)
                if len(get_res) > 0:
                    storage_power_battery_acc_export = float(get_res[0])
                    power_battery_acc_export = storage_power_battery_acc_export
            if "我国动力电池企业电池累计出口达" in line:
                get_res = re.findall("我国动力电池企业电池累计出口达(\d+\\.?\d+)GWh", line)
                if len(get_res) > 0:
                    power_battery_acc_export = float(get_res[0])
            if power_battery_production != -1 and power_battery_production_same != -1:
                print('电池当月产量', t_day, power_battery_production, power_battery_production_same)
                dict_data[t_day]['power_battery_production'] = power_battery_production
                dict_data[t_day]['power_battery_production_same'] = power_battery_production_same

            if power_battery_production_acc != -1 and power_battery_production_acc_same != -1:
                print("电池累计产量", t_day, power_battery_production_acc, power_battery_production_acc_same)
                dict_data[t_day]['power_battery_production_acc'] = power_battery_production_acc
                dict_data[t_day]['power_battery_production_acc_same'] = power_battery_production_acc_same
            if power_battery_loading != -1 and power_battery_loading_same != -1:
                print("动力电池装车量", t_day, power_battery_loading, power_battery_loading_same)
                dict_data[t_day]['power_battery_loading'] = power_battery_loading
                dict_data[t_day]['power_battery_loading_same'] = power_battery_loading_same

            if power_battery_loading_acc != -1 and power_battery_loading_acc_same != -1:
                print("动力电池累计装车量", t_day, power_battery_loading_acc, power_battery_loading_acc_same)
                dict_data[t_day]['power_battery_loading_acc'] = power_battery_loading_acc
                dict_data[t_day]['power_battery_loading_acc_same'] = power_battery_loading_acc_same

            if power_battery_export != -1:
                print("动力电池出口", t_day, power_battery_export)
                dict_data[t_day]['power_battery_export'] = power_battery_export

            if power_battery_acc_export != -1:
                print("动力电池累计出口", t_day, power_battery_acc_export)
                dict_data[t_day]['power_battery_acc_export'] = power_battery_acc_export

            if storage_power_battery_production != -1:
                dict_data[t_day]['storage_power_battery_production'] = storage_power_battery_production
            if storage_power_battery_production_acc != -1:
                dict_data[t_day]['storage_power_battery_production_acc'] = storage_power_battery_production_acc
            if storage_power_battery_export != -1:
                dict_data[t_day]['storage_power_battery_export'] = storage_power_battery_export
            if storage_power_battery_acc_export != -1:
                dict_data[t_day]['storage_power_battery_acc_export'] = storage_power_battery_acc_export

        ele_of_data = dict_data['20240101']
        ele_of_data['power_battery_production_acc'] = ele_of_data['power_battery_production']
        ele_of_data['power_battery_production_acc_same'] = ele_of_data['power_battery_production_same']
        ele_of_data['power_battery_loading_acc'] = ele_of_data['power_battery_loading']
        ele_of_data['power_battery_loading_acc_same'] = ele_of_data['power_battery_loading_same']
        ele_of_data['power_battery_acc_export'] = ele_of_data['power_battery_export']
        dict_data['20240101'] = ele_of_data

        update_request = []
        for k, v in dict_data.items():
            new_dict_data = v
            new_dict_data['time'] = k
            new_dict_data['data_type'] = 'power_battery'
            new_dict_data['metric_code'] = 'power_battery_abstract'
            update_request.append(
                UpdateOne(
                    {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                     "metric_code": new_dict_data['metric_code']},
                    {"$set": new_dict_data},
                    upsert=True)
            )
        stock_common = get_mongo_table(database='stock', collection='common_seq_data')

        print(update_request)
        if update_request is not None:
            mongo_bulk_write_data(stock_common, update_request)

def handle_company_battery_data(file_name='battery/2023company.txt'):
    with open(file_name, mode='r') as f:
        lines = f.readlines()
        dict_data = {}
        temp_time_list = []
        for line in lines:
            re_month = re.findall("(\d+)年(\d+)月", line)
            t_day = -1
            t_type = -1
            if len(re_month) > 0:
                year, month = int(re_month[0][0]), int(re_month[0][1])
                if month < 10:
                    t_month = f"0{month}"
                else:
                    t_month = month
                t_day = f"{year}{t_month}01"
                t_type = 'cur'
            re_month = re.findall("(\d+)年1-(\d+)月", line)
            if len(re_month) > 0:
                year, month = int(re_month[0][0]), int(re_month[0][1])
                if month < 10:
                    t_month = f"0{month}"
                else:
                    t_month = month
                t_day = f"{year}{t_month}01"
                t_type = 'acc'

            if t_day!=-1 and t_day not in dict_data.keys():
                dict_data[t_day] = {}
                dict_data[t_day]['type'] = t_type
                temp_time_list.append(t_day)

            splits = line.strip().split(" ")
            if len(splits)==4 and splits[0]!='序号':
                t_day = temp_time_list[-1]
                rank,company_name,capacity,market_rate = splits
                dict_data[t_day][f'rank{rank}'] = company_name
                dict_data[t_day][f'rank{rank}_capacity'] = capacity
                dict_data[t_day][f'rank{rank}_market_rate'] = market_rate
        for k,v in dict_data.items():
            print(k,len(v))

        update_request = []
        for k, v in dict_data.items():
            new_dict_data = v
            new_dict_data['time'] = k
            type = new_dict_data['type']
            new_dict_data['data_type'] = 'power_battery'
            new_dict_data['metric_code'] = f'power_battery_company_{type}_market_rate'
            update_request.append(
                UpdateOne(
                    {"data_type": new_dict_data['data_type'], "time": new_dict_data['time'],
                     "metric_code": new_dict_data['metric_code']},
                    {"$set": new_dict_data},
                    upsert=True)
            )
        stock_common = get_mongo_table(database='stock', collection='common_seq_data')

        print(update_request)
        if update_request is not None:
            mongo_bulk_write_data(stock_common, update_request)



def find_data():
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    for ele in news.find({"data_type": "power_battery", "metric_code": "power_battery_company_cur_market_rate"},
                         projection={'_id': False}).sort("time"):
        new_ele = {}
        for k,v in ele.items():
            if 'market_rate' in k:
                v = round(float(v.replace("%",""))/100,6)
                new_ele[k] = v
            else:
                new_ele[k] = v
        datas.append(new_ele)
        print(ele)
    pd_data = pd.DataFrame(data=datas)

    get_cols = ["power_battery_production_acc_same", "power_battery_loading_acc_same","power_battery_production_same", "power_battery_loading_same", 'time']
    get_cols = ["rank1_market_rate", 'time']
    data = pd_data[get_cols]

    show_data(data)
    for col in get_cols[:-1]:
        data[[col]] = data[[col]].astype(float)
    data.set_index(keys=['time'], inplace=True)
    # data['gk_teu_traffic_ptc'] = data['gk_teu_traffic'].pct_change(1)
    data.plot(kind='line', title='动力电池', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()



if __name__ == '__main__':
    handle_battery_data()
    for filename in ['battery/2024company.txt','battery/2024company_acc.txt']:
        handle_company_battery_data(filename)
    find_data()