import pandas as pd
import re
from data.mongodb import get_mongo_table
import os
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data
import matplotlib.pyplot as plt
#设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')

def show_data(data:pd.DataFrame):
    for index in data.index:
        print(dict(data.loc[index]))

def export_goods_detail_data(file_name,mogodb,dtype='export_goods_detail'):
    goods_datas = pd.read_excel(file_name)
    head = str(goods_datas.loc[0,:]['Unnamed: 1'])
    if head=='nan':
        head=goods_datas.loc[0,:]['Unnamed: 0']
    print(file_name,head)
    if file_name=='customs_import2020/2020082419125355640.xls':
        return
    result = re.findall(r"(\d+)年(\d+)",head)
    if '出口' in head:
        dtype = 'export_goods_detail'
    if '进口' in head:
        dtype = 'import_goods_detail'
    if len(result)>0:
        year,month = result[0]
        if int(month)<10:
            month = f"0{month}"
        date_str = f"{year}-{month}-01"
        print(f"handle date str {date_str},dtype={dtype}")
        cols_name_mapping = {
            "name":1,
            "unit":2,
            "month_volume":3,
            "month_amount":4,
            "acc_month_volume": 5,
            "acc_month_amount": 6,
            "month_volume_cyc": 7,
            "month_amount_cyc": 8,
            "acc_month_volume_cyc": 9,
            "acc_month_amount_cyc": 10,
        }
        data = goods_datas.loc[4:,:]
        save_datas = []
        for index in data.index:
            ele = data.loc[index]
            new_dict = {}
            new_dict['data_type'] = dtype
            for k,v in cols_name_mapping.items():
                col_name = f"Unnamed: {v}"
                value = str(ele[col_name]).replace(" ","").replace("\xa0","")
                if k in ['month_volume','month_amount','acc_month_volume','acc_month_amount']:
                    value = value.replace(",","")
                new_dict[k] = value
            if new_dict['month_amount']=='nan':
                print("nan data ",new_dict)
            else:
                new_dict['date'] = date_str
                save_datas.append(UpdateOne(
                {"name": new_dict['name'],"date":new_dict['date'],"data_type":new_dict['data_type'],"unit":new_dict['unit']},
                {"$set": new_dict},
                upsert=True))
        if len(save_datas)>0:
            mongo_bulk_write_data(mogodb, save_datas)
    else:
        print("get head date is None")



def export_country_detail_data(file_name,mogodb):
    country_data = pd.read_excel(file_name)
    head = country_data.loc[0, :]['Unnamed: 1']
    unit = country_data.loc[1,:]['Unnamed: 9'].replace("单位：","")
    result = re.findall(r"(\d+)年(\d+)", head)
    if '进出口商品国别' in head:
        dtype = 'country_export_import'
    else:
        print("no country export import data ")
        return
    if len(result) > 0:
        year, month = result[0]
        if int(month) < 10:
            month = f"0{month}"
        date_str = f"{year}-{month}-01"
        print(f"handle date str {date_str},dtype={dtype}")
        cols_name_mapping = {
            "name": 1,
            "export_import_amount": 2,
            "acc_export_import_amount": 3,
            "export_amount": 4,
            "acc_export_amount": 5,
            "import_amount": 6,
            "acc_import_amount": 7,
            "acc_export_import_amount_cyc": 8,
            "acc_export_amount_cyc": 9,
            "acc_import_amount_cyc": 10,
        }
        data = country_data.loc[4:, :]
        save_datas = []
        for index in data.index:
            ele = data.loc[index]
            new_dict = {}
            new_dict['data_type'] = dtype
            for k, v in cols_name_mapping.items():
                col_name = f"Unnamed: {v}"
                value = str(ele[col_name]).replace(" ", "").replace("\xa0", "")
                if k not in ['name']:
                    value = value.replace(",", "")
                new_dict[k] = value
            if new_dict['import_amount'] == 'nan':
                print("nan data ", new_dict)
            else:
                new_dict['date'] = date_str
                new_dict['unit'] = unit
                save_datas.append(UpdateOne(
                    {"name": new_dict['name'], "date": new_dict['date'], "data_type": new_dict['data_type'],"unit":new_dict['unit']},
                    {"$set": new_dict},
                    upsert=True))
        if len(save_datas) > 0:
            mongo_bulk_write_data(mogodb, save_datas)
    else:
        print("get head date is None")






def handle_all_export_data(file_dir):
    list_file = os.listdir(file_dir)
    mongodb = get_mongo_table(database='govstats', collection='customs_goods')
    for file in list_file:
        file_path = os.path.join(file_dir,file)
        export_goods_detail_data(file_path,mongodb)

def handle_all_inport_data(file_dir):
    list_file = os.listdir(file_dir)
    mongodb = get_mongo_table(database='govstats', collection='customs_goods')
    for file in list_file:
        file_path = os.path.join(file_dir, file)
        export_goods_detail_data(file_path, mongodb,dtype='import_goods_detail')

def handle_all_country_export_import_data(file_dir):
    list_file = os.listdir(file_dir)
    mongodb = get_mongo_table(database='govstats', collection='customs_goods')
    for file in list_file:
        file_path = os.path.join(file_dir, file)
        export_country_detail_data(file_path, mongodb)

def find_data():
    data_info = get_mongo_table(database='govstats', collection='customs_goods')
    datas = []
    for ele in data_info.find({"name":"德国","data_type":"country_export_import"},projection={'_id': False}).sort("date"):
        datas.append(ele)
        print(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data[['date','export_amount','import_amount','acc_export_amount_cyc','acc_import_amount_cyc','acc_export_import_amount_cyc']]
    data[['export_amount','import_amount','acc_export_amount_cyc','acc_import_amount_cyc','acc_export_import_amount_cyc']] = data[['export_amount','import_amount','acc_export_amount_cyc','acc_import_amount_cyc','acc_export_import_amount_cyc']].astype(float)
    data.set_index(keys=['date'],inplace=True)

    #data[['import_amount','export_amount']].plot(kind='bar',title='export car vol')
    data[['acc_export_amount_cyc','acc_import_amount_cyc','acc_export_import_amount_cyc']].plot(kind='bar',title='export car vol',figsize=(15,8),rot=45)
    plt.show()


def find_data_goods():
    data_info = get_mongo_table(database='govstats', collection='customs_goods')
    datas = [] # 太阳能电池 万吨     啤酒 万升
    for ele in data_info.find({"name":"啤酒","data_type":"export_goods_detail","unit":"万升"},projection={'_id': False}).sort("date"):
        datas.append(ele)
        print(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data
    fs = ['month_volume_cyc','month_amount_cyc','acc_month_amount_cyc','acc_month_volume_cyc']
    fs = ['month_volume_cyc','month_amount_cyc','acc_month_amount_cyc','acc_month_volume_cyc']
    dict_fs = {
        "month_volume_cyc":"当月数量同比",
        # "month_amount_cyc":"当月金额同比",
        # "acc_month_amount_cyc":"累计当月金额同比",
        "acc_month_volume_cyc":"累计当月数量同比",
    }
    #fs = ['month_volume']
    #data[['month_amount','month_volume']] = data[['month_amount','month_volume']].astype(float)
    data[list(dict_fs.keys())] = data[list(dict_fs.keys())].astype(float)
    data.set_index(keys=['date'],inplace=True)
    data = data.rename(columns=dict_fs)

    #data[['import_amount','export_amount']].plot(kind='bar',title='export car vol')
    data[list(dict_fs.values())].plot(kind='bar',title='export car vol',figsize=(15,8),rot=45)
    plt.show()



if __name__ == '__main__':
    dir = 'customs_export_import_temp'
    handle_all_export_data(dir)
    dir = 'customs_country_temp'
    handle_all_country_export_import_data(dir)
    find_data_goods()
