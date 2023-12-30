import pandas as pd

from utils.tool import get_data_from_mongo
import matplotlib.pyplot as plt

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
warnings.filterwarnings('ignore')

def month_show(data:pd.DataFrame,time_key,code_key,val_key,val_key_name,title):
    year_dict_data = {}
    for index in data.index:
        ele = data.loc[index]
        print(dict(ele))
        time = ele[time_key]
        code = ele[code_key]
        val = ele[val_key]
        year = time[0:4]
        metric = val_key_name
        combine_key = f"{year}年{code}{metric}"
        index_of = int(time.split("-")[1]) - 1
        if combine_key not in year_dict_data.keys():
            year_dict_data[combine_key] = [0.0] * 12
        year_dict_data[combine_key][index_of] = val
    convert_data = pd.DataFrame(data=year_dict_data,
                                index=['1月', '2月', '3月', '4月', '5月', '6月', '7月', '8月', '9月', '10月', '11月',
                                       '12月'])
    convert_data.plot(kind='bar', title=title, rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    plt.show()

def import_beans_price():
    database = 'govstats'
    collection = 'customs_goods'
    name = '大豆'
    unit = '万吨'
    data_type = "import_goods_detail"
    projection = {'_id': False}
    condition = {"name": name, "data_type": data_type, "unit": unit,"date":{"$gte":"2021-01-01"}}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    dict_fs = {
        "month_volume_cyc": "当月数量同比",
        "month_volume": "当月数量",
        "acc_month_volume_cyc": "累计当月数量同比",
        "acc_month_volume": "累计当月数量",
        "month_amount_cyc": "当月金额同比",
        "month_amount": "当月金额",
        "acc_month_amount_cyc": "累计当月金额同比",
        "acc_month_amount": "累计当月金额",

    }

    data['month_volume'] = data['month_volume'].astype(float)
    data['month_amount'] = data['month_amount'].astype(float)

    data['price'] = round(data['month_amount']/data['month_volume'],2)
    month_show(data,'date','name','price','大豆进口月价格','大豆进口价格')


if __name__ == '__main__':
    import_beans_price()