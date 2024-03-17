import pandas as pd
from utils.actions import show_data
from data.mongodb import get_mongo_table

def monitor_goods_price_change(goods_name_list=None):
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    other_datas = []

    if goods_name_list is None:
        # 轻质纯碱 铁矿石(澳) 螺纹钢 玻璃 乙二醇 重质纯碱
        goods_name_list = ['乙二醇', '轻质纯碱', '铁矿石(澳)', '螺纹钢', '玻璃', '重质纯碱', '鸡蛋', '玉米', '小麦', '甲醇',
                           '锦纶FDY', 'WTI原油', '尿素', '鸡蛋', '苯乙烯', 'PTA', '金银花', '生猪', '电解锰', '水泥']
        #goods_name_list = ['乙二醇']
    goods_condition = {"$in": goods_name_list}
    for ele in goods.find({"name": goods_condition, "data_type": "goods_price"}, projection={'_id': False}).sort(
            "time"):
        time = ele['time']
        value = ele['value']
        name = ele['name']
        datas.append({"time":time,"value":value,"name":name})
    pd_data = pd.DataFrame(datas)
    pd_data[['value']] = pd_data[['value']].astype(float)
    data = pd.pivot_table(pd_data, values='value', columns='name', index='time')
    chg = data.pct_change(1)
    show_data(chg)

if __name__ == '__main__':
    monitor_goods_price_change()