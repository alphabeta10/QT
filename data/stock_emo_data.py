import akshare as ak
from utils.actions import try_get_action
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data

# 长沙图书馆账号 00006230076234/19910813
def stock_emo_data():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    # 东方财富网-数据中心-特色数据-高管持股
    stock_ggcg_em_df = try_get_action(ak.stock_ggcg_em,try_count=3,symbol="全部")
    update_request = []
    for index in stock_ggcg_em_df.index:
        dict_data = dict(stock_ggcg_em_df.loc[index])
        code = dict_data['代码']
        name = dict_data['名称']
        ann_date = str(dict_data['公告日'])
        change_start_date = str(dict_data['变动开始日'])
        change_end_date = str(dict_data['变动截止日'])
        change_type = dict_data['持股变动信息-增减']
        if change_type == '减持':
            type = 'reduction'
        elif change_type == '增持':
            type = 'overweight'
        else:
            print(f"{code} {name} error_type_data ")
            continue

        in_db_data = {"data_type": f"stock_{type}", "metric_code": code, "time": ann_date}
        in_db_data['change_start_date'] = change_start_date
        in_db_data['change_end_date'] = change_end_date
        in_db_data['ann_date'] = ann_date
        in_db_data['name'] = name

        update_request.append(
            UpdateOne(
                {"data_type": in_db_data['data_type'], "time": in_db_data['time'],
                 "metric_code": in_db_data['metric_code']},
                {"$set": in_db_data},
                upsert=True)
        )
        if len(update_request)%1000==0:
            mongo_bulk_write_data(stock_common, update_request)
            update_request.clear()

    if len(update_request)>0:
        mongo_bulk_write_data(stock_common, update_request)
        update_request.clear()

    # 股票账户统计月度
    # stock_account_statistics_em_df = ak.stock_account_statistics_em()
    # show_data(stock_account_statistics_em_df)

    # 股票指数成交量，成交额

    # 机构调研 - 统计
    # stock_jgdy_tj_em_df = ak.stock_jgdy_tj_em(date="20231201")
    # show_data(stock_jgdy_tj_em_df)

    # 综合情绪指标数据
    # 间接情绪指标数据
    # 直接情绪指标数据

def find_data():
    news = get_mongo_table(database='stock', collection='common_seq_data')
    for data in news.find({"data_type":"global_micro_data","country":"美国","time":{"$gt":"2023-12-01"}},projection={'_id': False}).sort("time"):
        print(data)


if __name__ == '__main__':
    find_data()
