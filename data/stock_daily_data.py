import akshare as ak
import tushare as ts
from datetime import datetime
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from tqdm import tqdm
from utils.actions import try_get_action


def save_stock_info_data():
    token = '6a951bc342c8605185d761808e76eafa61064f774ad6a6bcf862b2a9'
    pro = ts.pro_api(token=token)
    data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
    update_request = []
    ticker_info = get_mongo_table(collection='ticker_info')

    for index in data.index:
        dict_data = dict(data.loc[index])
        update_request.append(
            UpdateOne(
            {"ts_code":dict_data['ts_code']},
            {"$set":dict_data},
            upsert=True)
        )

    if len(update_request)>0:
        update_result = ticker_info.bulk_write(update_request,ordered=False)
        print(' 插入：%4d条, 更新：%4d条' %
              (update_result.upserted_count, update_result.modified_count),
              flush=True)

def get_stock_info_data():
    ticker_info = get_mongo_table(collection='ticker_info')
    tickers_cursor = ticker_info.find(projection={'_id': False,'ts_code':True})
    new_codes = []
    for ticker in tickers_cursor:
        ts_code = ticker['ts_code']
        code,lr = ts_code.split(".")
        new_codes.append(code)
    return new_codes



def handle_stock_daily_data(codes=None,start_date=datetime.now().strftime("%Y%m01"),end_date=datetime.now().strftime("%Y%m%d")):
    if codes is None:
        codes = get_stock_info_data()
    tiker_daily = get_mongo_table(collection="ticker_daily")
    print(f"start={start_date},end={end_date}")
    update_request = []
    for code in tqdm(codes):
        stock_zh_a_hist_df =try_get_action(ak.stock_zh_a_hist,try_count=3,symbol=code, period="daily", start_date=start_date, end_date=end_date,
                                                adjust="qfq")

        if stock_zh_a_hist_df is not None:
            for index in stock_zh_a_hist_df.index:
                data = stock_zh_a_hist_df.loc[index]
                day = str(data['日期'])
                open = float(data['开盘'])
                high = float(data['最高'])
                low = float(data['最低'])
                close = float(data['收盘'])
                volume = int(data['成交量'])
                amount = float(data['成交额'])
                amplitude = float(data['振幅'])
                pct_chg = float(data['涨跌幅'])
                change = float(data['涨跌额'])
                turnover_rate = float(data['换手率'])
                dict_data = {
                    'time':day,
                    "open":open,
                    "high":high,
                    "low":low,
                    "close":close,
                    "volume":volume,
                    "code":code,
                    "amount":amount,
                    "amplitude":amplitude,
                    "pct_chg":pct_chg,
                    "change":change,
                    "turnover_rate":turnover_rate
                }
                update_request.append(
                    UpdateOne( {"code":code,'time':day},
                    {"$set":dict_data},
                    upsert=True)
                )
            if len(update_request)>500:
                update_result = tiker_daily.bulk_write(update_request,ordered=False)
                print('插入：%4d条, 更新：%4d条' %
                      (update_result.upserted_count, update_result.modified_count),
                      flush=True)
                update_request.clear()
    if len(update_request) > 0:
        update_result = tiker_daily.bulk_write(update_request, ordered=False)
        print('插入：%4d条, 更新：%4d条' %
              (update_result.upserted_count, update_result.modified_count),
              flush=True)
        update_request.clear()


def col_create_index():
    ticker_daily = get_mongo_table(collection="ticker_daily")
    ticker_daily.drop()
    #ticker_daily.drop_index([("code", 1), ("time", 1)])
    ticker_daily = get_mongo_table(collection="ticker_daily")
    ticker_daily.create_index([("code", 1), ("time", 1)],unique=True,background=True)

if __name__ == '__main__':
    #save_stock_info_data()
    handle_stock_daily_data()

