import akshare as ak
import tushare as ts
from datetime import datetime,timedelta
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
                {"ts_code": dict_data['ts_code']},
                {"$set": dict_data},
                upsert=True)
        )

    if len(update_request) > 0:
        update_result = ticker_info.bulk_write(update_request, ordered=False)
        print(' 插入：%4d条, 更新：%4d条' %
              (update_result.upserted_count, update_result.modified_count),
              flush=True)


def get_stock_info_data():
    ticker_info = get_mongo_table(collection='ticker_info')
    tickers_cursor = ticker_info.find(projection={'_id': False, 'ts_code': True})
    new_codes = []
    for ticker in tickers_cursor:
        ts_code = ticker['ts_code']
        code, lr = ts_code.split(".")
        new_codes.append(code)
    return new_codes


def handle_stock_daily_data(codes=None, start_date=None,
                            end_date=datetime.now().strftime("%Y%m%d")):
    if codes is None:
        codes = get_stock_info_data()
    if start_date is None:
        start_date = (datetime.now()-timedelta(days=5)).strftime("%Y%m%d")
    tiker_daily = get_mongo_table(collection="ticker_daily")
    print(f"start={start_date},end={end_date}")
    update_request = []
    for code in tqdm(codes):
        stock_zh_a_hist_df = try_get_action(ak.stock_zh_a_hist, try_count=3, symbol=code, period="daily",
                                            start_date=start_date, end_date=end_date,
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
                    'time': day,
                    "open": open,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "code": code,
                    "amount": amount,
                    "amplitude": amplitude,
                    "pct_chg": pct_chg,
                    "change": change,
                    "turnover_rate": turnover_rate
                }
                update_request.append(
                    UpdateOne({"code": code, 'time': day},
                              {"$set": dict_data},
                              upsert=True)
                )
            if len(update_request) > 500:
                update_result = tiker_daily.bulk_write(update_request, ordered=False)
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


def handle_stock_dzjy_mrtj(start_date=datetime.now().strftime("%Y%m01"), end_date=datetime.now().strftime("%Y%m%d")):
    stock_dzjy_mrtj_df = ak.stock_dzjy_mrtj(start_date=start_date, end_date=end_date)
    update_request = []
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    for index in stock_dzjy_mrtj_df.index:
        data = stock_dzjy_mrtj_df.loc[index]
        day = str(data['交易日期'])
        close = float(data['收盘价'])
        trade_price = float(data['成交价'])
        amount = float(data['成交总额'])
        pct_chg = float(data['涨跌幅'])
        dis_rate = float(data['折溢率'])
        trade_div_cir_total = float(data['成交总额/流通市值'])
        num_of_trade = int(data['成交笔数'])
        code = str(data['证券代码'])
        dict_data = {
            'time': day,
            "close": close,
            "trade_price": trade_price,
            "metric_code": code,
            "amount": amount,
            "num_of_trade": num_of_trade,
            "pct_chg": pct_chg,
            "trade_div_cir_total": trade_div_cir_total,
            "dis_rate": dis_rate,
            "data_type": "stock_dzjy"
        }
        update_request.append(
            UpdateOne({"metric_code": dict_data['metric_code'], 'time': dict_data['time'],
                       "data_type": dict_data['data_type']},
                      {"$set": dict_data},
                      upsert=True)
        )

        if len(update_request) > 500:
            update_result = stock_common.bulk_write(update_request, ordered=False)
            print('插入：%4d条, 更新：%4d条' %
                  (update_result.upserted_count, update_result.modified_count),
                  flush=True)
            update_request.clear()

    if len(update_request) > 0:
        update_result = stock_common.bulk_write(update_request, ordered=False)
        print('插入：%4d条, 更新：%4d条' %
              (update_result.upserted_count, update_result.modified_count),
              flush=True)
        update_request.clear()


def stock_dzjy_main():
    tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
    trade_dates = []
    start_date = datetime.now().strftime("%Y%m01")
    now_int = int(datetime.now().strftime("%Y%m%d"))
    for index in tool_trade_date_hist_sina_df.index:
        trade_date = tool_trade_date_hist_sina_df.loc[index]['trade_date']
        date_str = str(trade_date).replace("-", "")
        if int(date_str) > int(start_date) and int(date_str) <= now_int:
            trade_dates.append(date_str)

    for trade_date in trade_dates:
        print(f"handle {trade_date}")
        handle_stock_dzjy_mrtj(start_date=trade_date, end_date=trade_date)


def handle_stock_cyq_main():
    codes = get_stock_info_data()
    if codes is not None and len(codes) > 0:
        col_mapping = {'日期': 'time', '获利比例': 'profit_ratio', '平均成本': 'avg_cost', '90成本-低': 'cost_90_low',
                       '90成本-高': 'cost_90_high', '90集中度': 'concentration_90', '70成本-低': 'cost_70_low',
                       '70成本-高': 'cost_70_high',
                       '70集中度': 'concentration_90'}
        update_request = []
        stock_common = get_mongo_table(database='stock', collection='common_seq_data')
        for code in tqdm(codes):
            stock_cyq_em_df = try_get_action(ak.stock_cyq_em,try_count=3,symbol=code, adjust="")
            if stock_cyq_em_df is not None and len(stock_cyq_em_df)>0:
                before_day_str = (datetime.now()-timedelta(days=10)).strftime("%Y-%m-%d")
                stock_cyq_em_df['日期'] = stock_cyq_em_df.apply(
                    lambda row: str(row['日期'])[0:10],
                    axis=1)
                stock_cyq_em_df = stock_cyq_em_df[stock_cyq_em_df['日期']>before_day_str]
                for index in stock_cyq_em_df.index:
                    dict_data = dict(stock_cyq_em_df.loc[index])
                    new_dict_data = {"data_type": "stock_cyq","metric_code":code}
                    for col, re_col in col_mapping.items():
                        if col == '日期':
                            val = str(dict_data.get(col))
                        else:
                            val = float(dict_data.get(col))
                        new_dict_data[re_col] = val
                    update_request.append(
                        UpdateOne({"metric_code": new_dict_data['metric_code'], 'time': new_dict_data['time'],
                                   "data_type": new_dict_data['data_type']},
                                  {"$set": new_dict_data},
                                  upsert=True)
                    )
            if len(update_request) > 500:
                update_result = stock_common.bulk_write(update_request, ordered=False)
                print('插入：%4d条, 更新：%4d条' %
                      (update_result.upserted_count, update_result.modified_count),
                      flush=True)
                update_request.clear()

        if len(update_request) > 0:
            update_result = stock_common.bulk_write(update_request, ordered=False)
            print('插入：%4d条, 更新：%4d条' %
                  (update_result.upserted_count, update_result.modified_count),
                  flush=True)
            update_request.clear()


def col_create_index():
    ticker_daily = get_mongo_table(collection="ticker_daily")
    ticker_daily.drop()
    # ticker_daily.drop_index([("code", 1), ("time", 1)])
    ticker_daily = get_mongo_table(collection="ticker_daily")
    ticker_daily.create_index([("code", 1), ("time", 1)], unique=True, background=True)


if __name__ == '__main__':
    save_stock_info_data()
    handle_stock_daily_data()
    stock_dzjy_main()
    handle_stock_cyq_main()
