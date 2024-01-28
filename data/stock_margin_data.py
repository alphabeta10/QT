import time

import akshare as ak
from utils.actions import try_get_action
from pymongo import UpdateOne
from data.mongodb import get_mongo_table
from utils.tool import mongo_bulk_write_data
from datetime import datetime, timedelta


def handle_margin_sz_sh_total_data():
    sz_mapping_cf = {"融资买入额": "margin_trade_amount", "融资余额": "margin_trade_balance",
                     "融券卖出量": "short_sell_volume", "融券余量": "short_sell_balance_volume",
                     "融券余额": "short_sell_balance", "融资融券余额": "margin_trade_short_sell_balance"}
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    macro_china_market_margin_sz_df = try_get_action(ak.macro_china_market_margin_sz, try_count=3)
    update_request = []
    for index in macro_china_market_margin_sz_df.index:
        dict_data = dict(macro_china_market_margin_sz_df.loc[index])
        time = str(index)[0:10]

        in_db_data = {"data_type": "margin_data", "metric_code": "margin_sz", "time": time}
        for k, v in dict_data.items():
            in_db_data[sz_mapping_cf.get(k)] = v
        update_request.append(
            UpdateOne(
                {"data_type": in_db_data['data_type'], "time": in_db_data['time'],
                 "metric_code": in_db_data['metric_code']},
                {"$set": in_db_data},
                upsert=True)
        )
        if len(update_request) % 1000 == 0:
            mongo_bulk_write_data(stock_common, update_request)
            update_request.clear()

    if len(update_request) > 0:
        mongo_bulk_write_data(stock_common, update_request)
        update_request.clear()

    sh_mapping_cf = {'日期': 'date', '融资余额': 'margin_trade_balance', '融资买入额': 'margin_trade_amount',
                     '融券余量': 'short_sell_volume', '融券余额': 'short_sell_balance',
                     '融券卖出量': 'short_sell_balance_volume',
                     '融资融券余额': 'margin_trade_short_sell_balance'}
    macro_china_market_margin_sh_df = try_get_action(ak.macro_china_market_margin_sh, try_count=3)
    update_request = []
    for index in macro_china_market_margin_sh_df.index:
        dict_data = dict(macro_china_market_margin_sh_df.loc[index])
        in_db_data = {"data_type": "margin_data", "metric_code": "margin_sh"}
        for k, v in dict_data.items():
            if k == '日期':
                in_db_data['time'] = str(v)[0:10]
            else:
                in_db_data[sh_mapping_cf.get(k)] = v
        update_request.append(
            UpdateOne(
                {"data_type": in_db_data['data_type'], "time": in_db_data['time'],
                 "metric_code": in_db_data['metric_code']},
                {"$set": in_db_data},
                upsert=True)
        )

        if len(update_request) % 1000 == 0:
            mongo_bulk_write_data(stock_common, update_request)
            update_request.clear()

    if len(update_request) > 0:
        mongo_bulk_write_data(stock_common, update_request)
        update_request.clear()


def handle_simple_sz_margin_data(start_date_str=None):
    simple_sz_mapping_cf = {'融资买入额': 'margin_trade_amount', '融资余额': 'margin_trade_balance',
                            '融券卖出量': 'short_sell_volume', '融券余量': 'short_sell_balance_volume',
                            '融券余额': 'short_sell_balance',
                            '融资融券余额': 'margin_trade_short_sell_balance'}
    if start_date_str is None:
        before_day = datetime.now() - timedelta(days=15)
        start_date_str = before_day.strftime("%Y%m%d")
    now_int = int(datetime.now().strftime("%Y%m%d"))
    tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
    trade_dates = [str(ele)[0:10].replace("-", "") for ele in tool_trade_date_hist_sina_df['trade_date'].values if
                   int(str(ele)[0:10].replace("-", "")) >= int(start_date_str) and int(
                       str(ele)[0:10].replace("-", "")) < now_int]
    if len(trade_dates) > 0:
        update_request = []
        stock_common = get_mongo_table(database='stock', collection='common_seq_data')
        for date_str in trade_dates:
            print(f"handel {date_str}")
            stock_margin_sse_df = try_get_action(ak.stock_margin_szse, try_count=3, date=date_str)
            for index in stock_margin_sse_df.index:
                dict_data = dict(stock_margin_sse_df.loc[index])
                in_db_data = {"data_type": "margin_data", "metric_code": "margin_sz_simple", "time": date_str}
                for k, v in dict_data.items():
                    in_db_data[simple_sz_mapping_cf.get(k)] = v
                update_request.append(
                    UpdateOne(
                        {"data_type": in_db_data['data_type'], "time": in_db_data['time'],
                         "metric_code": in_db_data['metric_code']},
                        {"$set": in_db_data},
                        upsert=True)
                )

        if len(update_request) > 0:
            mongo_bulk_write_data(stock_common, update_request)
            update_request.clear()


def delete_data():
    stock_common = get_mongo_table(database='stock', collection='common_seq_data')
    x = stock_common.delete_many({"metric_code": "margin_data"})
    print(x.deleted_count)


if __name__ == '__main__':
    handle_simple_sz_margin_data()
    handle_margin_sz_sh_total_data()