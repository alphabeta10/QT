import os.path
import time

from utils.actions import show_data
from data.global_micro_data import *
from indicator.talib_indicator import common_indictator_cal
from monitor.real_common import construct_indicator_send_msg, comm_indicator_send_msg_by_email,st_peak_data
from utils.send_msg import MailSender
from utils.tool import *
from monitor.indicator_config import buy_indicator_config,sell_indicator_config

def real_monitor_stock_and_cal_indicator():
    code_name = 'code'
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

    now_str = datetime.now().strftime("%Y%m%d")
    trigger_count_json_file = f"stock{now_str}.json"
    if os.path.exists(trigger_count_json_file):
        json_data = load_json_data(trigger_count_json_file)
        sell_trigger_count = json_data['sell']
        buy_trigger_count = json_data['buy']
    else:
        sell_trigger_count = {}
        buy_trigger_count = {}
    code_dict = comm_read_stock('../stock.txt')
    codes = list(code_dict.keys())
    condition = {code_name: {"$in": codes}, "time": {"$gte": start_date}}
    database = 'stock'
    collection = 'ticker_daily'
    projection = {'_id': False}
    sort_key = "time"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['high', 'close', 'low', 'volume', 'code', 'open', 'pct_chg']
    data.index = pd.to_datetime(data[sort_key])
    sender = MailSender()
    while True:
        stock_zh_a_spot_em_df = try_get_action(ak.stock_zh_a_spot_em, try_count=3)
        c_cols = {'代码': "code", '成交量': "volume", '最高': "high", '最低': "low", '最新价': "close",
                  "涨跌幅": "pct_chg", "今开": "open"}
        stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df['代码'].isin(codes)][list(c_cols.keys())]
        stock_zh_a_spot_em_df.rename(columns=c_cols, inplace=True)
        stock_zh_a_spot_em_df['time'] = datetime.now().strftime("%Y-%m-%d")
        stock_zh_a_spot_em_df.index = pd.to_datetime(stock_zh_a_spot_em_df['time'])
        stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[cols]
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        construct_buy_msg_list = []
        construct_sell_msg_list = []
        for code in codes:
            pd_data = data[data[code_name] == code][cols]
            today_data = stock_zh_a_spot_em_df[stock_zh_a_spot_em_df[code_name] == code]
            new_data = pd.concat([pd_data, today_data])
            new_data = st_peak_data(new_data,sort_key)
            new_data['name'] = code_dict.get(code)
            common_indictator_cal(new_data, ma_timeperiod=20)
            new_data['stop_rate'] = round((new_data['atr14'] * 2) / new_data['close'], 4)
            show_data(new_data.tail(1))
            ret_send_msg = construct_indicator_send_msg(new_data.tail(1), buy_indicator_config)
            name = code_dict.get(code)
            for indicator_name in ret_send_msg.keys():
                if indicator_name not in ['row_data','other_show_indicator']:
                    combine_name = f"{name}_{indicator_name}"
                    if buy_trigger_count.get(combine_name, 0) < 2:
                        construct_buy_msg_list.append(ret_send_msg)
                        buy_trigger_count[combine_name] = buy_trigger_count.get(combine_name, 0) + 1

            ret_send_msg = construct_indicator_send_msg(new_data.tail(1), sell_indicator_config)
            for indicator_name in ret_send_msg.keys():
                if indicator_name not in ['row_data','other_show_indicator']:
                    combine_name = f"{name}_{indicator_name}"
                    if sell_trigger_count.get(combine_name, 0) < 2:
                        construct_sell_msg_list.append(ret_send_msg)
                        sell_trigger_count[combine_name] = sell_trigger_count.get(combine_name, 0) + 1
        trigger_json_data  = {}
        if len(construct_buy_msg_list) > 0:
            comm_indicator_send_msg_by_email(construct_buy_msg_list, sender, msg_title='实时股票指标触发买入的信号')
            trigger_json_data['sell'] = sell_trigger_count
            trigger_json_data['buy'] = buy_trigger_count

        if len(construct_sell_msg_list) > 0:
            comm_indicator_send_msg_by_email(construct_sell_msg_list, sender, msg_title='实时股票指标触发卖出的信号')
            trigger_json_data['sell'] = sell_trigger_count
            trigger_json_data['buy'] = buy_trigger_count
        if len(trigger_json_data.keys())>0:
            dump_json_data(trigger_count_json_file,trigger_json_data)

        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        print(f"****************{now_time}****************")
        time.sleep(80)


if __name__ == '__main__':
    real_monitor_stock_and_cal_indicator()
