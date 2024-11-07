import time

import os
from utils.actions import show_data
from utils.tool import *
from data.global_micro_data import *
from indicator.talib_indicator import common_indictator_cal
from monitor.real_common import *
from monitor.indicator_config import index_buy_indicator_config,index_sell_indicator_config
from monitor.dingtalk_msg import DingtalkSendMsg

def real_monitor_stock_index_and_cal_indicator():
    code_name = 'code'
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

    now_str = datetime.now().strftime("%Y%m%d")
    trigger_count_json_file = f"index{now_str}.json"
    if os.path.exists(trigger_count_json_file):
        json_data = load_json_data(trigger_count_json_file)
        sell_trigger_count = json_data['sell']
        buy_trigger_count = json_data['buy']
    else:
        sell_trigger_count = {}
        buy_trigger_count = {}

    code_dict = {
        "sh000001": "上证指数",
        "sz399001": "深证成指",
        "sh000852": "中证1000"
    }
    codes = list(code_dict.keys())
    condition = {code_name: {"$in": codes}, "date": {"$gte": start_date}}
    database = 'stock'
    collection = 'index_data'
    projection = {'_id': False}
    sort_key = "date"

    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['high', 'close', 'low', 'volume', 'code', 'open', 'pct_chg']
    data.index = pd.to_datetime(data[sort_key])
    data['pct_chg'] = 0
    sender = DingtalkSendMsg()
    while True:
        stock_index_zh_a_spot_em_df = try_get_action(ak.stock_zh_index_spot_sina, try_count=3)
        c_cols = {'代码': "code", '成交量': "volume", '最高': "high", '最低': "low", '最新价': "close",
                  "涨跌幅": "pct_chg", "今开": "open"}
        stock_index_zh_a_spot_em_df = stock_index_zh_a_spot_em_df[stock_index_zh_a_spot_em_df['代码'].isin(codes)][
            list(c_cols.keys())]
        stock_index_zh_a_spot_em_df.rename(columns=c_cols, inplace=True)
        stock_index_zh_a_spot_em_df['date'] = datetime.now().strftime("%Y-%m-%d")
        stock_index_zh_a_spot_em_df.index = pd.to_datetime(stock_index_zh_a_spot_em_df['date'])
        stock_index_zh_a_spot_em_df = stock_index_zh_a_spot_em_df[cols]
        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        construct_buy_msg_list = []
        construct_sell_msg_list = []

        for code in codes:
            pd_data = data[data[code_name] == code][cols]
            today_data = stock_index_zh_a_spot_em_df[stock_index_zh_a_spot_em_df[code_name] == code]
            new_data = pd.concat([pd_data, today_data])
            new_data = st_peak_data(new_data, sort_key,before_peak=-2)
            linear_result = cal_linear_data_fn(new_data[sort_key].values,new_data['close'].values,new_data['is_peak'].values,new_data['is_low'].values)
            low_keys = list(linear_result['low'].keys())
            high_keys = list(linear_result['high'].keys())
            new_data['low_linear'] = linear_result['low'][low_keys[-1]]
            new_data['high_linear'] = linear_result['high'][high_keys[-1]]
            new_data['name'] = code_dict.get(code)
            common_indictator_cal(new_data, ma_timeperiod=20,b_line_timeperiod=120)
            new_data['stop_rate'] = round((new_data['atr14'] * 3) / new_data['close'], 4)
            show_data(new_data.tail(1))

            ret_send_msg = construct_indicator_send_msg(new_data.tail(1), index_buy_indicator_config)
            name = code_dict.get(code)
            is_add = False
            for indicator_name in ret_send_msg.keys():
                if indicator_name not in ['row_data', 'other_show_indicator']:
                    combine_name = f"{name}_{indicator_name}"
                    if buy_trigger_count.get(combine_name, 0) < 2:
                        is_add = True
                        buy_trigger_count[combine_name] = buy_trigger_count.get(combine_name, 0) + 1
            if is_add:
                construct_buy_msg_list.append(ret_send_msg)

            is_add = False
            ret_send_msg = construct_indicator_send_msg(new_data.tail(1), index_sell_indicator_config)
            for indicator_name in ret_send_msg.keys():
                if indicator_name not in ['row_data', 'other_show_indicator']:
                    combine_name = f"{name}_{indicator_name}"
                    if sell_trigger_count.get(combine_name, 0) < 2:
                        is_add = True
                        sell_trigger_count[combine_name] = sell_trigger_count.get(combine_name, 0) + 1

            if is_add:
                construct_sell_msg_list.append(ret_send_msg)

        trigger_json_data = {}
        if len(construct_buy_msg_list) > 0:
            #comm_indicator_send_msg_by_email(construct_buy_msg_list, sender, msg_title='实时指数指标触发买入的信号')
            sender.send_msg(type='ticker_trigger_msg',data_list=construct_buy_msg_list,msg_title='实时指数指标触发买入的信号')

            trigger_json_data['sell'] = sell_trigger_count
            trigger_json_data['buy'] = buy_trigger_count

        if len(construct_sell_msg_list) > 0:
            #comm_indicator_send_msg_by_email(construct_sell_msg_list, sender, msg_title='实时指数指标触发卖出的信号')
            sender.send_msg(type='ticker_trigger_msg',data_list=construct_sell_msg_list,msg_title='实时指数指标触发卖出的信号')

            trigger_json_data['sell'] = sell_trigger_count
            trigger_json_data['buy'] = buy_trigger_count
        if len(trigger_json_data.keys()) > 0:
            dump_json_data(trigger_count_json_file, trigger_json_data)

        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        time.sleep(80)


if __name__ == '__main__':
    real_monitor_stock_index_and_cal_indicator()
