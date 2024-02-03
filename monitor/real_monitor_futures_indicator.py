import time

from utils.send_msg import MailSender
from utils.actions import show_data
from data.global_micro_data import *
from indicator.talib_indicator import common_indictator_cal
from utils.tool import *
import os
from monitor.real_common import *
pd.set_option('display.precision', 4)


def real_monitor_futures_and_cal_indicator():
    buy_indicator_config = {"K": {"range": [0, 20], "name": "KDJ的K值在范围[0,20]"},
                            "pre_K": {"range": [0, 20], "name": "pre_KDJ的K值在范围[0,20]"},
                            "自定义20日均线": {"gt": "close", "name": "20日均线大于收盘价"},
                            "D": {"range": [0, 20], "name": "KDJ的D值在范围[0,20]"},
                            "pre_D": {"range": [0, 20], "name": "prd_KDJ的D值在范围[0,20]"},
                            "rsi12": {"range": [0, 20], "name": "rsi12值在范围[0,20]"},
                            }

    sell_indicator_config = {"K": {"range": [70, 100], "name": "KDJ的K值在范围[70, 100]"},
                             "pre_K": {"range": [70, 100], "name": "pre_KDJ的K值在范围[70, 100]"},
                             "D": {"range": [70, 100], "name": "KDJ的D值在范围[70, 100]"},
                             "pre_D": {"range": [70, 100], "name": "pre_KDJ的D值在范围[70, 100]"},
                             "rsi12": {"range": [70, 100], "name": "rsi12值在范围[70,100]"},
                             }

    code_name = 'symbol'
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

    now_str = datetime.now().strftime("%Y%m%d")
    trigger_count_json_file = f"futures{now_str}.json"
    if os.path.exists(trigger_count_json_file):
        json_data = load_json_data(trigger_count_json_file)
        sell_trigger_count = json_data['sell']
        buy_trigger_count = json_data['buy']
    else:
        sell_trigger_count = {}
        buy_trigger_count = {}


    code_config_list = [
        {"name": "玻璃连续", "market_type": "CF", "code": "FG0", "stop_price": 1000, "stop_time": 1,
         "contract_chg_price": 20},
        {"name": "豆二连续", "market_type": "CF", "code": "B0", "stop_price": 1000, "stop_time": 1,
         "contract_chg_price": 10},
        {"name": "螺纹钢连续", "market_type": "CF", "code": "RB0", "stop_price": 1000, "stop_time": 1,
         "contract_chg_price": 10},
        {"name": "乙二醇连续", "market_type": "CF", "code": "EG0", "stop_price": 1000, "stop_time": 1,
         "contract_chg_price": 10},
    ]

    comm_info_dict = {"name": "名称", "close": "C", "open": "O", "high": "H", "low": "L"}
    codes = [ele['code'] for ele in code_config_list]
    condition = {code_name: {"$in": codes}, "date": {"$gte": start_date}}
    database = 'futures'
    collection = 'futures_daily'
    projection = {'_id': False}
    sort_key = "date"
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['high', 'close', 'low', 'volume', 'symbol','open']
    data.index = pd.to_datetime(data[sort_key])
    data = data[cols]
    cf_code = ",".join([ele['code'] for ele in code_config_list if ele['market_type'] == 'CF'])
    ff_code = ",".join([ele['code'] for ele in code_config_list if ele['market_type'] == 'FF'])
    print(f"cf code {cf_code}")
    print(f"ff code {ff_code}")
    r_col = ['high', 'current_price', 'low', 'volume', 'symbol','open']
    sender = MailSender()
    while True:
        cf_futures_zh_spot_df = None
        ff_futures_zh_spot_df = None
        hour = int(datetime.now().strftime("%H"))
        if hour>20:
            new_date_str = (datetime.now()+timedelta(days=1)).strftime("%Y-%m-%d")
        else:
            new_date_str = datetime.now().strftime("%Y-%m-%d")
        if cf_code != '':
            cf_futures_zh_spot_df = ak.futures_zh_spot(symbol=cf_code, market="CF", adjust='0')
            cf_futures_zh_spot_df['date'] = new_date_str
            cf_futures_zh_spot_df.index = pd.to_datetime(cf_futures_zh_spot_df['date'])
            cf_futures_zh_spot_df = cf_futures_zh_spot_df[r_col]
        if ff_code != '':
            ff_futures_zh_spot_df = ak.futures_zh_spot(symbol=ff_code, market="FF", adjust='0')
            ff_futures_zh_spot_df['date'] = new_date_str
            ff_futures_zh_spot_df.index = pd.to_datetime(ff_futures_zh_spot_df['date'])
            ff_futures_zh_spot_df = ff_futures_zh_spot_df[r_col]

        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        construct_buy_msg_list = []
        construct_sell_msg_list = []
        for futures_cf in code_config_list:
            name = futures_cf['name']
            code = futures_cf['code']
            market_type = futures_cf['market_type']
            pd_data = data[data[code_name] == code][cols]
            new_data = None
            if cf_futures_zh_spot_df is not None and market_type == 'CF':
                new_data = cf_futures_zh_spot_df[cf_futures_zh_spot_df['symbol'] == name]
                new_data['symbol'] = code
                new_data.rename(columns={"current_price": "close"}, inplace=True)
            if ff_futures_zh_spot_df is not None and market_type == 'FF':
                new_data = ff_futures_zh_spot_df[ff_futures_zh_spot_df['symbol'] == name]
                new_data['symbol'] = code
                new_data.rename(columns={"current_price": "close"}, inplace=True)
            if new_data is not None:
                new_data = pd.concat([pd_data, new_data])
                common_indictator_cal(new_data, ma_timeperiod=20)
                new_data['name'] = name
                new_data['position'] = futures_cf['stop_price'] / (
                            new_data['atr14'] * futures_cf['stop_time'] * futures_cf['contract_chg_price'])
                show_data(new_data.tail(1))

                ret_send_msg = construct_indicator_send_msg(new_data.tail(1), buy_indicator_config)

                if len(ret_send_msg.keys()) > 0 and buy_trigger_count.get(name, 0) < 2:
                    construct_buy_msg_list.append(ret_send_msg)
                    buy_trigger_count[name] = buy_trigger_count.get(name, 0) + 1

                ret_send_msg = construct_indicator_send_msg(new_data.tail(1), sell_indicator_config)
                if len(ret_send_msg.keys()) > 0 and sell_trigger_count.get(name, 0) < 2:
                    construct_sell_msg_list.append(ret_send_msg)
                    sell_trigger_count[name] = sell_trigger_count.get(name, 0) + 1
        trigger_json_data = {}
        if len(construct_buy_msg_list) > 0:
            comm_indicator_send_msg_by_email(construct_buy_msg_list, sender, msg_title='实时指期货标触发买入的信号',comm_info_dict=comm_info_dict)
            trigger_json_data['sell'] = sell_trigger_count
            trigger_json_data['buy'] = buy_trigger_count

        if len(construct_sell_msg_list) > 0:
            comm_indicator_send_msg_by_email(construct_sell_msg_list, sender, msg_title='实时期货指标触发卖出的信号',comm_info_dict=comm_info_dict)
            trigger_json_data['sell'] = sell_trigger_count
            trigger_json_data['buy'] = buy_trigger_count
        if len(trigger_json_data.keys()) > 0:
            dump_json_data(trigger_count_json_file, trigger_json_data)

        now_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"****************{now_time}****************")
        time.sleep(80)


def get_futures_name():
    cf_futures_zh_spot_df = ak.futures_zh_spot(symbol='B0,FG0', market="CF", adjust='0')
    show_data(cf_futures_zh_spot_df)


if __name__ == '__main__':
    real_monitor_futures_and_cal_indicator()
