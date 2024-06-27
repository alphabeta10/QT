import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
import akshare as ak
from data.mongodb import get_mongo_table
from pymongo import UpdateOne
from utils.actions import try_get_action
from utils.tool import mongo_bulk_write_data



def stock_ggcg():
    stock_seq_daily = get_mongo_table(collection='stock_seq_daily')
    ret_data = try_get_action(ak.stock_ggcg_em,try_count=3,symbol="全部")
    request_update = []
    col_mapping = {'代码': 'metric_key', '名称': 'name', '最新价': 'price','涨跌幅':'pct_change', '股东名称': 'sub_key',
     '持股变动信息-增减': 'shareholding_change_overweight', '持股变动信息-变动数量': 'shareholding_change_num', '持股变动信息-占总股本比例':'shareholding_change_total_share_rate',
     '持股变动信息-占流通股比例': 'shareholding_change_outstanding_share_rate', '变动后持股情况-持股总数':'after_shareholding_change_total_held', '变动后持股情况-占总股本比例': 'after_shareholding_change_total_share_rate',
     '变动后持股情况-持流通股数':'after_shareholding_change_outstanding_share_held', '变动后持股情况-占流通股比例': 'after_shareholding_change_outstanding_share_rate', '变动开始日':'start_time',
     '变动截止日':'time', '公告日': 'ann_time',}

    if ret_data is not None and len(ret_data)>0:
        for index in ret_data.index:
            dict_data = dict(ret_data.loc[index])
            new_dict = {}
            for raw_key,key in col_mapping.items():
                if raw_key in dict_data.keys():
                    val = str(dict_data.get(raw_key))
                    if val=='nan':
                        val = ''
                    new_dict[key] = val
                else:
                    print(f"no maaping data key {raw_key}")
            new_dict['sub_key'] = new_dict['sub_key']+"_"+new_dict['shareholding_change_overweight']
            #metric_key:一级编码,sub_key：二级编码,time：时间
            request_update.append(UpdateOne(
                {"metric_key": new_dict['metric_key'], "sub_key": new_dict['sub_key'],"time":new_dict['time']},
                {"$set": new_dict},
                upsert=True))
            if len(request_update)>100:
                mongo_bulk_write_data(stock_seq_daily,request_update)
                request_update.clear()
    if len(request_update) > 0:
        mongo_bulk_write_data(stock_seq_daily, request_update)
        request_update.clear()

def create_index():
    stock_seq_daily = get_mongo_table(database='stock', collection='stock_seq_daily')
    stock_seq_daily.create_index([("metric_key",1),("sub_key",1),('time',1)],unique=True,background=True)

if __name__ == '__main__':
    stock_ggcg()