import pandas as pd
import akshare as ak
from data.mongodb import get_mongo_table
from utils.actions import try_get_action
from pymongo import UpdateOne
from datetime import datetime

def handle_data(df_data: pd.DataFrame, data_type, data_date):
    request_update = []
    if data_type == 'lrb':
        for index in df_data.index:
            data = df_data.loc[index]
            code = data['股票代码']
            name = data['股票简称']
            net_profit = data['净利润']
            net_profit_cycle = data['净利润同比']
            total_revenue = data['营业总收入']
            total_revenue_cycle = data['营业总收入同比']
            oper_exp = data['营业总支出-营业支出']
            sell_exp = data['营业总支出-销售费用']
            admin_exp = data['营业总支出-管理费用']
            fin_exp = data['营业总支出-财务费用']
            total_oper_exp = data['营业总支出-营业总支出']
            operate_profit = data['营业利润']
            total_profit = data['利润总额']
            ann_date = data['公告日期']
            dict_data = {
                "code": code,
                "name": name,
                'income': float(net_profit),
                'income_cycle': float(net_profit_cycle),
                'total_revenue': float(total_revenue),
                'total_revenue_cycle': float(total_revenue_cycle),
                'oper_exp': float(oper_exp),
                'sell_exp': float(sell_exp),
                'admin_exp': float(admin_exp),
                'fin_exp': float(fin_exp),
                'total_oper_exp': float(total_oper_exp),
                'operate_profit': float(operate_profit),
                'total_profit': float(total_profit),
                "ann_date": str(ann_date),
                "date": data_date,
                "data_type": data_type
            }

            request_update.append(UpdateOne(
                {"code": dict_data['code'], "date": data_date, "date_type": data_type},
                {"$set": dict_data},
                upsert=True))
    elif data_type == 'zcfz':
        for index in df_data.index:
            zcfz_data = df_data.loc[index]
            code = zcfz_data['股票代码']
            name = zcfz_data['股票简称']
            money_cap = zcfz_data['资产-货币资金']
            accounts_receiv = zcfz_data['资产-应收账款']
            inventories = zcfz_data['资产-存货']
            total_assets = zcfz_data['资产-总资产']
            assets_cycle = zcfz_data['资产-总资产同比']
            lia_acct_payable = zcfz_data['负债-应付账款']
            lia_acct_receiv = zcfz_data['负债-预收账款']
            lia_assets = zcfz_data['负债-总负债']
            lia_assets_cycle = zcfz_data['负债-总负债同比']
            lia_assets_rate = zcfz_data['资产负债率']
            total_hldr_eqy_exc_min_int = zcfz_data['股东权益合计']
            ann_date = zcfz_data['公告日期']

            dict_data = {
                "code": code,
                "name": name,
                "money_cap": float(money_cap),
                "accounts_receiv": float(accounts_receiv),
                "inventories": float(inventories),
                "total_assets": float(total_assets),
                "assets_cycle": float(assets_cycle),
                "lia_acct_payable": float(lia_acct_payable),
                "lia_acct_receiv": float(lia_acct_receiv),
                "lia_assets": float(lia_assets),
                "lia_assets_cycle": float(lia_assets_cycle),
                "lia_assets_rate": float(lia_assets_rate),
                "total_hldr_eqy_exc_min_int": float(total_hldr_eqy_exc_min_int),
                "ann_date": str(ann_date),
                "date": data_date,
                "data_type": data_type
            }

            request_update.append(UpdateOne(
                {"code": dict_data['code'], "date": data_date, "data_type": data_type},
                {"$set": dict_data},
                upsert=True))
    elif data_type == 'xjll':
        for index in df_data.index:
            data = df_data.loc[index]
            code = data['股票代码']
            name = data['股票简称']
            net_cash_flow = data['净现金流-净现金流']
            net_cash_flow_cycle = data['净现金流-同比增长']
            net_oper_cash_flow = data['经营性现金流-现金流量净额']
            net_oper_cash_flow_rate = data['经营性现金流-净现金流占比']
            invest_cash_flow = data['投资性现金流-现金流量净额']
            invest_cash_flow_rate = data['投资性现金流-净现金流占比']
            fa_cash_flow = data['融资性现金流-现金流量净额']
            fa_cash_flow_rate = data['融资性现金流-净现金流占比']
            ann_date = data['公告日期']

            dict_data = {
                "code": code,
                "name": name,
                'net_cash_flow': float(net_cash_flow),
                'net_cash_flow_cycle': float(net_cash_flow_cycle),
                'net_oper_cash_flow': float(net_oper_cash_flow),
                'net_oper_cash_flow_rate': float(net_oper_cash_flow_rate),
                'invest_cash_flow': float(invest_cash_flow),
                'invest_cash_flow_rate': float(invest_cash_flow_rate),
                'fa_cash_flow': float(fa_cash_flow),
                'fa_cash_flow_rate': float(fa_cash_flow_rate),
                "ann_date": str(ann_date),
                "date": data_date,
                "data_type": data_type
            }

            request_update.append(UpdateOne(
                {"code": dict_data['code'], "date": data_date, "data_type": data_type},
                {"$set": dict_data},
                upsert=True))
    elif data_type == 'yjbb':
        for index in df_data.index:
            data = df_data.loc[index]
            code = data['股票代码']
            name = data['股票简称']
            net_per_share = data['每股收益']
            total_revenue = data['营业收入-营业收入']
            total_revenue_cylce_in = data['营业收入-同比增长']
            total_revenue_q_same_in = data['营业收入-季度环比增长']
            net_profit = data['净利润-净利润']
            net_profit_cycle_in = data['净利润-同比增长']
            net_profit_q_same_in = data['净利润-季度环比增长']
            net_assert_per_share = data['每股净资产']
            assert_ret = data['净资产收益率']
            oper_cash_flow_per_share = data['每股经营现金流量']
            gross_profit_ratio = data['销售毛利率']
            industry = data['所处行业']
            ann_date = data['最新公告日期']

            dict_data = {
                "code": code,
                "name": name,
                'net_per_share': float(net_per_share),
                'total_revenue': float(total_revenue),
                'total_revenue_cylce_in': float(total_revenue_cylce_in),
                'total_revenue_q_same_in': float(total_revenue_q_same_in),
                'net_profit': float(net_profit),
                'net_profit_cycle_in': float(net_profit_cycle_in),
                'net_profit_q_same_in': float(net_profit_q_same_in),
                'net_assert_per_share': float(net_assert_per_share),
                'assert_ret': float(assert_ret),
                'oper_cash_flow_per_share': float(oper_cash_flow_per_share),
                'gross_profit_ratio': float(gross_profit_ratio),
                'industry': industry,
                "ann_date": str(ann_date),
                "date": data_date,
                "data_type": data_type
            }

            request_update.append(UpdateOne(
                {"code": dict_data['code'], "date": data_date, "data_type": data_type},
                {"$set": dict_data},
                upsert=True))
    return request_update


def handle_fin_data(start_date='2010-01-01', end_date=datetime.now().strftime("%Y-%m-%d")):
    quater_list = ["0331", "0630", "0930", "1231"]
    dates = pd.date_range(start_date, end_date, freq='Y')
    dates = set([str(date)[0:4] for date in dates.values])
    dates.add(end_date[0:4])
    dates = list(set(dates))
    dates = sorted(dates)
    start_date_int = int(start_date.replace("-",""))
    new_dates = []
    for quater in quater_list:
        for date in dates:
            cur = int(f"{date}{quater}")
            now_int = int(end_date.replace("-",""))
            if now_int>=cur and cur>=start_date_int:
                new_dates.append(f"{date}{quater}")

    fin_col = get_mongo_table(collection='fin_simple')

    for date in new_dates:
        print(f"get date {date}")
        data_type_fn_mapping = {
            "lrb":ak.stock_lrb_em,
            "zcfz":ak.stock_zcfz_em,
            "xjll":ak.stock_xjll_em,
            "yjbb":ak.stock_yjbb_em
        }
        for data_type,fn in data_type_fn_mapping.items():
            df_data = try_get_action(fn,try_count=3,date=date)
            if df_data is not None:
                request_update = handle_data(df_data,data_type,date)
                if len(request_update) > 0:
                    update_result = fin_col.bulk_write(request_update, ordered=False)
                    print('data_type:%s 插入：%4d条, 更新：%4d条' %
                          (data_type,update_result.upserted_count, update_result.modified_count),
                          flush=True)
def create_index():
    fin_col = get_mongo_table(collection='fin_simple')
    fin_col.create_index([("code", 1), ("data_type", 1), ("date", 1)], unique=True, background=True)
def get_data():
    fin_col = get_mongo_table(collection='fin_simple')
    ret = fin_col.find({"code": "000001"},projection={'_id': False,'code':True,'date':True}).sort("date")
    for ele in ret:
        print(ele)

if __name__ == '__main__':
    handle_fin_data(start_date='2024-01-01')



