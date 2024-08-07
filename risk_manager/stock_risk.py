from indicator.common_indicator import get_stock_holder_or_reduce_risk, get_stock_margin_indicator, get_stock_last_dzjy, \
    get_stock_cyq_risk, get_stock_vol_risk, get_stock_fin_risk
from indicator.ai_model_indicator import get_model_stock_news_analysis_from_db
from utils.actions import show_data
from utils.tool import comm_read_stock
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from utils.tool import sort_dict_data_by
from risk_manager.industry_risk import get_new_industry_op_profit_risk, get_new_industry_num_loss_risk

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def read_industry_dict_code():
    with open('../stock.txt', mode='r') as f:
        lines = f.readlines()
        dict_data = {}
        for line in lines:
            splits = line.replace("\n", "").split(",")
            if len(splits) == 1:
                industry = splits[0].replace("#", "")
                dict_data[industry] = {}
            if len(splits) == 2:
                code, name = splits
                dict_data[industry][code] = name
        return dict_data


def stock_risk(codes:list=None,quarter=3):
    """
    个股风险
    :param codes:
    :return:
    """
    if codes is None:
        codes = set()
        dict_data = read_industry_dict_code()
        for vs in dict_data.values():
            vs = list(vs.keys())
            for ele in vs:
                codes.add(ele)
        codes = list(codes)
    total_risk = {k: 0 for k in codes}
    holder_or_reduce_risk = get_stock_holder_or_reduce_risk(codes)
    risk_level = holder_or_reduce_risk[1]
    print('减持风险', risk_level)
    for code, combine_dict in risk_level.items():
        total_risk[code] += combine_dict['risk_value']
    dzjy_risk = get_stock_last_dzjy(codes)
    print('大宗交易风险', dzjy_risk)
    for code, combine_dict in dzjy_risk.items():
        total_risk[code] += combine_dict['risk_value']
    # 主力筹码
    data, up_rate, cyq_risk = get_stock_cyq_risk(codes)
    print('下跌的筹码风险', cyq_risk)
    for code, risk_val in cyq_risk.items():
        total_risk[code] += risk_val
    print('上涨的收益率', up_rate)
    # 新闻数据风险
    data = get_model_stock_news_analysis_from_db(codes)
    print('新闻风险', data)
    for code, combine_dict in data.items():
        total_count = sum(list(combine_dict.values()))
        if '中性' in combine_dict.keys():
            total_risk[code] += (combine_dict['中性'] / total_count) * 0.02
        if '悲观' in combine_dict.keys():
            total_risk[code] += (combine_dict['悲观'] / total_count)
    # 个股波动率风险
    result = get_stock_vol_risk(codes)
    print('波动率风险', result)
    for code, combine_dict in result.items():
        total_risk[code] += combine_dict['atr14_rate']
    # 财报风险
    quarter_mapping = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
    data = get_stock_fin_risk(codes,quarter_mapping[quarter])
    print(f"财报风险 {data}")

    for data_type, combine_dict in data.items():
        if data_type in ['income_cycle_risk', 'total_revenue_cycle_risk', 'assets_cycle_risk',
                         'net_cash_flow_cycle_risk']:
            for code, ele_combine_dict in combine_dict.items():
                total_risk[code] += ele_combine_dict['down']
        else:
            for code, ele_combine_dict in combine_dict.items():
                total_risk[code] += ele_combine_dict['up']
    print(total_risk)
    return sort_dict_data_by(total_risk, by='value')


def stock_industry_risk(file_name: str = None):
    """
    股票行业风险
    :param file_name:
    :return:
    """
    if file_name is None:
        file_name = '../cur_my_stock_industry.txt'
    with open(file_name, mode='r') as f:
        lines = f.readlines()
        lines = [line.replace("\n", "") for line in lines]
        stock_industry_dict = {}
        for line in lines:
            splits = line.split(",")
            code, name = splits[0], splits[1]
            industry_list = splits[2:]
            stock_industry_dict[code] = {"name": name, "industry_list": industry_list}
    industry_op_profit_risk = get_new_industry_op_profit_risk()
    datas = []
    industry_num_loss_risk = get_new_industry_num_loss_risk()

    for time, risk_combine_dict in industry_op_profit_risk.items():
        loss_risk = industry_num_loss_risk[time]['total_risk']
        op_risk = industry_op_profit_risk[time]['total_risk']
        for code, combine_dict in stock_industry_dict.items():
            industry_list = combine_dict['industry_list']
            name = combine_dict['name']
            industry_loss_val, industry_profit_val = 0, 0
            for industry in industry_list:
                loss_key = industry + "亏损企业单位数_增减"
                profit_key = industry + "营业利润_累计增长"
                industry_loss_val += loss_risk[loss_key] * (1 / len(industry_list))
                industry_profit_val += op_risk[profit_key] * (1 / len(industry_list))
            industry_profit_val = round(industry_profit_val, 4)
            industry_loss_val = round(industry_loss_val, 4)
            total_risk = round((industry_loss_val + industry_profit_val) / 2, 4)
            datas.append(
                {"code": code, "name": name, "total_risk": total_risk, "industry_profit_risk": industry_profit_val,
                 "industry_loss_risk": industry_loss_val, "time": time})
    return datas


def total_macro_risk():
    pass


if __name__ == '__main__':
    code_dict = comm_read_stock('../stock.txt')
    datas = stock_risk()
    print(datas)
    print(code_dict)
    for k,v in datas.items():
        print(code_dict.get(k),v)
