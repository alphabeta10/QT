from indicator.common_indicator import get_stock_holder_or_reduce_risk, get_stock_margin_indicator, get_stock_last_dzjy, \
    get_stock_cyq_risk, get_stock_vol_risk, get_stock_fin_risk
from indicator.ai_model_indicator import get_model_stock_news_analysis_from_db
from utils.actions import show_data
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from utils.tool import sort_dict_data_by

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


def main_risk():
    # 宏观风险 中性，
    # 宏观风险
    cash = 50000
    # 风险和报偿比
    # 减持风险
    codes = set()
    dict_data = read_industry_dict_code()
    for vs in dict_data.values():
        vs = list(vs.keys())
        for ele in vs:
            codes.add(ele)
    codes = list(codes)
    industry_dict = {'科技': ['300124', '002602', '600050', '002415'], "啤酒": ['', ''], }
    total_risk = {k: 0 for k in codes}
    x = get_stock_holder_or_reduce_risk(codes)
    risk_level = x[1]
    print('减持风险', risk_level)
    for code, combine_dict in risk_level.items():
        total_risk[code] += combine_dict['risk_value']
    # 融资融券风险
    # data, risk = get_stock_margin_indicator(codes[0])
    # print(risk)

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
    data = get_stock_fin_risk(codes)
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
    print(sort_dict_data_by(total_risk, by='value'))
    industry_risk_dict = {}
    for type_, combine_stocks in dict_data.items():
        stocks = list(combine_stocks.keys())
        total_sum = sum([1 - v for k, v in total_risk.items() if k in stocks])
        industry_risk_dict[type_] = np.mean([v for k, v in total_risk.items() if k in stocks])

        print(f'start handel {type_}')
        for code in stocks:
            name = combine_stocks.get(code)
            rate = (1 - total_risk[code]) / total_sum
            if rate < 0:
                invent_cash = 0
            else:
                invent_cash = cash * rate
            print(name, rate, invent_cash, code, up_rate[code], total_risk[code])
        print(f'end handel {type_}')
    # 行业风险
    print(sort_dict_data_by(industry_risk_dict,by='value'))
    for k,v in sort_dict_data_by(industry_risk_dict,by='value').items():
        print(k,(1-v)*cash,v)


if __name__ == '__main__':
    main_risk()
