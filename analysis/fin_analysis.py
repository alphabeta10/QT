import pandas as pd
from data.mongodb import get_mongo_table
from utils.actions import show_data
import matplotlib.pyplot as plt
import copy
import google.generativeai as genai
import numpy as np
from utils.tool import load_json_data
from datetime import datetime,timedelta
# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
from big_models.google_api import handle_model_table_data, simple_big_gen_model_fn
from utils.actions import try_get_action

warnings.filterwarnings('ignore')
from data.stock_detail_fin import handle_comm_stock_fin_em, handle_fin_analysis_indicator
from pymongo import UpdateOne
from utils.tool import mongo_bulk_write_data,get_data_from_mongo, sort_dict_data_by
from analysis.analysis_tool import convert_pd_data_to_month_data
def get_data(cods=None, dtype="fin_indicator", projection=None,date=None):
    if projection is None:
        projection = {"_id": False}
    if cods is None:
        cods = ['603288', '601009', '600036', '002507', '002385', '603363']
    if date is None:
        date = (datetime.now() -timedelta(days=1825)).strftime("%Y-%m-%d")
    fin_col = get_mongo_table(collection='fin')
    ret = fin_col.find({"code": {"$in": cods},"date":{"$gte":date}, "data_type": dtype}, projection=projection).sort("date")
    datas = []
    for ele in ret:
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    return pd_data


def plot_bar_line_list(x, bar_y_list, line_z_list, bar_label_list, line_label_list, title_name):
    # 绘制柱图
    for bar_y, bar_label in zip(bar_y_list, bar_label_list):
        plt.bar(x=x, height=bar_y, label=bar_label, alpha=0.8)
    # 在左侧显示图例
    plt.legend(loc='upper left')
    # 设置标题
    plt.title(title_name)
    plt.xlabel("日期")
    plt.ylabel("数值")

    # 画折线图
    ax2 = plt.twinx()
    ax2.set_ylabel("同比")
    # 设置y坐标范围
    # ax2.set_ylim([-100,100])
    for line_z, line_label in zip(line_z_list, line_label_list):
        plt.plot(x, line_z, linewidth='1', label=line_label)
    # 显示数字
    for line_z in line_z_list:
        for a, b in zip(x, line_z):
            plt.text(a, b, b, ha='center', va='bottom', fontsize=8)
    plt.legend(loc="upper right")
    plt.show()


def plot_bar_line(x, bar_y, line_z, bar_label, line_label):
    # 绘制柱图
    plt.bar(x=x, height=bar_y, label=bar_label, color='Coral', alpha=0.8)
    # 在左侧显示图例
    plt.legend(loc='upper left')
    # 设置标题
    plt.title("Detection results")
    plt.xlabel("日期")
    plt.ylabel("数值")

    # 画折线图
    ax2 = plt.twinx()
    ax2.set_ylabel("同比")
    # 设置y坐标范围
    # ax2.set_ylim([-100,100])
    plt.plot(x, line_z, "r", marker='.', c='r', ms=5, linewidth='1', label=line_label)
    # 显示数字
    for a, b in zip(x, line_z):
        plt.text(a, b, b, ha='center', va='bottom', fontsize=8)

    plt.legend(loc="upper right")
    plt.show()


def get_fin_assets_metric(code_list, isDataFromLocal=True,start_date=None):
    """
    资产负债率
    财务状况分析 偿债能力指标
        1.流动资产比率 = 流动资产/总资产 反映企业商业的活跃性，看该值的变化是否对利润增长有啥作用
        2.应收账款率= 应收票据及应收账款/流动资产合计 该值很大，可能有应收账款违约的风险，给企业经验带来风险，但这个值很大，
        也可能企业在扩展业务阶段，延迟收款，以具体情况判读
        3.应收账款增长率= (应收票据及应收账款-上年同期应收票据及应收账款)/上年同期应收票据及应收账款
        4.存货率 = 存货/流动资产合计 该值大，近年都高，可能商品不畅销
        5.资本负债率=（有息负债-现金和现金等价物）/ 所有者权益
        有息负债=短期借款+长期借款+应付票据
        现金和现金等价物=货币资金+其他流动资产
        所有者权益=归属于母公司股东权益总计
        6.资产负债率=负债总额/资产总额
         总资产=负债+所有者权益
        7,产权比率=负债总额/股东权益总额 大于1说明企业还债压力大，
        8.股东权益比率=股东权益总额/资产总额 过小说明企业负债，过高说明企业较少用到财务杠杆，对于企业生产规模及市场占有率的扩大，较高的财务杠杆也不是坏事
        9.平均资本总额 = (年初实收资本+年初资本公积极+年末实收资本+年末资本公积)/2
        10.现金比率 = （ 货币资金 + 交易性金融资产 ）/ 流动负债
        11.速动比率 = （流动资产-存货）/ 流动负债
        12.流动比率 = 流动资产 / 流动负债
    :param code_list:
    :param isDataFromLocal:
    :return:
    """

    def convert_ele(ele):
        if ele == '--' or str(ele) == 'nan':
            ele = 0
        if float(ele) < 0:
            return 0
        return float(ele)

    local_codes = [code[2:] for code in code_list]
    if isDataFromLocal is False:
        handle_comm_stock_fin_em(codes=code_list, data_type="zcfz_report_detail")
    get_col_dict = {"MONETARYFUNDS": "货币资金",
                    "INVENTORY": "存货",
                    "ACCOUNTS_RECE": "应收账款",
                    "PREPAYMENT": "预付款项",
                    "TOTAL_ASSETS": "资产总计",
                    "TOTAL_LIABILITIES": "负债合计",
                    "INTANGIBLE_ASSET": "无形资产",
                    "NOTE_ACCOUNTS_PAYABLE": "应付票据及应付账款",
                    "ACCOUNTS_PAYABLE": "其中:应付账款",
                    "LONG_LOAN": "长期借款",
                    "SHORT_LOAN": "短期借款",
                    "NOTE_ACCOUNTS_RECE": "应收票据及应收账款",
                    "CIP": "在建工程",
                    "FIXED_ASSET": "固定资产",
                    "TOTAL_CURRENT_ASSETS": "流动资产合计",
                    "TOTAL_PARENT_EQUITY": "归属于母公司股东权益总计",
                    'TOTAL_EQUITY': "股东权益合计",
                    'OTHER_CURRENT_ASSET': '其他流动资产',
                    'NOTE_PAYABLE': "其中:应付票据",
                    'BOND_PAYABLE': "应付债券",
                    'SHARE_CAPITAL': '实收资本（或股本',
                    'CAPITAL_RESERVE': '资本公积',
                    'TOTAL_CURRENT_LIAB': '流动负债合计',
                    'TRADE_FINASSET_NOTFVTPL': '交易性金融资产'
                    }
    projection = {"code": True, "date": True}
    for k, _ in get_col_dict.items():
        projection[k] = True
    zcfz_pd_data = get_data(dtype='zcfz_report_detail', cods=local_codes, projection=projection,date=start_date)
    get_db_cols = list(zcfz_pd_data.columns)
    for col in get_col_dict.keys():
        if col in get_db_cols:
            zcfz_pd_data[col] = zcfz_pd_data[col].apply(convert_ele)
        else:
            zcfz_pd_data[col] = 0
    zcfz_pd_data['流动资产比率'] = zcfz_pd_data['TOTAL_CURRENT_ASSETS'] / zcfz_pd_data['TOTAL_ASSETS']
    zcfz_pd_data['应收账款率'] = zcfz_pd_data['NOTE_ACCOUNTS_RECE'] / zcfz_pd_data['TOTAL_CURRENT_ASSETS']
    zcfz_pd_data['存货率'] = zcfz_pd_data['INVENTORY'] / zcfz_pd_data['TOTAL_CURRENT_ASSETS']
    zcfz_pd_data['资本负债率'] = (zcfz_pd_data['SHORT_LOAN'] + zcfz_pd_data['LONG_LOAN'] + zcfz_pd_data[
        'BOND_PAYABLE'] - zcfz_pd_data['OTHER_CURRENT_ASSET']) / zcfz_pd_data['TOTAL_PARENT_EQUITY']
    zcfz_pd_data['资产负债率'] = zcfz_pd_data['TOTAL_LIABILITIES'] / zcfz_pd_data['TOTAL_ASSETS']
    zcfz_pd_data['产权比率'] = zcfz_pd_data['TOTAL_LIABILITIES'] / zcfz_pd_data['TOTAL_EQUITY']
    zcfz_pd_data['股东权益比率'] = zcfz_pd_data['TOTAL_EQUITY'] / zcfz_pd_data['TOTAL_ASSETS']
    zcfz_pd_data['现金比率'] = (zcfz_pd_data['MONETARYFUNDS'] + zcfz_pd_data['TRADE_FINASSET_NOTFVTPL']) / zcfz_pd_data[
        'TOTAL_CURRENT_LIAB']
    zcfz_pd_data['速动比率'] = (zcfz_pd_data['TOTAL_CURRENT_ASSETS'] - zcfz_pd_data['INVENTORY']) / zcfz_pd_data[
        'TOTAL_CURRENT_LIAB']
    zcfz_pd_data['流动比率'] = zcfz_pd_data['TOTAL_CURRENT_ASSETS'] / zcfz_pd_data['TOTAL_CURRENT_LIAB']

    avg_assets_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'TOTAL_ASSETS')
    avg_equity_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'TOTAL_EQUITY')
    avg_share_capital_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'SHARE_CAPITAL')
    avg_capital_reserve_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'CAPITAL_RESERVE')
    avg_account_rece_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'ACCOUNTS_RECE')
    avg_inventory_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'INVENTORY')
    avg_fixed_asset_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'FIXED_ASSET')
    avg_total_current_assets_data = handle_fin_avg_data(zcfz_pd_data, local_codes, 'TOTAL_CURRENT_ASSETS')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_assets_data, on=['date', 'code'], how='left')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_equity_data, on=['date', 'code'], how='left')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_share_capital_data, on=['date', 'code'], how='left')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_capital_reserve_data, on=['date', 'code'], how='left')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_account_rece_data, on=['date', 'code'], how='left')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_inventory_data, on=['date', 'code'], how='left')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_total_current_assets_data, on=['date', 'code'], how='left')
    zcfz_pd_data = pd.merge(zcfz_pd_data, avg_fixed_asset_data, on=['date', 'code'], how='left')
    zcfz_pd_data['平均资本总额'] = zcfz_pd_data['AVG_SHARE_CAPITAL'] + zcfz_pd_data['AVG_CAPITAL_RESERVE']

    return zcfz_pd_data


def get_fin_earning_metric(code_list, isDataFromLocal=True,start_date=None):
    """
    盈利能力指标
        1.毛利率 = (营业收入-营业成本)/营业收入 毛利率越高，市场竞争力大
        2.销售净利率= 净利润/营业收入
        3.主营业务利润率=(主营业务收入-主营业务成本-主营业务税金及附加)/主营业务收入
        4.营业利润率 = 营业利润/营业收入

        注意：上面指标要结合一起看才能得出结论
        :param code_list:
    :param isDataFromLocal:
    :return:
    """

    def convert_ele(ele):
        if ele == '--' or str(ele) == 'nan':
            ele = 0
        if float(ele) < 0:
            return 0
        return float(ele)

    local_codes = [code[2:] for code in code_list]
    if isDataFromLocal is False:
        handle_comm_stock_fin_em(codes=code_list, data_type="profit_report_em_detail")

    projection = {"date": True, "code": True}
    get_col_dict = {"FE_INTEREST_EXPENSE": "其中:利息费用",
                    "FE_INTEREST_INCOME": "利息收入",
                    "OPERATE_INCOME": "营业收入",
                    "OPERATE_COST": "营业成本",
                    'NETPROFIT': '净利润',
                    'TOTAL_PROFIT': '利润总额',
                    'OPERATE_TAX_ADD': '税金及附加',
                    'OPERATE_PROFIT': '营业利润',
                    'RESEARCH_EXPENSE':'研发费用'
                    }
    for k, _ in get_col_dict.items():
        projection[k] = True
    profit_pd_data = get_data(dtype='profit_report_em_detail', cods=local_codes, projection=projection,date=start_date)
    get_db_cols = list(profit_pd_data.columns)
    for col in get_col_dict.keys():
        if col in get_db_cols:
            profit_pd_data[col] = profit_pd_data[col].apply(convert_ele)
        else:
            profit_pd_data[col] = 0
    profit_pd_data['毛利率'] = (profit_pd_data['OPERATE_INCOME'] - profit_pd_data['OPERATE_COST']) / profit_pd_data[
        'OPERATE_INCOME']
    profit_pd_data['销售净利率'] = profit_pd_data['NETPROFIT'] / profit_pd_data['OPERATE_INCOME']
    profit_pd_data['主营业务利润率'] = (profit_pd_data['OPERATE_INCOME'] - profit_pd_data['OPERATE_COST'] -
                                        profit_pd_data['OPERATE_TAX_ADD']) / profit_pd_data['OPERATE_INCOME']
    profit_pd_data['营业利润率'] = profit_pd_data['OPERATE_PROFIT'] / profit_pd_data['OPERATE_INCOME']
    return profit_pd_data


def get_fin_cash_flow_metric(code_list, isDataFromLocal=True,start_date=None):
    """
    计算现金流量相关数据
    :param code_list:
    :param isDataFromLocal:
    :return:
    """

    def convert_ele(ele):
        if ele == '--' or str(ele) == 'nan':
            ele = 0
        if float(ele) < 0:
            return 0
        return float(ele)

    local_codes = [code[2:] for code in code_list]
    if isDataFromLocal is False:
        handle_comm_stock_fin_em(codes=code_list, data_type="cash_flow_report_em_detail")

    projection = {"date": True, "code": True}
    get_col_dict = {"NETCASH_OPERATE": "经营活动产生的现金流量净额",
                    "TOTAL_OPERATE_OUTFLOW": "经营活动现金流出小计",
                    "TOTAL_OPERATE_INFLOW": "经营活动现金流入小计",
                    "SALES_SERVICES": "销售商品、提供劳务收到的现金",
                    "BUY_SERVICES": "购买商品、接受劳务支付的现金",
                    "PAY_STAFF_CASH": "支付给职工以及为职工支付的现金",
                    'NETCASH_INVEST': '投资活动产生的现金流量净额',
                    }
    for k, _ in get_col_dict.items():
        projection[k] = True
    profit_pd_data = get_data(dtype='cash_flow_report_em_detail', cods=local_codes, projection=projection,date=start_date)
    get_db_cols = list(profit_pd_data.columns)
    for col in get_col_dict.keys():
        if col in get_db_cols:
            profit_pd_data[col] = profit_pd_data[col].apply(convert_ele)
        else:
            profit_pd_data[col] = 0
    return profit_pd_data


def get_fin_common_metric(code_list, isZcfcDataFromLocal=True, isProfitDataFromLocal=True, isCashDataFromLocal=True,start_date=None):
    """

    盈利指标
        总资产收益率 = 净利润/平均资产总额
        净资产收益率 = 净利润/平均股东权利
        总资产利润率 = 利润总额/平均资产额
        资本收益率 =  净利润/平均资本总额
        资本报酬率 = 净利润/股东权益
    营运能力指标
        全部资产现金回收率 = 自由现金流/总资产
            注：自由现金流 = 经营活动产生的现金流量净额+投资活动产生的现金流量净额
        销售现金比率 = 经营活动现金流量净额/营业收入
        现金流动负债率=经营活动现金流量净额/流动负债
        应收账款周转率 =  营业收入/平均应收账款
        应收周转天数 = 365 / 应收账款周转率
        存货周转率 = 营业成本 / 平均存货
        存货周转天数 = 365 / 存货周转率
        流动资产周转率 = 营业成本 /平均流动资产
        流动资产周转天数 = 365 / 流动资产周转率
        固定资产周转率 =  营业收入/平均固定资产
        固定资产周转天数 = 365 / 固定资产周转率
        总资产周转率 =  营业收入/平均总资产
        总资产周转天数 = 365 / 总资产周转率


    :param code_list:
    :param isZcfcDataFromLocal:
    :param isProfitDataFromLocal:
    :param isCashDataFromLocal:
    :return:
    """
    zcfc_data = get_fin_assets_metric(code_list, isZcfcDataFromLocal,start_date)
    profit_data = get_fin_earning_metric(code_list, isProfitDataFromLocal,start_date)
    cash_flow_data = get_fin_cash_flow_metric(code_list, isCashDataFromLocal,start_date)
    pd_merge_data = pd.merge(zcfc_data, profit_data, on=['date', 'code'], how='left')
    pd_merge_data = pd.merge(pd_merge_data, cash_flow_data, on=['date', 'code'], how='left')
    pd_merge_data['总资产收益率'] = pd_merge_data['NETPROFIT'] / pd_merge_data['AVG_TOTAL_ASSETS']
    pd_merge_data['净资产收益率'] = pd_merge_data['NETPROFIT'] / pd_merge_data['AVG_TOTAL_EQUITY']
    pd_merge_data['总资产利润率'] = pd_merge_data['TOTAL_PROFIT'] / pd_merge_data['AVG_TOTAL_ASSETS']
    pd_merge_data['资本收益率'] = pd_merge_data['NETPROFIT'] / pd_merge_data['平均资本总额']
    pd_merge_data['资本报酬率'] = pd_merge_data['NETPROFIT'] / pd_merge_data['TOTAL_EQUITY']

    # 运营能力指标
    pd_merge_data['全部资产现金回收率'] = (pd_merge_data['NETCASH_OPERATE'] + pd_merge_data['NETCASH_INVEST']) / \
                                          pd_merge_data['TOTAL_ASSETS']
    pd_merge_data['销售现金比率'] = pd_merge_data['NETCASH_OPERATE'] / pd_merge_data['OPERATE_INCOME']
    pd_merge_data['现金流动负债率'] = pd_merge_data['NETCASH_OPERATE'] / pd_merge_data['TOTAL_CURRENT_LIAB']
    pd_merge_data['应收账款周转率'] = pd_merge_data['OPERATE_INCOME'] / pd_merge_data['AVG_ACCOUNTS_RECE']
    pd_merge_data['应收周转天数'] = 365 / pd_merge_data['应收账款周转率']
    pd_merge_data['存货周转率'] = pd_merge_data['OPERATE_COST'] / pd_merge_data['AVG_INVENTORY']
    pd_merge_data['存货周转天数'] = 365 / pd_merge_data['存货周转率']
    pd_merge_data['流动资产周转率'] = pd_merge_data['OPERATE_COST'] / pd_merge_data['AVG_TOTAL_CURRENT_ASSETS']
    pd_merge_data['流动资产周转天数'] = 365 / pd_merge_data['流动资产周转率']
    pd_merge_data['固定资产周转率'] = pd_merge_data['OPERATE_INCOME'] / pd_merge_data['AVG_FIXED_ASSET']
    pd_merge_data['固定资产周转天数'] = 365 / pd_merge_data['固定资产周转率']
    pd_merge_data['总资产周转率'] = pd_merge_data['OPERATE_INCOME'] / pd_merge_data['AVG_TOTAL_ASSETS']
    pd_merge_data['总资产周转天数'] = 365 / pd_merge_data['总资产周转率']

    pd_merge_data['权益乘数'] = pd_merge_data['AVG_TOTAL_ASSETS'] / pd_merge_data['AVG_TOTAL_EQUITY']
    return pd_merge_data


def credit_funds_fin_inst_analysis():
    income_config = {
        "住户贷款": "loans_to_households",
        "住户短期贷款": "short_term_loans",
        "住户中长期贷款": "mid_long_term_loans",
        "(事)业单位贷款": "loans_to_non_financial_enterprises_and_government_departments_organizations",
        "企业短期贷款": "short_term_loans_1",
        "企业中长期贷款": "mid_long_term_loans_1",
        "债券投资": "portfolio_investments",
    }
    all_income_config = {
        "time": True,
        "_id": False
    }
    for _, v in income_config.items():
        all_income_config[v] = True

    re_all_config = {}
    for k, v in income_config.items():
        re_all_config[v] = k
    print(re_all_config)
    news = get_mongo_table(database='stock', collection='common_seq_data')
    datas = []
    for ele in news.find({"data_type": "credit_funds", "metric_code": "credit_funds_fin_inst_rmb"},
                         projection=all_income_config).sort(
        "time"):
        datas.append(ele)
    pd_data = pd.DataFrame(data=datas)
    data = pd_data.rename(columns=re_all_config)
    for col in income_config.keys():
        data[[col]] = data[[col]].astype(float)
    data.dropna(inplace=True,axis=0)
    data.set_index(keys=['time'], inplace=True)
    print("*" * 50)
    show_data(data)
    print("*" * 50)
    data.plot(kind='bar', title='社会融资贷款规模分析', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()
    data = data.diff()
    print("*" * 50)
    show_data(data)
    print("*" * 50)
    data.plot(kind='bar', title='社会融资增量贷款分析', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()
    data = data.dropna()
    data = data.pct_change(12)
    data = data.dropna()
    print("*" * 50)
    show_data(data)
    print("*" * 50)
    data.plot(kind='bar', title='社会融资增量贷款同比对比分析', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()


def stock_score(pd_data: pd.DataFrame, metric, sort_type=False):
    data = pd.pivot_table(pd_data, values=metric, index=['date'], columns=['code'])
    dict_list = []
    for index in data.index:
        dict_data = dict(data.loc[index])
        sort_dict_data = sort_dict_data_by(dict_data, by='value', reverse=sort_type)
        num = len(sort_dict_data)
        for i, combine in enumerate(sort_dict_data.items()):
            k, v = combine
            score = ((i + 1) / num) * 100
            dict_list.append({"code": k, f"{metric}_score": score, "date": index})
    score_df = pd.DataFrame(data=dict_list)
    return score_df


def analysis_fin_by_metric(code_dict=None, isLocal=False,quarter=4,is_show=True,start_date=None):

    def fin_data_same_rate(pd_data, val_col, rename_col):
        temp_data = pd.pivot_table(pd_data, values=val_col, index='date', columns='code')
        temp_data.sort_index(inplace=True)
        pct_change_data = temp_data.pct_change(1)
        data_list = []
        for index in pct_change_data.index:
            dict_data = dict(pct_change_data.loc[index])
            for code, value in dict_data.items():
                data_list.append({"code": code, rename_col: value, "date": index})
        return pd.DataFrame(data_list)

    quarter_mapping = {1:"03-31",2:"06-30",3:"09-30",4:"12-31"}
    quarter_month = quarter_mapping[quarter]
    def handle_score(row, col_list):
        total_score = 0
        for col in col_list:
            total_score += row[col]
        return total_score

    if code_dict is None:
        code_dict = {"sh603019": "中科曙光", "sz002230": "科大讯飞", "sz000977": "浪潮信息", "sz300474": "景嘉微"}
    rename_code = {}
    for k, v in code_dict.items():
        rename_code[k[2:]] = v
    codes = list(code_dict.keys())
    data = get_fin_common_metric(code_list=codes, isZcfcDataFromLocal=isLocal, isProfitDataFromLocal=isLocal,
                                 isCashDataFromLocal=isLocal,start_date=start_date)
    pd_data = data[data['date'].str.contains(quarter_month)]
    # 同期的比较同比的指标 净利润增长率:NETPROFIT 营业收入增长率:OPERATE_INCOME 总资产增长率:TOTAL_ASSETS 净资产增长率:TOTAL_EQUITY 营业利润增长率:OPERATE_PROFIT

    same_df = fin_data_same_rate(pd_data,'NETPROFIT','净利润增长率')
    pd_data = pd.merge(pd_data, same_df, on=['date', 'code'], how='left')

    same_df = fin_data_same_rate(pd_data, 'OPERATE_INCOME', '营业收入增长率')
    pd_data = pd.merge(pd_data, same_df, on=['date', 'code'], how='left')

    same_df = fin_data_same_rate(pd_data, 'TOTAL_ASSETS', '总资产增长率')
    pd_data = pd.merge(pd_data, same_df, on=['date', 'code'], how='left')


    same_df = fin_data_same_rate(pd_data, 'TOTAL_EQUITY', '净资产增长率')
    pd_data = pd.merge(pd_data, same_df, on=['date', 'code'], how='left')


    same_df = fin_data_same_rate(pd_data, 'OPERATE_PROFIT', '营业利润增长率')
    pd_data = pd.merge(pd_data, same_df, on=['date', 'code'], how='left')

    same_df = fin_data_same_rate(pd_data, 'RESEARCH_EXPENSE', '研发费用增长率')
    pd_data = pd.merge(pd_data, same_df, on=['date', 'code'], how='left')

    # 公司发展指标
    future_dev_metric_cols = ['净利润增长率', '营业收入增长率', '总资产增长率', '净资产增长率', '营业利润增长率','研发费用增长率']
    # 盈利能力指标
    profitability_metric_cols = ['毛利率', '销售净利率', '总资产收益率', '净资产收益率', '资本收益率', '资本报酬率',
                                 '总资产利润率']
    # 偿债能力指标
    pay_debt_metric_cols = ['资产负债率', '流动比率', '速动比率', '现金比率', '产权比率']

    operator_metric_cols = ['全部资产现金回收率', '销售现金比率', '现金流动负债率', '应收账款周转率', '存货周转率',
                            '流动资产周转率', '固定资产周转率', '总资产周转率']

    # 净资产收益率=销售净利率*总资产周转率*权益乘数
    # (净利润/营业收入)*(营业收入/平均资产)*(平均资产/平均净资产)

    pay_debt_score_sort_type = {'资产负债率': True, "产权比率": True}
    # 财务指标分类打分
    metric_sort_type = pay_debt_score_sort_type  # 有指标要评分，倒序指定就行
    metric_col = operator_metric_cols + pay_debt_metric_cols + profitability_metric_cols + future_dev_metric_cols

    score_df_list = []
    metric_core_col = []
    for metric in metric_col:
        metric_core_col.append(f"{metric}_score")
        sort_type = False
        if metric in metric_sort_type.keys():
            sort_type = metric_sort_type.get(metric)
        score_df = stock_score(pd_data, metric, sort_type=sort_type)
        score_df_list.append(score_df)
        data = pd.pivot_table(pd_data, values=metric, index=['date'], columns=['code'])
        data = data.rename(columns=rename_code)
        if is_show:
            show_data(data)
            data.plot(kind='bar', title=metric, rot=45, width=0.5, figsize=(15, 8), fontsize=10)
            plt.show()

    score_df = score_df_list[0]
    for ele in score_df_list[1:]:
        score_df = pd.merge(score_df, ele, left_on=['code', 'date'], right_on=['code', 'date'])
    score_df['total_score'] = score_df.apply(handle_score, axis=1, args=(metric_core_col,))

    data = pd.pivot_table(score_df, values='total_score', index=['date'], columns=['code'])
    ret_data = copy.deepcopy(data)
    data = data.rename(columns=rename_code)
    if is_show:
        show_data(data)
        data.plot(kind='bar', title='分数', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
        plt.show()
    return ret_data


def handle_fin_avg_data(pd_data, local_codes, handle_key):
    data = pd.pivot_table(pd_data, values=handle_key, index='date', columns='code')
    data.reset_index(level=0, inplace=True)

    def handle_date(ele):
        year, month, day = str(ele).split("-")
        year = int(year) + 1
        return f"{year}-{month}-{day}"

    data['next_year'] = data['date'].apply(handle_date)
    cols = copy.deepcopy(local_codes)
    cols.append('next_year')
    new_data = data[cols]
    pd_data = pd.merge(data, new_data, left_on=['date'], right_on=['next_year'], suffixes=("_current", "_before"))
    for col in local_codes:
        cur = f"{col}_current"
        before = f"{col}_before"
        pd_data[col] = (pd_data[cur] + pd_data[before]) / 2
    cols = copy.deepcopy(local_codes)
    cols.append('date')
    pd_data = pd_data[cols]
    new_pd_data_list = []
    for index in pd_data.index:
        dict_data = dict(pd_data.loc[index])
        for k, v in dict_data.items():
            if k != "date":
                new_dict_data = {}
                new_dict_data['code'] = k
                new_dict_data[f'AVG_{handle_key}'] = v
                new_dict_data['date'] = dict_data.get('date')
                new_pd_data_list.append(new_dict_data)
    return pd.DataFrame(data=new_pd_data_list)


def big_model_stock_fin_data(local_codes, model):
    def format_data(df_data: pd.DataFrame, cols):
        data_cols = df_data.columns
        for col in cols:
            if col in data_cols:
                df_data[col] = df_data[col].astype(float)
                df_data[col] = np.round(df_data[col] / 1e8, 4)

    if local_codes is None or len(local_codes) > 1:
        print("输入代码不能为空")
        return
    if len(local_codes) > 1:
        print("只能输入一个代码")
        return
    get_col_dict = {"MONETARYFUNDS": "货币资金",
                    "INVENTORY": "存货",
                    "ACCOUNTS_RECE": "应收账款",
                    "PREPAYMENT": "预付款项",
                    "TOTAL_ASSETS": "资产总计",
                    "TOTAL_LIABILITIES": "负债合计",
                    "INTANGIBLE_ASSET": "无形资产",
                    "NOTE_ACCOUNTS_PAYABLE": "应付票据及应付账款",
                    "ACCOUNTS_PAYABLE": "其中:应付账款",
                    "LONG_LOAN": "长期借款",
                    "SHORT_LOAN": "短期借款",
                    "NOTE_ACCOUNTS_RECE": "应收票据及应收账款",
                    "CIP": "在建工程",
                    "FIXED_ASSET": "固定资产",
                    "TOTAL_CURRENT_ASSETS": "流动资产合计",
                    "TOTAL_PARENT_EQUITY": "归属于母公司股东权益总计",
                    'TOTAL_EQUITY': "股东权益合计",
                    'OTHER_CURRENT_ASSET': '其他流动资产',
                    'NOTE_PAYABLE': "其中:应付票据",
                    'BOND_PAYABLE': "应付债券",
                    'SHARE_CAPITAL': '实收资本（或股本',
                    'CAPITAL_RESERVE': '资本公积',
                    'TOTAL_CURRENT_LIAB': '流动负债合计',
                    'TRADE_FINASSET_NOTFVTPL': '交易性金融资产',
                    'date': '日期',
                    'code': '股票代码'
                    }
    format_cols = [k for k in get_col_dict.keys() if k not in ['date', 'code']]
    projection = {"code": True, "date": True, "_id": False}
    for k, _ in get_col_dict.items():
        projection[k] = True
    data = get_data(cods=local_codes, dtype='zcfz_report_detail', projection=projection)
    data = data[data['date'] > '2023-01-01']
    format_data(data, format_cols)
    zcfz_max = np.max(data['date'].values)

    data.rename(columns=get_col_dict, inplace=True)
    zcfz_input_str = handle_model_table_data(data)
    get_col_dict = {"FE_INTEREST_EXPENSE": "其中:利息费用",
                    "FE_INTEREST_INCOME": "利息收入",
                    "OPERATE_INCOME": "营业收入",
                    "OPERATE_COST": "营业成本",
                    'NETPROFIT': '净利润',
                    'TOTAL_PROFIT': '利润总额',
                    'OPERATE_TAX_ADD': '税金及附加',
                    'OPERATE_PROFIT': '营业利润',
                    'date': '日期',
                    'code': '股票代码'
                    }
    format_cols = [k for k in get_col_dict.keys() if k not in ['date', 'code']]
    projection = {"code": True, "date": True, "_id": False}
    for k, _ in get_col_dict.items():
        projection[k] = True
    data = get_data(cods=local_codes, dtype='profit_report_em_detail', projection=projection)
    data = data[data['date'] > '2023-01-01']
    format_data(data, format_cols)
    profit_max = np.max(data['date'].values)

    data.rename(columns=get_col_dict, inplace=True)
    profit_input_str = handle_model_table_data(data)

    projection = {"date": True, "code": True, "_id": False}
    get_col_dict = {"NETCASH_OPERATE": "经营活动产生的现金流量净额",
                    "TOTAL_OPERATE_OUTFLOW": "经营活动现金流出小计",
                    "TOTAL_OPERATE_INFLOW": "经营活动现金流入小计",
                    "SALES_SERVICES": "销售商品、提供劳务收到的现金",
                    "BUY_SERVICES": "购买商品、接受劳务支付的现金",
                    "PAY_STAFF_CASH": "支付给职工以及为职工支付的现金",
                    'NETCASH_INVEST': '投资活动产生的现金流量净额',
                    'date': '日期',
                    'code': '股票代码'
                    }
    format_cols = [k for k in get_col_dict.keys() if k not in ['date', 'code']]
    for k, _ in get_col_dict.items():
        projection[k] = True
    data = get_data(dtype='cash_flow_report_em_detail', cods=local_codes, projection=projection)
    data = data[data['date'] > '2023-01-01']
    format_data(data, format_cols)
    cash_max = np.max(data['date'].values)

    data.rename(columns=get_col_dict, inplace=True)
    cash_flow_input_str = handle_model_table_data(data)

    input_txt = """给定资产负债表，利润表，现金流量表，总结出盈利能力，收入增长，债务水平，现金周转，资产及股权以及最终结论。资产负债表(数值单位亿元)：| 股票代码 | 日期 | 其中:应付账款 | 应收账款 | 资本公积 | 在建工程 | 固定资产 | 无形资产 | 存货 | 货币资金 | 应付票据及应付账款 | 应收票据及应收账款 | 其中:应付票据 | 其他流动资产 | 预付款项 | 实收资本（或股本 | 短期借款 | 资产总计 | 流动资产合计 | 流动负债合计 | 股东权益合计 | 负债合计 | 归属于母公司股东权益总计 | 长期借款 | 交易性金融资产 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 002230 | 2023-03-31 | 40.6885 | 97.3536 | 100.3972 | 7.7867 | 25.4997 | 25.8149 | 28.8286 | 32.1937 | 63.4071 | 102.3143 | 22.7187 | 1.9825 | 3.2523 | 23.2308 | 3.6401 | 317.1518 | 186.1447 | 104.1367 | 167.4155 | 149.7363 | 163.4949 | 22.7952 |  |
| 002230 | 2023-06-30 | 48.6926 | 108.75 | 98.8862 | 9.5987 | 26.2006 | 25.8527 | 26.7974 | 36.9739 | 69.0859 | 114.0461 | 20.3933 | 1.786 | 2.9745 | 23.1568 | 9.3403 | 337.0917 | 199.963 | 113.4202 | 169.1204 | 167.9712 | 165.2496 | 29.7971 |  |
| 002230 | 2023-09-30 | 56.0364 | 119.3862 | 98.9577 | 11.1309 | 29.2692 | 26.2652 | 26.5752 | 33.9815 | 78.9302 | 125.3536 | 22.8938 | 2.3238 | 3.6365 | 23.1568 | 11.8203 | 352.1416 | 209.4441 | 130.4688 | 167.1926 | 184.949 | 163.2782 | 29.0776 |  |
盈利表(数值单位亿元)：| 股票代码 | 日期 | 净利润 | 营业成本 | 营业收入 | 营业利润 | 税金及附加 | 利润总额 | 其中:利息费用 | 利息收入 |
|---|---|---|---|---|---|---|---|---|---|
| 002230 | 2023-03-31 | -0.9502 | 16.7816 | 28.8758 | -0.77 | 0.1644 | -0.7274 | 0.1641 | 0.0603 |
| 002230 | 2023-06-30 | 0.1486 | 46.9448 | 78.4155 | -0.3702 | 0.425 | -0.4323 | 0.3479 | 0.2103 |
| 002230 | 2023-09-30 | 0.4503 | 75.3003 | 126.1375 | 0.0221 | 0.7278 | -0.0704 | 0.5255 | 0.3249 |
现金流量表(数值单位亿元)：| 股票代码 | 日期 | 购买商品、接受劳务支付的现金 | 投资活动产生的现金流量净额 | 经营活动产生的现金流量净额 | 支付给职工以及为职工支付的现金 | 销售商品、提供劳务收到的现金 | 经营活动现金流入小计 | 经营活动现金流出小计 |
|---|---|---|---|---|---|---|---|---|
| 002230 | 2023-03-31 | 29.838 | -2.7971 | -16.6842 | 15.3572 | 36.7059 | 39.1216 | 55.8058 |
| 002230 | 2023-06-30 | 56.149 | -8.473 | -15.2874 | 25.2462 | 75.064 | 82.985 | 98.2724 |
| 002230 | 2023-09-30 | 83.2916 | -15.3073 | -11.716 | 34.7165 | 119.924 | 130.9132 | 142.6291 |
输出：{"盈利能力":"净利润在逐季改善，从2023年3月末的-0.95亿元增至2023年9月末的0.45亿元。毛利率从2023年3月末的-2.7%上升至2023年9月末的0.36%。","收入增长":" 营业收入在逐季增长，从2023年3月末的28.88亿元增至2023年9月末的126.14亿元。营业收入同比增长42%。","债务水平":"流动负债总额逐季小幅增长，从2023年3月末的104.14亿元增至2023年9月末的130.47亿元。流动负债占总负债的比例从69.6%上升至70.5%。","现金周转":"经营活动产生的现金流量净额在逐季改善，从2023年3月末的-16.68亿元增至2023年9月末的-11.72亿元。收现比从2023年3月末的0.70倍提升至2023年9月末的0.92倍。","资产及股权":"资产总额从2023年3月末的317.15亿元增至2023年9月末的352.14亿元。股东权益总额从2023年3月末的167.42亿元增至2023年9月末的167.19亿元。","最终结论":"公司盈利能力呈改善趋势，营业收入逐季增长，收入同比增长强劲。公司流动负债较多，但仍处于可控范围，流动负债与长期负债的比例为70.50:29.50。公司现金流改善，收现比提升，资产总额和股东权益总额基本保持稳定。"}
资产负债表(数值单位亿元)：$zcfz盈利表(数值单位亿元)：$profit现金流量表(数值单位亿元)：$cash输出："""
    input_txt = input_txt.replace("$zcfz", zcfz_input_str).replace("$profit", profit_input_str).replace("$cash",
                                                                                                        cash_flow_input_str)

    ret_json = try_get_action(simple_big_gen_model_fn, model=model, request_txt=input_txt)
    if ret_json is None:
        return None,None
    return ret_json,max([zcfz_max,profit_max,cash_max])


def enter_big_model_analysis_stock_fin(code_dict: dict = None):
    api_key_json = load_json_data("google_api.json")
    api_key = api_key_json['api_key']
    genai.configure(api_key=api_key, transport='rest')
    model = genai.GenerativeModel('gemini-pro')

    if code_dict is None:
        code_dict = {
            # 半导体
            "002409": "雅克科技",
            # 电力
            "002015": "协鑫能科",
            # 游戏
            "002555": "三七互娱",
            "002602": "世纪华通",
            "603444": "吉比特",
            # 通讯
            "000063": "中兴通讯",
            "600522": "中天科技",
            # 白酒
            "000858": "五粮液",
            "600519": "贵州茅台",
            # 机器人
            "002472": "双环传动",
            "002527": "新时达",
            # 银行
            "600036": "招商银行",
            "600919": "江苏银行",
            # AI相关
            "300474": "景嘉微",
            "002230": "科大讯飞",
            "603019": "中科曙光",
            "000977": "浪潮信息",
            # 新能源
            "300750": "宁德时代",
            "002594": "比亚迪",
            # 零食
            "300783": "三只松鼠",
            "603719": "良品铺子",
            # 啤酒
            "600132": "重庆啤酒",
            "600600": "青岛啤酒",
        }

    update_request = []
    big_model_col = get_mongo_table(database='stock', collection="big_model")
    for code,name in code_dict.items():
        print(f"handel {name}")

        ret_json,time = big_model_stock_fin_data([code],model)
        if ret_json is not None:
            new_dict = {"data_type": "stock_fin_summary",
                        "time": time, "code": code}
            for k,v in ret_json.items():
                new_dict[k] = v
            update_request.append(
                UpdateOne({"code": code, 'time': new_dict['time'], "data_type": new_dict['data_type']},
                          {"$set": new_dict},
                          upsert=True)
            )
            if len(update_request)%10==0:
                mongo_bulk_write_data(big_model_col,update_request)
                update_request.clear()
    if len(update_request)>0:
        mongo_bulk_write_data(big_model_col, update_request)
        update_request.clear()
def m1_and_m2_diff_analysis():
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    sort_key = "time"
    code_dict = {"A0D0102_yd": "货币和准货币(M2)供应量同比增长(%)","A0D0104_yd":"货币(M1)供应量同比增长(%)","A0D0106_yd":"流通中现金(M0)供应量同比增长(%)"}
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['time', 'data','code']
    data = data[cols]
    data = pd.pivot_table(data,values='data',index='time',columns='code')
    data.rename(columns=code_dict,inplace=True)
    data['m1_m2_diff'] = round(data['货币(M1)供应量同比增长(%)'] - data['货币和准货币(M2)供应量同比增长(%)'],4)
    data['time'] = data.index
    data['code'] = 'm1与m2增速之差'
    data = convert_pd_data_to_month_data(data,'time','m1_m2_diff','code')
    show_data(data)
    data.plot(kind='line', title='m1与m2增速之差', rot=45, figsize=(15, 8), fontsize=10)
    plt.show()
def money_multiply_analysis():
    """
    中国货币乘数
    :return:
    """
    database = 'govstats'
    collection = 'data_info'
    projection = {'_id': False}
    sort_key = "time"
    code_dict = {"A0D0101_yd": "m2"}
    code_list = {"$in": list(code_dict.keys())}
    condition = {"code": code_list}
    m2_data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    cols = ['time', 'data']
    m2_data = m2_data[cols]
    database = 'stock'
    collection = 'common_seq_data'
    projection = {'_id': False,'reserve_money':True,'time':True}
    sort_key = "time"
    condition = {"data_type":"fin_monetary","metric_code":"balance_monetary_authority","time":{"$gt":"20170101"}}
    base_data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    base_data['time'] = base_data['time'].apply(lambda key:key[0:6])

    merge_data = pd.merge(base_data,m2_data,on=['time'])
    merge_data['m_multiply'] = round(merge_data['data']/merge_data['reserve_money'],4)
    merge_data.set_index(keys='time',inplace=True)
    merge_data['m_multiply'].plot(kind='line', title='中国货币乘数', rot=45, figsize=(15, 8), fontsize=10)
    show_data(merge_data)
    plt.show()







if __name__ == '__main__':
    enter_big_model_analysis_stock_fin()
