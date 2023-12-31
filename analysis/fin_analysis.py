import pandas as pd
from data.mongodb import get_mongo_table
from utils.actions import show_data
from utils.tool import sort_dict_data_by
import matplotlib.pyplot as plt
import copy

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')
from data.stock_detail_fin import handle_comm_stock_fin_em, handle_fin_analysis_indicator


def get_data(cods=None, dtype="fin_indicator", projection=None):
    if projection is None:
        projection = {"_id": False}
    if cods is None:
        cods = ['603288', '601009', '600036', '002507', '002385', '603363']
    fin_col = get_mongo_table(collection='fin')
    ret = fin_col.find({"code": {"$in": cods}, "data_type": dtype}, projection=projection).sort("date")
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



def get_fin_assets_metric(code_list, isDataFromLocal=True):
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
    zcfz_pd_data = get_data(dtype='zcfz_report_detail', cods=local_codes, projection=projection)
    for col in get_col_dict.keys():
        zcfz_pd_data[col] = zcfz_pd_data[col].apply(convert_ele)
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


def get_fin_earning_metric(code_list, isDataFromLocal=True):
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
                    'OPERATE_PROFIT': '营业利润'
                    }
    for k, _ in get_col_dict.items():
        projection[k] = True
    profit_pd_data = get_data(dtype='profit_report_em_detail', cods=local_codes, projection=projection)
    for col in get_col_dict.keys():
        profit_pd_data[col] = profit_pd_data[col].apply(convert_ele)
    profit_pd_data['毛利率'] = (profit_pd_data['OPERATE_INCOME'] - profit_pd_data['OPERATE_COST']) / profit_pd_data[
        'OPERATE_INCOME']
    profit_pd_data['销售净利率'] = profit_pd_data['NETPROFIT'] / profit_pd_data['OPERATE_INCOME']
    profit_pd_data['主营业务利润率'] = (profit_pd_data['OPERATE_INCOME'] - profit_pd_data['OPERATE_COST'] -
                                        profit_pd_data['OPERATE_TAX_ADD']) / profit_pd_data['OPERATE_INCOME']
    profit_pd_data['营业利润率'] = profit_pd_data['OPERATE_PROFIT'] / profit_pd_data['OPERATE_INCOME']
    return profit_pd_data


def get_fin_cash_flow_metric(code_list, isDataFromLocal=True):
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
    profit_pd_data = get_data(dtype='cash_flow_report_em_detail', cods=local_codes, projection=projection)
    for col in get_col_dict.keys():
        profit_pd_data[col] = profit_pd_data[col].apply(convert_ele)
    return profit_pd_data


def get_fin_common_metric(code_list, isZcfcDataFromLocal=True, isProfitDataFromLocal=True, isCashDataFromLocal=True):
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
    zcfc_data = get_fin_assets_metric(code_list, isZcfcDataFromLocal)
    profit_data = get_fin_earning_metric(code_list, isProfitDataFromLocal)
    cash_flow_data = get_fin_cash_flow_metric(code_list, isCashDataFromLocal)
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


def analysis_fin_by_metric(code_dict=None,isLocal=False):
    def handle_score(row, col_list):
        total_score = 0
        for col in col_list:
            total_score += row[col]
        return total_score
    if code_dict is None:
        code_dict = {"sh603019": "中科曙光", "sz002230": "科大讯飞","sz000977":"浪潮信息","sz300474":"景嘉微"}
    rename_code = {}
    for k, v in code_dict.items():
        rename_code[k[2:]] = v
    codes = list(code_dict.keys())
    data = get_fin_common_metric(code_list=codes, isZcfcDataFromLocal=isLocal, isProfitDataFromLocal=isLocal,
                                 isCashDataFromLocal=isLocal)
    pd_data = data[data['date'].str.contains("09-30")]

    # 同期的比较同比的指标 净利润增长率:NETPROFIT 营业收入增长率:OPERATE_INCOME 总资产增长率:TOTAL_ASSETS 净资产增长率:TOTAL_EQUITY 营业利润增长率:OPERATE_PROFIT
    pd_data['净利润增长率'] = pd_data['NETPROFIT'].pct_change(1)
    pd_data['营业收入增长率'] = pd_data['OPERATE_INCOME'].pct_change(1)
    pd_data['总资产增长率'] = pd_data['TOTAL_ASSETS'].pct_change(1)
    pd_data['净资产增长率'] = pd_data['TOTAL_EQUITY'].pct_change(1)
    pd_data['营业利润增长率'] = pd_data['OPERATE_PROFIT'].pct_change(1)

    # 公司发展指标
    future_dev_metric_cols = ['净利润增长率', '营业收入增长率', '总资产增长率', '净资产增长率', '营业利润增长率']
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
        data = pd.pivot_table(pd_data, values=metric, index=['date'], columns=['code']).tail(10)
        data = data.rename(columns=rename_code)
        data.plot(kind='bar', title=metric, rot=45, width=0.5, figsize=(15, 8), fontsize=10)
        plt.show()

    score_df = score_df_list[0]
    for ele in score_df_list[1:]:
        score_df = pd.merge(score_df, ele, left_on=['code', 'date'], right_on=['code', 'date'])
    score_df['total_score'] = score_df.apply(handle_score, axis=1, args=(metric_core_col,))

    data = pd.pivot_table(score_df, values='total_score', index=['date'], columns=['code']).tail(10)
    data = data.rename(columns=rename_code)
    show_data(data)
    data.plot(kind='bar', title='分数', rot=45, width=0.5, figsize=(15, 8), fontsize=10)
    plt.show()


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


if __name__ == '__main__':
    credit_funds_fin_inst_analysis()
