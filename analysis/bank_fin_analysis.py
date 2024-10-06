from datetime import datetime, timedelta
import os

import pandas as pd

from analysis.common_analysis import BasicAnalysis
from data.stock_detail_fin import handle_comm_stock_fin_em
from utils.actions import show_data
from data.common_get_data import get_ticker_info_data
from pyecharts.charts import Tab

class BankFinAnalysis(BasicAnalysis):

    def __init__(self, *args, **kwargs):
        self.code = kwargs['code']
        self.date = kwargs.get("date", str(datetime.now().year - 1) + "年度")
        self.fin_start_date = kwargs.get('fin_start_date',
                                         (datetime.now() - timedelta(days=365 * 20)).strftime("%Y-%m-%d"))
        self.cal_cur_cols = kwargs.get("cal_cur_cols", None)
        self.name = kwargs.get('name', self.code)
        self.dir = kwargs.get("dir", None)
        is_local = kwargs.get("is_local", True)
        if self.dir and not os.path.exists(self.dir):
            os.mkdir(self.dir)

        if int(self.code[0]) < 6:
            self.pre_market_code = f"sz{self.code}"
        elif int(self.code[0]) == 6:
            self.pre_market_code = f'sh{self.code}'
        elif int(self.code[0]) == 8:
            self.pre_market_code = f"bj{self.code}"
        else:
            print("没有找到有前缀的数据")
            self.pre_market_code = self.code
        self.google_model = None
        self.current_price_market_info = None
        self.fin_data = None
        self.__load_config()
        self.__load_fin_data(is_local)

    def __load_config(self):
        self.bank_profit_col = {"OPERATE_INCOME": "营业收入",
                                'INTEREST_NI': '利息净收入',
                                'INTEREST_INCOME': '其中:利息收入',
                                'INTEREST_EXPENSE': '利息支出',
                                'INVEST_INCOME': '加:投资收益',
                                'FEE_COMMISSION_NI': '手续费及佣金净收入',
                                'FEE_COMMISSION_INCOME': '其中:手续费及佣金收入',
                                'FEE_COMMISSION_EXPENSE': '手续费及佣金支出',
                                'INVEST_JOINT_INCOME': '其中:对联营企业和合营企业的投资收益/（损失）',
                                'FAIRVALUE_CHANGE_INCOME': '公允价值变动收益',
                                'EXCHANGE_INCOME': '汇兑损失',
                                'OTHER_BUSINESS_INCOME': '其他业务收入',
                                'ASSET_DISPOSAL_INCOME': '资产处置收益',
                                'OTHER_INCOME': '其他收益',

                                'OPERATE_EXPENSE': '营业支出',
                                'OPERATE_TAX_ADD': '营业税金及附加',
                                'BUSINESS_MANAGE_EXPENSE': '业务及管理费',
                                'CREDIT_IMPAIRMENT_LOSS': '信用减值损失',
                                'OTHER_BUSINESS_COST': '其他业务成本',

                                'OPERATE_PROFIT': '营业利润',
                                'NONBUSINESS_INCOME': '加:营业外收入',
                                'NONBUSINESS_EXPENSE': '减:营业外支出',

                                'TOTAL_PROFIT': '利润总额',
                                'INCOME_TAX': '减:所得税',
                                'NETPROFIT': '净利润',
                                'DEDUCT_PARENT_NETPROFIT': '扣除非经常性损益后的净利润',

                                'BASIC_EPS': '基本每股收益',
                                'DILUTED_EPS': '稀释每股收益',
                                }

        self.bank_asset_col = {
            # 负债相关
            'LOAN_PBC': '向中央银行借款',
            'IOFI_DEPOSIT': '同业及其他金融机构存放款项',
            'BORROW_FUND': '拆入资金',
            'ACCEPT_DEPOSIT': '吸收存款',
            'BOND_PAYABLE': '应付债券',
            'TOTAL_LIABILITIES': '负债合计',
            # 资产相关
            'CASH_DEPOSIT_PBC': '现金及存放中央银行款项',
            'DEPOSIT_INTERBANK': '存放同业款项',
            'LEND_FUND': '拆出资金',
            'LOAN_ADVANCE': '发放贷款及垫款',
            'TRADE_FINASSET_NOTFVTPL': '交易性金融资产',
            'CREDITOR_INVEST': '债权投资',
            'OTHER_CREDITOR_INVEST': '其他债权投资',
            'TOTAL_ASSETS': '资产总计',

        }
        self.bank_cash_flow_col = {
            # 经营活动
            'DEPOSIT_IOFI_OTHER': '客户存款和同业及其他金融机构存放款项净增加额',
            'PBC_IOFI_REDUCE': '存放中央银行和同业款项及其他金融机构净减少额',
            'BORROW_REPO_ADD': '拆入资金及卖出回购金融资产款净增加额',
            'SELL_REPO_ADD': '卖出回购金融资产款净增加额',
            'RECEIVE_INTEREST_COMMISSION': '收取利息、手续费及佣金的现金',
            'TOTAL_OPERATE_INFLOW': '经营活动现金流入小计',

            'LOAN_ADVANCE_ADD': '客户贷款及垫款净增加额',
            'LOAN_PBC_REDUCE': '向中央银行借款净减少额',
            'BORROW_REPO_REDUCE': '拆入资金及卖出回购金融资产款净减少额',
            'BORROW_FUND_REDUCE': '其中:拆入资金净减少额',
            'TRADE_FINASSET_ADD': '交易性金融资产净增加额',
            'PAY_INTEREST_COMMISSION': '支付利息、手续费及佣金的现金',
            'PAY_STAFF_CASH': '支付给职工以及为职工支付的现金',
            'PAY_ALL_TAX': '支付的各项税费',
            'PAY_OTHER_OPERATE': '支付其他与经营活动有关的现金',
            'TOTAL_OPERATE_OUTFLOW': '经营活动现金流出小计',

            'NETCASH_OPERATE': '经营活动产生的现金流量净额',

            # 投资活动
            'WITHDRAW_INVEST': '收回投资收到的现金',
            'RECEIVE_INVEST_INCOME': '取得投资收益收到的现金',
            'DISPOSAL_LONG_ASSET': '处置固定资产、无形资产和其他长期资产收回的现金',
            'TOTAL_INVEST_INFLOW': '投资活动现金流入小计',

            'INVEST_PAY_CASH': '投资支付的现金',
            'CONSTRUCT_LONG_ASSET': '购建固定资产、无形资产和其他长期资产支付的现金',
            'TOTAL_INVEST_OUTFLOW': '投资活动现金流出小计',
            'NETCASH_INVEST': '投资活动产生的现金流量净额',

            # 筹资活动
            'ISSUE_BOND': '发行债券收到的现金',
            'ACCEPT_INVEST_CASH': '吸收投资收到的现金',
            'TOTAL_FINANCE_INFLOW': '筹资活动现金流入小计',

            'PAY_DEBT_CASH': '偿还债务所支付的现金',
            'ASSIGN_DIVIDEND_PORFIT': '分配股利、利润或偿付利息支付的现金',
            'FINANCE_OUTFLOW_OTHER': '分配股利、筹资活动现金流出的其他项目',
            'TOTAL_FINANCE_OUTFLOW': '筹资活动现金流出小计',
            'NETCASH_FINANCE': '筹资活动产生的现金流量净额',
        }

        self.data_col_dict = {
            "profit_report_em_detail": self.bank_profit_col,
            "cash_flow_report_em_detail": self.bank_cash_flow_col,
            "zcfz_report_detail": self.bank_asset_col,
        }

    def __load_fin_data(self, is_local=True):

        def convert_ele(ele):
            if ele == '--' or str(ele) == 'nan':
                ele = 0
            # if float(ele) < 0:
            #     return 0
            return float(ele)
        if not is_local:
            for data_type in ['profit_report_em_detail', 'cash_flow_report_em_detail', 'zcfz_report_detail']:
                handle_comm_stock_fin_em([self.pre_market_code], data_type=data_type)
        codes = [self.code]
        df_datas = []
        for d_type, cols in self.data_col_dict.items():
            condition = {"code": {"$in": codes}, "date": {"$gte": self.fin_start_date}, "data_type": {
                "$in": [d_type]}}
            database = 'stock'
            collection = 'fin'
            projection = {'_id': False, "code": True, "date": True, "data_type": True}
            for col in cols:
                projection[col] = True
            sort_key = "date"
            fin_data = self.get_data_from_mongondb(database, collection, projection, condition, sort_key)
            get_db_cols = list(fin_data.columns)
            for col in cols:
                if col in get_db_cols:
                    fin_data[col] = fin_data[col].apply(convert_ele)
                else:
                    fin_data[col] = 0

            if len(fin_data) > 0:
                df_datas.append(fin_data)
        if len(df_datas) >= 2:
            temp_df = pd.merge(df_datas[0], df_datas[1], on=['date', 'code'], how='left')
            for df in df_datas[2:]:
                temp_df = pd.merge(temp_df, df, on=['date', 'code'], how='left')
            self.fin_data = temp_df
        else:
            print("缺少数据请检查")
            self.fin_data = df_datas[0]

    def cal_indicator(self):
        def mid_fn(cur_dict_data: dict, before_dict_data: dict):
            cur_dict_data['营业收入同比增长率'] = round(
                (cur_dict_data['OPERATE_INCOME'] - before_dict_data['OPERATE_INCOME']) / before_dict_data[
                    'OPERATE_INCOME'], 4)
            cur_dict_data['净利润同比增长率'] = round(
                (cur_dict_data['NETPROFIT'] - before_dict_data['NETPROFIT']) / before_dict_data['NETPROFIT'], 4)
            cur_dict_data['扣除非经常性损益后的净利润同比增长率'] = round(
                (cur_dict_data['DEDUCT_PARENT_NETPROFIT'] - before_dict_data['DEDUCT_PARENT_NETPROFIT']) /
                before_dict_data[
                    'DEDUCT_PARENT_NETPROFIT'], 4)

        change_config_dict = {
            "OPERATE_PROFIT": {"sub_key": {
                "OPERATE_INCOME": "营业总收入",
                'INTEREST_NI': '利息净收入',
                'INTEREST_INCOME': '其中:利息收入',
                'INVEST_INCOME': '投资收益',
                'FEE_COMMISSION_NI': '手续费及佣金净收入',
                'FEE_COMMISSION_INCOME': '其中:手续费及佣金收入',
                'ASSET_DISPOSAL_INCOME': '资产处置收益',
                'OTHER_INCOME': '其他收益',
            }, "name": "营业利润分析"},
        }
        self.indicator_df = pd.DataFrame(self.common_cal_fin_result(self.fin_data, change_config_dict, analysis_type='profit', def_fn=mid_fn))

def bank_type_config():
    """
    银行分类
    :return:
    """
    ticker = get_ticker_info_data()
    bank_ticker = [ele for ele in ticker if ele.get('industry', '') == '银行']
    bank_config = {
        "big_bank": "工商银行,建设银行,农业银行,中国银行,交通银行,邮储银行",
        "joint_bank": "招商银行,浦发银行,中信银行,光大银行,华夏银行,民生银行,兴业银行,平安银行,浙商银行,渤海银行",
        "city_bank": "兰州银行,宁波银行,郑州银行,青岛银行,苏州银行,江苏银行,杭州银行,西安银行,南京银行,北京银行,厦门银行,上海银行,长沙银行,齐鲁银行,成都银行,重庆银行,贵阳银行",
        "far_bank": "江阴银行,张家港行,青农商行,渝农商行,常熟银行,瑞丰银行,沪农商行,紫金银行,苏农银行,无锡银行",
    }
    type_bank_config = {}
    for info in bank_ticker:
        name = info['name']
        for type, names in bank_config.items():
            if name in names.split(","):
                type_bank_config.setdefault(type, [])
                type_bank_config[type].append(info)
    return type_bank_config

def bank_fin_common_analysis(analysis_config_dict=None,analysis_file_name=None):
    dir = '银行'
    if not os.path.exists(dir):
        os.mkdir(dir)
    type_bk_name = {
        "big_bank": "大型银行",
        "joint_bank": "股份制银行",
        "city_bank": "城市银行",
        "far_bank": "农商银行",
    }
    type_bk_info = bank_type_config()
    all_bk_analysis = BasicAnalysis()
    tab = Tab()
    quarter = 2
    quarter_mapping = {1: "03-31", 2: "06-30", 3: "09-30", 4: "12-31"}
    quarter_month = quarter_mapping[quarter]

    all_add_data_dict = {}
    if analysis_config_dict is None:
        analysis_config_dict = {
            "OPERATE_INCOME": "营业总收入",
            "NETPROFIT": "净利润",
            'INTEREST_NI': '利息净收入',
            'FEE_COMMISSION_NI': '手续费及佣金净收入',
            'INVEST_INCOME': '加:投资收益',
        }
        analysis_file_name = "银行利润总体分析"
    if analysis_file_name is None and analysis_config_dict is None:
        raise Exception("error for config file and dict please check check!!!")

    for type_key, bk_infos in type_bk_info.items():
        add_data_dict = {}
        for ak in analysis_config_dict.keys():
            add_data_dict[ak] = {}
            all_add_data_dict.setdefault(ak, {})
        for bk in bk_infos:
            code = bk['code']
            ba = BankFinAnalysis(code=code, is_local=True)
            ba.cal_indicator()
            for index in ba.indicator_df.index:
                ele_dict = dict(ba.indicator_df.loc[index])
                date = ele_dict['date']
                for ak in analysis_config_dict.keys():
                    add_data_dict[ak].setdefault(date, 0)
                    all_add_data_dict[ak].setdefault(date, 0)
                    add_data_dict[ak][date] += ele_dict.get(ak, 0)
                    all_add_data_dict[ak][date] += ele_dict.get(ak, 0)

        for a_key, a_name in analysis_config_dict.items():
            data_dict = add_data_dict.get(a_key)
            datas = []
            for date, value in data_dict.items():
                datas.append({"date": date, "value": value, "data_type": a_key})
            df = pd.DataFrame(datas)
            df.sort_values(by="date", inplace=True)
            df['value_same'] = round(df['value'].pct_change(4), 2)
            df['value'] = round(df['value'] / 1e8, 2)
            df = df[df['date'].str.contains(quarter_month)]
            x_labels = list(df['date'].values)
            bar_ylables = {a_name + "单位亿": list(df['value'].values)}
            line_ylables = {a_name + "同比增长": list(df['value_same'].values)}
            bar = all_bk_analysis.bar_line_overlap(x_labels, bar_ylables, line_ylables)
            tab.add(bar, type_bk_name.get(type_key) + a_name + "分析")
    for a_key, a_name in analysis_config_dict.items():
        data_dict = all_add_data_dict.get(a_key)
        datas = []
        for date, value in data_dict.items():
            datas.append({"date": date, "value": value, "data_type": a_key})
        df = pd.DataFrame(datas)
        df.sort_values(by="date", inplace=True)
        df['value_same'] = round(df['value'].pct_change(4), 2)
        df['value'] = round(df['value'] / 1e8, 2)
        df = df[df['date'].str.contains(quarter_month)]
        x_labels = list(df['date'].values)
        bar_ylables = {a_name + "单位亿": list(df['value'].values)}
        line_ylables = {a_name + "同比增长": list(df['value_same'].values)}
        bar = all_bk_analysis.bar_line_overlap(x_labels, bar_ylables, line_ylables)
        tab.add(bar, 'all' + a_name + "分析")
    tab.render(f"{dir}/{analysis_file_name}.html")

if __name__ == '__main__':
    asset_analysis_config_dict = {
        'CASH_DEPOSIT_PBC': '现金及存放中央银行款项',
        'DEPOSIT_INTERBANK': '存放同业款项',
        'LEND_FUND': '拆出资金',
        'LOAN_ADVANCE': '发放贷款及垫款',
        'TRADE_FINASSET_NOTFVTPL': '交易性金融资产',
        'CREDITOR_INVEST': '债权投资',
        'OTHER_CREDITOR_INVEST': '其他债权投资',
        'TOTAL_ASSETS': '资产总计',
        'ACCEPT_DEPOSIT': '吸收存款',
    }
    asset_analysis_file_name = "资产结构分析"

    bank_fin_common_analysis(asset_analysis_config_dict,asset_analysis_file_name)