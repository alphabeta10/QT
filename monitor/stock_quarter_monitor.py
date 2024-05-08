import akshare as ak
from utils.actions import show_data
from monitor.comm_mail_utils import comm_construct_mail_str
from utils.send_msg import MailSender
from datetime import datetime


def quarter_stock_hold_msg(code_dict: dict = None, quarter_day=None):
    if code_dict is None:
        code_dict = {'002555': "三七互娱"}
    cur_day_str = datetime.now().strftime("%Y%m%d")
    cur_year = datetime.now().year
    cur_month = datetime.now().month
    quarter_mapping = {1: "0331", 2: "0630", 3: "0930", 4: "1231"}
    if quarter_day is None:
        if cur_month >= 1 and cur_month <= 3:
            quarter_num = 4
            cur_year = cur_year - 1
        elif cur_month > 3 and cur_month <= 6:
            quarter_num = 1
        elif cur_month > 6 and cur_month <= 9:
            quarter_num = 2
        elif cur_month > 9 and cur_month <= 12:
            quarter_num = 3
        else:
            print("no data")
            return
        month = quarter_mapping[quarter_num]
        quarter_day = f"{cur_year}{month}"
        institute_quarter_day = f"{cur_year}{quarter_num}"
    else:
        cur_year = quarter_day[0:4]
        quarter_num = None
        for num, month in quarter_mapping.items():
            if int(month[0:2]) == int(quarter_day[4:6]):
                quarter_num = num
                break
        if quarter_num is None:
            print("输入季度出错")
        institute_quarter_day = f"{cur_year}{quarter_num}"
    stock_gdhs_str = stock_gdhs_msg(code_dict)
    institute_str = stock_institute_hold(code_dict, institute_quarter_day)
    top10_hold_str = stock_gdfx_top_10(code_dict, quarter_day)
    all_send_str = institute_str + top10_hold_str + stock_gdhs_str
    mail = MailSender()
    mail.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], f'股票股东数以及机构变动；{quarter_day}季度数据',
                        all_send_str)


def stock_gdfx_top_10(code_dict: dict, date='20240331'):
    col_dict = {'名次': '名次', '股东名称': '股东名称', '股份类型': '股份类型', '持股数': '持股数',
                '占总股本持股比例': '占总股本持股比例', '增减': '增减', '变动比率': '变动比率'}
    top10_hold_str = ""
    for code, name in code_dict.items():
        if int(code[0]) >= 6:
            code = f"sh{code}"
        else:
            code = f"sz{code}"
        stock_gdfx_top_10_em_df = ak.stock_gdfx_top_10_em(symbol=code, date=date)
        top10_hold_str += comm_construct_mail_str(col_dict, stock_gdfx_top_10_em_df, f'{name}前10大股东持股', '',
                                                  is_tail=False, num=11)
    return top10_hold_str


def stock_gdhs_msg(code_dict: dict):
    """"
    季度 看股东数的变化，机构变化
    """
    stock_gdhs_str = ""
    col_dict = {'股东户数统计截止日': '股东户数统计截止日', '区间涨跌幅': '区间涨跌幅',
                '股东户数-本次': '股东户数-本次',
                '股东户数-上次': '股东户数-上次', '股东户数-增减': '股东户数-增减',
                '股东户数-增减比例': '股东户数-增减比例',
                '户均持股市值': '户均持股市值', '户均持股数量': '户均持股数量', '总市值': '总市值', '总股本': '总股本',
                '股本变动': '股本变动', '股本变动原因': '股本变动原因', '股东户数公告日期': '股东户数公告日期',
                '代码': '代码',
                '名称': '名称'}
    format_cols = ['股东户数-增减比例', '区间涨跌幅', '户均持股市值', '户均持股数量']
    for code, name in code_dict.items():
        # 股东户数季度
        stock_zh_a_gdhs_detail_em_df = ak.stock_zh_a_gdhs_detail_em(symbol=code)
        html_str = comm_construct_mail_str(col_dict, stock_zh_a_gdhs_detail_em_df, f'{name}股东数', '', is_tail=False,
                                           format_cols=format_cols)
        stock_gdhs_str += html_str
    return stock_gdhs_str


def stock_institute_hold(code_dict: dict, quarter='20241'):
    stock_institute_hold_df = ak.stock_institute_hold(symbol=quarter)
    col_dict = {'证券代码': '证券代码', '证券简称': '证券简称', '机构数': '机构数', '机构数变化': '机构数变化',
                '持股比例': '持股比例',
                '持股比例增幅': '持股比例增幅', '占流通股比例': '占流通股比例', '占流通股比例增幅': '占流通股比例增幅'}
    stock_institute_hold_df = stock_institute_hold_df[stock_institute_hold_df['证券代码'].isin(list(code_dict.keys()))]
    institue_hold_str = comm_construct_mail_str(col_dict, stock_institute_hold_df, f'{quarter}机构持股变动', '',
                                                is_tail=False)
    return institue_hold_str


if __name__ == '__main__':
    code_dict = {
        "600926": "杭州银行",
        "300474": "景嘉微",
        "600050": "中国联通",
        "002230": "科大讯飞",
        "000977": "浪潮信息",
        "002555": "三七互娱",
    }
    #code_dict = None
    quarter_stock_hold_msg(code_dict)
