import akshare as ak
import pandas as pd

from utils.actions import show_data
from monitor.comm_mail_utils import comm_construct_mail_str
from utils.send_msg import MailSender
from datetime import datetime, timedelta
from utils.tool import get_data_from_mongo
from risk_manager.macro_risk import comm_down_or_up_risk


def stock_daily_margin_data(market_type="sz", code_map: list = None):
    """
    股票融资融券
    :return:
    """
    database = 'stock'
    collection = 'stock_margin_daily'
    projection = {'_id': False}

    sh_col_dict = {
        "fin_balance": "融资余额",
        "fin_purchase_amount": "融资买入额",
        "sec_selling_volume": "融券卖出量",
    }

    sz_col_dict = {
        "fin_balance": "融资余额",
        "fin_purchase_amount": "融资买入额",
        "sec_selling_volume": "融券卖出量",
        'sec_lending_balance': '融券余额'
    }

    stock_market_type_map = {"sz": sz_col_dict, "sh": sh_col_dict}
    up_down_dict_map = {"sz": {
        "fin_balance": "down",
        "fin_purchase_amount": "down",
        "sec_selling_volume": "up",
        'sec_lending_balance': 'up'
    }, "sh": {
        "fin_balance": "down",
        "fin_purchase_amount": "down",
        "sec_selling_volume": "up",
    }}
    if code_map is None:
        code_map = ["002555", "三七互娱"]
    code, name = code_map
    col_dict = stock_market_type_map[market_type]
    time = (datetime.now() - timedelta(days=365 * 2)).strftime("%Y0101")
    condition = {"code": code, "time": {"$gte": time}}
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.set_index(keys='time', inplace=True)
    risks, datas = comm_down_or_up_risk(data, list(col_dict.keys()), [1, 2, 3, 4, 5, 6], up_down_dict_map[market_type],
                                        'index')
    risk_df = pd.DataFrame(datas)
    col_dict['code'] = '股票代码'
    col_dict['name'] = '股票名称'
    col_dict['time'] = '时间'
    col_dict['total_risk'] = '总风险'
    html_str = comm_construct_mail_str(col_dict, risk_df, f'{name}融资融券', is_tail=True, format_cols=['total_risk'])
    return html_str




def market_daily_margin_data():
    """
    市场融资融券
    :return:
    """


def stock_dzjy_data():
    """
    股票大宗交易
    :return:
    """
    pass


def stock_plot_data():
    """
    股票画图分析
    :return:
    """
    pass


def stock_news_data():
    """
    股票新闻数据
    :return:
    """
def main_email():
    code_dict = {
        "600926": "杭州银行",
        "300474": "景嘉微",
        "600050": "中国联通",
        "002230": "科大讯飞",
        "000977": "浪潮信息",
        "002555": "三七互娱",
        "600011": "华能国际",
        "600887": "伊利股份",
        "002602": "世纪华通",
        "002415": "海康威视",
        "300124": "汇川技术",
        "002409": "雅克科技",
    }
    html_str = ""
    for code,name in code_dict.items():
        if code[0]=='6':
            market_type = 'sh'
        else:
            market_type = 'sz'
        html_str += stock_daily_margin_data(market_type=market_type,code_map=[code,name])
    mail = MailSender()
    mail.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], f'股票融资融券数据',
                        html_str)


if __name__ == '__main__':
    code_dict = {
        "600926": "杭州银行",
        "300474": "景嘉微",
        "600050": "中国联通",
        "002230": "科大讯飞",
        "000977": "浪潮信息",
        "002555": "三七互娱",
        "600887": "伊利股份",
    }

    main_email()
