from datetime import datetime, timedelta
from monitor.real_common import st_peak_data
from utils.tool import *
import matplotlib.pyplot as plt
from utils.send_msg import MailSender

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings

warnings.filterwarnings('ignore')


def get_market_data_by_code_name(database='stock', collection='index_data', sort_key="date", codes=None,code_name='code'):
    start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")
    if codes is None:
        # sh000001,sz399001
        codes = ['sz399001']
    condition = {code_name: {"$in": codes}, sort_key: {"$gte": start_date}}
    projection = {'_id': False}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    data.index = pd.to_datetime(data[sort_key])
    st_peak_data(data, sort_key)
    return data


def plot_bar_value_line_same_rate_data(dates, volume_data, close_data, is_high_data, is_low_data, is_linear_high_data,
                                       is_linear_low_data, filename='market.png', is_show=False, title='图形分析'):
    plt.figure(figsize=(12, 6))
    # 绘柱状图
    plt.bar(x=dates, height=volume_data, label='成交量', color='Coral', alpha=0.8)
    # 在左侧显示图例
    plt.legend(loc="upper left")

    # 设置标题
    plt.title(title)
    # 为两条坐标轴设置名称
    plt.xlabel('时间')
    plt.ylabel('成交量')
    # 旋转45度
    plt.xticks(rotation=45)

    # 画折线图
    ax2 = plt.twinx()
    ax2.set_ylabel('收盘价')
    # 设置坐标轴范围
    plt.plot(dates, close_data, "r", marker='.', c='r', ms=5, linewidth='1', label='收盘价')
    # 显示数字
    for date, is_high, is_low, close in zip(dates, is_high_data, is_low_data, close_data):
        if is_high == 1:
            plt.annotate('h', xy=(date, close), arrowprops=dict(facecolor='r', shrink=0.05))
        if is_low == 1:
            plt.annotate('l', xy=(date, close), arrowprops=dict(facecolor='g', shrink=0.1))
    # 在右侧显示图例

    plt.plot(dates[-30:], is_linear_high_data[-30:], "r", marker='.', c='r', ms=5, linewidth='1', label='高谷线')
    plt.plot(dates[-30:], is_linear_low_data[-30:], "r", marker='.', c='r', ms=5, linewidth='1', label='低谷线')
    plt.legend(loc="upper right")
    plt.savefig(filename)
    if is_show:
        plt.show()


def linear_fn(k, x1, y1, cal_x2):
    return k * (cal_x2 - x1) + y1


def handle_point(is_high_or_low, temp_points_list: list, linear_fn_dict: dict, num, combine_data):
    if is_high_or_low == 1:
        temp_points_list.append(combine_data)
    if len(temp_points_list) == 2:
        first_point = temp_points_list[0]
        second_point = temp_points_list[1]
        y0, x0 = first_point[1], 0
        y1, x1 = second_point[1], second_point[2] - first_point[2]
        k = (y1 - y0) / (x1 - x0)
        linear_fn_dict[first_point[0]] = [k, y0, first_point[2], num, second_point]
        temp_points_list.pop(0)


def cal_seq_linear_point(linear_fn_dict: dict, result: dict, type='high'):
    for date, combine_list in linear_fn_dict.items():
        cal_list_data = []
        k, y0, start, end, x1 = combine_list
        for i in range(0, start):
            ele = linear_fn(k, 0, y0, -(start - i))
            cal_list_data.append(ele)
        for i in range(start, end):
            ele = linear_fn(k, 0, y0, i - start)
            cal_list_data.append(ele)
        result[type][date] = cal_list_data


def cal_linear_data_fn(dates, close_data, is_high_data, is_low_data):
    temp_high_linear_data = []
    temp_low_linear_data = []
    start_index = 0
    high_linear_fn_dict_data = {}
    low_linear_fn_dict_data = {}
    for date, close, is_high, is_low in zip(dates, close_data, is_high_data, is_low_data):
        combine_data = [date, close, start_index]
        handle_point(is_high, temp_high_linear_data, high_linear_fn_dict_data, len(dates), combine_data)
        handle_point(is_low, temp_low_linear_data, low_linear_fn_dict_data, len(dates), combine_data)
        start_index += 1

    linear_result = {"high": {}, "low": {}}
    cal_seq_linear_point(high_linear_fn_dict_data, linear_result, 'high')
    cal_seq_linear_point(low_linear_fn_dict_data, linear_result, 'low')
    return linear_result



def common_plot_send_mail(database='stock', collection='ticker_daily', sort_key="time", mail_theme='股票走势图',
                          ticker_config=None,code_name='code'):
    if ticker_config is None:
        ticker_config = {
            "汇川技术": {"code": "300124", "cid": "300124", "header": "汇川技术成交量以及走势图"},
            "领益制造": {"code": "002600", "cid": "002600", "header": "领益制造成交量以及走势图"},
            "中国联通": {"code": "600050", "cid": "600050", "header": "中国联通成交量以及走势图"},
            "海康威视": {"code": "002415", "cid": "002415", "header": "海康威视成交量以及走势图"},
            "科大讯飞": {"code": "002230", "cid": "002230", "header": "科大讯飞成交量以及走势图"},
            "重庆啤酒": {"code": "600132", "cid": "600132", "header": "重庆啤酒成交量以及走势图"},
            "青岛啤酒": {"code": "600600", "cid": "600600", "header": "青岛啤酒成交量以及走势图"},
        }
    name_stram_dict = {}
    mail_msg = ""
    for name, combine_dict in ticker_config.items():
        code = combine_dict['code']
        cid = combine_dict['cid']
        header = combine_dict['header']
        data = get_market_data_by_code_name(database=database, collection=collection, sort_key=sort_key, codes=[code],code_name=code_name)
        data = data.tail(50)
        x = data[sort_key].values
        y = data['volume'].values
        close = data['close'].values
        is_low_data = data['is_low']
        is_high_data = data['is_peak']
        linear_result = cal_linear_data_fn(x, close, is_high_data, is_low_data)
        low_keys = list(linear_result['low'].keys())
        high_keys = list(linear_result['high'].keys())
        plot_bar_value_line_same_rate_data(x, y, close, is_high_data, is_low_data, linear_result['high'][high_keys[-1]],
                                           linear_result['low'][low_keys[-1]], filename=f'{cid}.png', title=header)

        mail_msg += f"<p>{header}</p>"
        mail_msg += f"<img src=\"cid:{cid}\" width=\"99%\">"
        with open(f"{cid}.png", 'rb') as f:
            stram = f.read()
            name_stram_dict.setdefault(cid, stram)

    mail = MailSender()
    mail.send_html_with_img_data(['905198301@qq.com'], ['2394023336@qq.com'], mail_theme, mail_msg,
                                 name_stram_dict)


def daily_market_plot_notice():
    """
    大盘指数基础版本发送图表分析
    :return:
    """
    market_config = {"上证": {"code": "sh000001", "cid": "sh000001", "header": "上证指数成交量以及走势图"},
                     "深证": {"code": "sz399001", "cid": "sz399001", "header": "深证指数成交量以及走势图"},
                     }
    database = 'stock'
    collection = 'index_data'
    sort_key = "date"
    mail_theme = '大盘走势'
    common_plot_send_mail(database, collection, sort_key, mail_theme, market_config)


def daily_stock_plot_notice():
    """
    股票基础版本发送图表分析
    :return:
    """
    database = 'stock'
    collection = 'ticker_daily'
    sort_key = "time"
    mail_theme = '股票走势图'
    ticker_config = {
        "汇川技术": {"code": "300124", "cid": "300124", "header": "汇川技术成交量以及走势图"},
        "领益制造": {"code": "002600", "cid": "002600", "header": "领益制造成交量以及走势图"},
        "中国联通": {"code": "600050", "cid": "600050", "header": "中国联通成交量以及走势图"},
        "海康威视": {"code": "002415", "cid": "002415", "header": "海康威视成交量以及走势图"},
        "科大讯飞": {"code": "002230", "cid": "002230", "header": "科大讯飞成交量以及走势图"},
        "重庆啤酒": {"code": "600132", "cid": "600132", "header": "重庆啤酒成交量以及走势图"},
        "青岛啤酒": {"code": "600600", "cid": "600600", "header": "青岛啤酒成交量以及走势图"},
    }
    common_plot_send_mail(database, collection, sort_key, mail_theme, ticker_config)


def daily_future_plot_notice():
    """
    期货基础版本发送图表分析
    :return:
    """
    database = 'futures'
    collection = 'futures_daily'
    sort_key = "date"
    mail_theme = '期货走势图'
    code_name = 'symbol'
    ticker_config = {
        "玻璃主连": {"code": "FG0", "cid": "FG0", "header": "玻璃成交量以及走势图"}
    }
    common_plot_send_mail(database, collection, sort_key, mail_theme, ticker_config,code_name=code_name)





if __name__ == '__main__':
    daily_market_plot_notice()
    daily_stock_plot_notice()
    daily_future_plot_notice()
