from datetime import datetime, timedelta
from monitor.real_common import st_peak_data
from utils.tool import *
import matplotlib.pyplot as plt
from utils.send_msg import MailSender
from monitor.real_common import cal_linear_data_fn
from indicator.talib_indicator import adj_obv,common_indictator_cal

# 设置中文显示不乱码
plt.rcParams['font.sans-serif'] = ['Arial Unicode MS']
import warnings
import os
import akshare as ak

warnings.filterwarnings('ignore')
show_indicators = ['ADX', 'minus_di', 'plus_di', 'H_line_40', 'M_line_40', 'L_line_40', 'K', 'D', 'obv120_cross']



def index_future_long_short_rate_image():
    database = 'futures'
    collection = 'futures_basic_info'
    sort_key = "date"
    codes = ak.match_main_contract(symbol='cffex').split(",")
    start_time = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    projection = {'_id': False}
    condition = {"data_type": "futures_long_short_rate", "date": {"$gt": start_time}, "code": {"$in": codes}}
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    code_dict = {
        "IM": "中证1000",
        "IH": "上证50",
        "IC": "中证500",
        "IF": "沪深300",
    }

    fig, axes = plt.subplots(4, 1, figsize=(20, 10))
    i = 0
    for code in codes:
        name = code_dict.get(code[0:2])
        if name is not None:
            future_date = code[2:]
            name = f"{future_date}{name}"
            ele_data = data[data['code'] == code]
            ele_data.set_index(keys='date', inplace=True)
            ele_data[['long_short_rate']].plot(ax=axes[i], grid=True, title=f'{name}多空比')
            plt.legend(loc='best', shadow=True)
            i += 1
    plt.savefig("plot/future_market_date.png")
def get_market_data_by_code_name(database='stock', collection='index_data', sort_key="date", codes=None,
                                 code_name='code'):
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
    data['adj_obv'] = adj_obv(data.high, data.low, data.close, data.volume)
    return data


def plot_bar_value_line_same_rate_data(dates, volume_data, close_data, is_high_data, is_low_data, is_linear_high_data,
                                       is_linear_low_data, filename='market.png', is_show=False, title='图形分析',
                                       futures_hold=None, adj_odv_values=None):
    plt.figure(figsize=(20, 10))
    plt.subplot(2, 1, 1)
    # 绘柱状图
    plt.bar(x=dates, height=volume_data, label='成交量', color='Coral', alpha=0.8)
    if futures_hold is not None:
        plt.plot(dates, futures_hold, 'b', marker='.', c='b', ms=5, linewidth='1', label='持仓兴趣')
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
    if adj_odv_values is not None:
        plt.subplot(2, 1, 2)
        plt.plot(dates, adj_odv_values, 'k', marker='.', c='k', ms=5, linewidth='1', label='调整计算的obv')
        plt.xlabel('时间')
        plt.ylabel('调整计算的obv')
        plt.xticks(rotation=45)
    plt.savefig("plot/" + filename)
    if is_show:
        plt.show()
    plt.close()


def common_plot_send_mail(database='stock', collection='ticker_daily', sort_key="time", mail_theme='股票走势图',
                          ticker_config=None, code_name='code',is_index_future_send=False):
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
        cid = combine_dict['code']
        if 'header' not in combine_dict.keys():
            header = f'{name}成交量以及走势图'
        else:
            header = combine_dict['header']
        data = get_market_data_by_code_name(database=database, collection=collection, sort_key=sort_key, codes=[code],
                                            code_name=code_name)
        common_indictator_cal(data,ma_timeperiod=20)
        data = data.tail(50)
        x = data[sort_key].values
        y = data['volume'].values
        close = data['close'].values
        is_low_data = data['is_low']
        is_high_data = data['is_peak']
        adj_obv_val = data['adj_obv'].values

        indicator_text = ''
        for indicator in show_indicators:
            val = round(data[indicator].values[-1],2)
            indicator_text += f"{indicator}={val}&ensp;;"

        linear_result = cal_linear_data_fn(x, close, is_high_data, is_low_data)
        low_keys = list(linear_result['low'].keys())
        high_keys = list(linear_result['high'].keys())
        plot_bar_value_line_same_rate_data(x, y, close, is_high_data, is_low_data, linear_result['high'][high_keys[-1]],
                                           linear_result['low'][low_keys[-1]], filename=f'{cid}.png', title=header,
                                           adj_odv_values=adj_obv_val)
        high_price = round(linear_result['high'][high_keys[-1]][-1], 2)
        lower_price = round(linear_result['low'][low_keys[-1]][-1], 2)
        last_price = close[-1]
        price_str = f"当前价格{last_price},反转价格:{lower_price},目标价格{high_price}"
        mail_msg += f"<p>{header};价格判断:{price_str}</p><p>指标:{indicator_text}</p>"
        mail_msg += f"<img src=\"cid:{cid}\" width=\"99%\">"
        with open(f"plot/{cid}.png", 'rb') as f:
            stram = f.read()
            name_stram_dict.setdefault(cid, stram)
    if is_index_future_send:
        if os.path.exists('plot/future_market_date.png'):
            mail_msg += "<p>股指期货多空比走势图</p>"
            mail_msg += f"<img src=\"cid:future_market_date\" width=\"99%\">"
            with open(f"plot/future_market_date.png", 'rb') as f:
                stram = f.read()
                name_stram_dict.setdefault('future_market_date', stram)
    mail = MailSender()
    mail.send_html_with_img_data(['905198301@qq.com'], ['2394023336@qq.com'], mail_theme, mail_msg, name_stram_dict)


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
    #股指期货多空比和大盘相关
    #index_future_long_short_rate_image()
    common_plot_send_mail(database, collection, sort_key, mail_theme, market_config,is_index_future_send=False)


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
        "金禾实业": {"code": "002597", "cid": "002597", "header": "金禾实业成交量以及走势图"},
        "雅克科技": {"code": "002409", "cid": "002409", "header": "雅克科技成交量以及走势图"},
        "世纪华通": {"code": "002602", "cid": "002602", "header": "世纪华通成交量以及走势图"},
        "伊利股份": {"code": "600887", "cid": "600887", "header": "伊利股份成交量以及走势图"},
        "汇川技术": {"code": "300124", "cid": "300124", "header": "汇川技术成交量以及走势图"},
        "浪潮信息": {"code": "000977", "cid": "000977", "header": "浪潮信息成交量以及走势图"},
        "中国联通": {"code": "600050", "cid": "600050", "header": "中国联通成交量以及走势图"},
        "海康威视": {"code": "002415", "cid": "002415", "header": "海康威视成交量以及走势图"},
        "科大讯飞": {"code": "002230", "cid": "002230", "header": "科大讯飞成交量以及走势图"},
        "重庆啤酒": {"code": "600132", "cid": "600132", "header": "重庆啤酒成交量以及走势图"},
        "青岛啤酒": {"code": "600600", "cid": "600600", "header": "青岛啤酒成交量以及走势图"},
        "景嘉微": {"code": "300474", "cid": "300474", "header": "景嘉微成交量以及走势图"},
        "华能国际": {"code": "600011", "cid": "600011", "header": "华能国际成交量以及走势图"},
        "五粮液": {"code": "000858", "cid": "000858", "header": "五粮液成交量以及走势图"},
        "贵州茅台": {"code": "600519", "cid": "600519", "header": "贵州茅台成交量以及走势图"},
        "三七互娱": {"code": "002555", "cid": "002555", "header": "三七互娱成交量以及走势图"},
        "爱尔眼科": {"code": "300015", "cid": "300015", "header": "爱尔眼科成交量以及走势图"},
        "杭州银行": {"code": "600926", "cid": "600926", "header": "杭州银行成交量以及走势图"},
        "成都银行": {"code": "601838", "cid": "601838", "header": "成都银行成交量以及走势图"},
        "招商银行": {"code": "600036", "cid": "600036", "header": "招商银行成交量以及走势图"},
        "中国银行": {"code": "601988", "cid": "601988", "header": "中国银行成交量以及走势图"},
        "宁德时代": {"code": "300750", "cid": "300750", "header": "宁德时代成交量以及走势图"},
        "中际旭创": {"code": "300308", "cid": "300308", "header": "中际旭创成交量以及走势图"},
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
        "玻璃主连": {"code": "FG0", "cid": "FG0", "header": "玻璃成交量以及走势图"},
        "乙二醇主连": {"code": "EG0", "cid": "EG0", "header": "乙二醇成交量以及走势图"},
        "螺纹钢主连": {"code": "RB0", "cid": "RB0", "header": "螺纹钢成交量以及走势图"},
        "纯碱主连": {"code": "SA0", "cid": "SA0", "header": "纯碱成交量以及走势图"},
        "沥青主连": {"code": "BU0", "cid": "BU0", "header": "沥青成交量以及走势图"},
    }
    name_stram_dict = {}
    mail_msg = ""
    judge_future_config = [
        {"price": "up", "volume": "up", "hold": "up", "result": "坚挺"},
        {"price": "up", "volume": "down", "hold": "down", "result": "疲弱"},
        {"price": "down", "volume": "up", "hold": "up", "result": "疲弱"},
        {"price": "down", "volume": "down", "hold": "down", "result": "坚挺"},
    ]

    def jude_current_market(row: pd.Series):
        volume = row['volume']
        pre_volume = row['pre_volume']
        pct_change = row['ret_1']
        hold = row['hold']
        pre_hold = row['pre_hold']
        result_dict = {}
        if volume > pre_volume:
            result_dict['volume'] = 'up'
        else:
            result_dict['volume'] = 'down'

        if pct_change > 0:
            result_dict['price'] = 'up'
        else:
            result_dict['price'] = 'down'

        if hold > pre_hold:
            result_dict['hold'] = 'up'
        else:
            result_dict['hold'] = 'down'
        for config in judge_future_config:
            is_pass = True
            for k in ['price', 'volume', 'hold']:
                if config[k] != result_dict.get(k):
                    is_pass = False
                    break
            if is_pass:
                result_dict['result'] = config.get('result')
                break
        return result_dict.get('result', '无法判断市场行情')

    for name, combine_dict in ticker_config.items():
        code = combine_dict['code']
        cid = combine_dict['code']
        if 'header' not in combine_dict.keys():
            header = f'{name}成交量以及走势图'
        else:
            header = combine_dict['header']
        data = get_market_data_by_code_name(database=database, collection=collection, sort_key=sort_key, codes=[code],
                                            code_name=code_name)
        common_indictator_cal(data,ma_timeperiod=20)
        data['pre_volume'] = data['volume'].shift(1)
        data['ret_1'] = data['close'].pct_change(1)
        data['pre_hold'] = data['hold'].shift(1)
        data['market_env'] = data.apply(jude_current_market, axis=1)
        data = data.tail(50)
        indicator_text = ''
        for indicator in show_indicators:
            val = round(data[indicator].values[-1], 2)
            indicator_text += f"{indicator}={val}&ensp;;"
        x = data[sort_key].values
        y = data['volume'].values
        close = data['close'].values
        is_low_data = data['is_low']
        is_high_data = data['is_peak']
        hold = data['hold'].values
        adj_obv_val = data['adj_obv'].values
        linear_result = cal_linear_data_fn(x, close, is_high_data, is_low_data)
        low_keys = list(linear_result['low'].keys())
        high_keys = list(linear_result['high'].keys())
        plot_bar_value_line_same_rate_data(x, y, close, is_high_data, is_low_data, linear_result['high'][high_keys[-1]],
                                           linear_result['low'][low_keys[-1]], filename=f'{cid}.png', title=header,
                                           futures_hold=hold, adj_odv_values=adj_obv_val)
        market_env = data.tail(1).iloc[0]['market_env']
        high_price = round(linear_result['high'][high_keys[-1]][-1], 2)
        lower_price = round(linear_result['low'][low_keys[-1]][-1], 2)
        last_price = close[-1]

        price_str = f"当前价格{last_price},反转价格:{lower_price},目标价格{high_price}"
        mail_msg += f"<p>{header};市场判断:{market_env};价格判断:{price_str}</p></p><p>指标:{indicator_text}</p>"
        mail_msg += f"<img src=\"cid:{cid}\" width=\"99%\">"
        with open(f"plot/{cid}.png", 'rb') as f:
            stram = f.read()
            name_stram_dict.setdefault(cid, stram)

    mail = MailSender()
    mail.send_html_with_img_data(['905198301@qq.com'], ['2394023336@qq.com'], mail_theme, mail_msg,
                                 name_stram_dict)


def del_file(path_data):
    for i in os.listdir(path_data):
        file_data = path_data + "/" + i
        if os.path.isfile(file_data) == True:
            os.remove(file_data)
            print(f"删除文件{file_data}")


if __name__ == '__main__':
    del_file('plot')
    daily_market_plot_notice()
    daily_stock_plot_notice()
    daily_future_plot_notice()
