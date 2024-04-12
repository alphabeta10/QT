import pandas as pd

from risk_manager.industry_risk import comm_cn_industry_metric_sort
from utils.tool import get_data_from_mongo,sort_dict_data_by
from utils.actions import show_data
from analysis.fin_analysis import analysis_fin_by_metric


def get_data_from_gov():
    code_dict = {"A02092U04_yd": "服务机器人产量累计增长(%)"}
    code_dict = {"A02092U02_yd": "服务机器人产量累计值(套)"}
    code_dict = {"A02092204_yd": "工业机器人产量累计增长(%)", "A02092U04_yd": "服务机器人产量累计增长(%)"}
    x = comm_cn_industry_metric_sort(code_dict)


def handle_ts_code(df: pd.DataFrame):
    code_dict_data = {}
    for index in df.index:
        dict_data = dict(df.loc[index])
        ts_code = dict_data['ts_code']
        code, sec = ts_code.split(".")
        sec = str(sec)
        sec = sec.lower()
        if sec not in ['sh', 'sz']:
            continue
        name = dict_data['name']
        key = f"{sec}{code}"
        code_dict_data[key] = name
    return code_dict_data


def handle_fin_rank():
    database = 'stock'
    collection = 'ticker_info'
    projection = {'_id': False}
    condition = None
    sort_key = 'time'
    data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                               sort_key=sort_key)
    is_local = True
    quarter = 3
    is_show = False

    # 减速器
    ticker_names = ['昊志机电', '中大力德', '巨轮智能', '双环传动', '绿的谐波']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))

    # 伺服系统
    ticker_names = ['汇川技术', '新时达', '步科股份', '伟创电气', '奥普光电', '峰岹科技']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))

    # 控制器
    ticker_names = ['埃夫特', '埃斯顿']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))

    # 空心杯电机
    ticker_names = ['江苏雷利']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))

    # 丝杠
    ticker_names = ['鼎智科技', '贝斯特', '秦川机床', '恒立液压']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]:v for k,v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]:v for k,v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict,by='value'))

    # 轴承
    ticker_names = ['国机精工', '苏轴股份', '五洲新春']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))

    # 传感器
    ticker_names = ['柯力传感']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))
    # 机器视觉
    ticker_names = ['奥比中光']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))
    # 机器人本体
    ticker_names = ['巨能股份', '恒为科技', '博实股份']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))
    # 其他
    ticker_names = ['三花智控', '鸣志电器', '拓普集团']
    filter_data = data[data['name'].isin(ticker_names)]
    new_code_dict = handle_ts_code(filter_data)
    ret_data = analysis_fin_by_metric(new_code_dict, isLocal=is_local, quarter=quarter, is_show=is_show)
    if ret_data is not None:
        show_data(ret_data)
        new_code_dict = {k[2:]: v for k, v in new_code_dict.items()}
        ret_dict = {new_code_dict[k]: v for k, v in dict(ret_data.tail(1).iloc[0]).items()}
        print(sort_dict_data_by(ret_dict, by='value'))


if __name__ == '__main__':
    handle_fin_rank()
