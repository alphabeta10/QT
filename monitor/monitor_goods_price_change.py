import pandas as pd
from data.mongodb import get_mongo_table
from utils.send_msg import MailSender


def comm_construct_html_data(goods_name_list: list = None, goods_type=None):
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    if goods_type is None:
        goods_type = '监控商品最新消息如下'
    if goods_name_list is None:
        goods_name_list = ['铜', '铝', '锡', '氧化镝', '金属镝', '镨钕氧化物', '黄金', '白银', 'WTI原油', 'Brent原油']
    goods_condition = {"$in": goods_name_list}
    for ele in goods.find({"name": goods_condition, "data_type": "goods_price"}, projection={'_id': False}).sort(
            "time"):
        time = ele['time']
        value = ele['value']
        name = ele['name']
        datas.append({"time": time, "value": value, "name": name})
    pd_data = pd.DataFrame(datas)
    pd_data[['value']] = pd_data[['value']].astype(float)
    data = pd.pivot_table(pd_data, values='value', columns='name', index='time')
    pct_change_list = [1, 7, 14, 30, 60, 120, 240]
    for name in goods_name_list:
        for num in pct_change_list:
            data[f'{name}_pct_{num}'] = data[name].pct_change(num)
    inflation_risk = 0
    deflation_risk = 0
    detail_goods_inflation_risk_dict = {}
    last_dict_data = dict(data.tail(1).iloc[0])
    last_time = str(data.tail(1).index.values[0])
    html_str = f"<p>{goods_type}</p>"
    html_str += f"<table border=\"1\">"
    day_ch_html = ""
    for day in pct_change_list:
        day_ch_html+=f"<th>{day}日变化率</th>"
    html_str += f"<tr> <th>名称</th> <th>价格</th> {day_ch_html}<th>涨跌统计</th> <th>时间</th> </tr>"
    for name in goods_name_list:
        detail_goods_inflation_risk_dict[name] = {"up": 0, "down": 0}
        html_str += "<tr>"
        price = last_dict_data[name]
        html_str += f"<td>{name}</td>"
        html_str += f"<td>{price}</td>"
        for num in pct_change_list:
            ele = round(last_dict_data[f'{name}_pct_{num}'], 4)
            if ele > 0:
                detail_goods_inflation_risk_dict[name]['up'] += 1
            else:
                detail_goods_inflation_risk_dict[name]['down'] += 1
            html_str += f"<td>{ele}</td>"
        up, down = detail_goods_inflation_risk_dict.get(name)['up'], detail_goods_inflation_risk_dict.get(name)['down']
        html_str += f"<td>上涨次数:{up};下跌次数:{down}</td>"
        html_str += f"<td>{last_time}</td>"
        html_str += "</tr>"
    for _, combine_dict in detail_goods_inflation_risk_dict.items():
        inflation_risk += combine_dict['up'] / len(pct_change_list)
        deflation_risk += combine_dict['down'] / len(pct_change_list)
    avg_inflation_risk = round(inflation_risk / len(list(detail_goods_inflation_risk_dict.keys())), 4)
    avg_deflation_risk = round(deflation_risk / len(list(detail_goods_inflation_risk_dict.keys())), 4)
    html_str += "</table>"
    html_str += f"<p> 通胀风险:{avg_inflation_risk};通缩风险:{avg_deflation_risk}"
    return html_str


def monitor_goods_price_change(goods_name_list=None):
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    if goods_name_list is None:
        goods_name_list = ['铜', '铝', '锡', '氧化镝', '金属镝', '镨钕氧化物', '黄金', '白银', 'WTI原油', 'Brent原油']
    goods_condition = {"$in": goods_name_list}
    for ele in goods.find({"name": goods_condition, "data_type": "goods_price"}, projection={'_id': False}).sort(
            "time"):
        time = ele['time']
        value = ele['value']
        name = ele['name']
        datas.append({"time": time, "value": value, "name": name})
    pd_data = pd.DataFrame(datas)
    pd_data[['value']] = pd_data[['value']].astype(float)
    data = pd.pivot_table(pd_data, values='value', columns='name', index='time')
    pct_change_list = [1, 7, 14, 30]
    for name in goods_name_list:
        for num in pct_change_list:
            data[f'{name}_pct_{num}'] = data[name].pct_change(num)
    inflation_risk = 0
    deflation_risk = 0
    detail_goods_inflation_risk_dict = {}
    last_dict_data = dict(data.tail(1).iloc[0])
    html_str = f"<p>监控商品最新消息如下</p>"
    html_str += f"<table border=\"1\">"
    html_str += "<tr> <th>名称</th> <th>价格</th> <th>1日变化率</th> <th>7日变化率</th> <th>14日变化率</th> <th>30日变化率</th> <th>涨跌统计</th></tr>"
    for name in goods_name_list:
        detail_goods_inflation_risk_dict[name] = {"up": 0, "down": 0}
        html_str += "<tr>"
        price = last_dict_data[name]
        html_str += f"<td>{name}</td>"
        html_str += f"<td>{price}</td>"
        for num in pct_change_list:
            ele = round(last_dict_data[f'{name}_pct_{num}'], 4)
            if ele > 0:
                detail_goods_inflation_risk_dict[name]['up'] += 1
            else:
                detail_goods_inflation_risk_dict[name]['down'] += 1
            html_str += f"<td>{ele}</td>"
        up, down = detail_goods_inflation_risk_dict.get(name)['up'], detail_goods_inflation_risk_dict.get(name)['down']
        html_str += f"<td>上涨次数:{up};下跌次数:{down}<td>"
        html_str += "</tr>"
    for _, combine_dict in detail_goods_inflation_risk_dict.items():
        inflation_risk += combine_dict['up'] / len(pct_change_list)
        deflation_risk += combine_dict['down'] / len(pct_change_list)
    avg_inflation_risk = round(inflation_risk / len(list(detail_goods_inflation_risk_dict.keys())), 4)
    avg_deflation_risk = round(deflation_risk / len(list(detail_goods_inflation_risk_dict.keys())), 4)
    html_str += "</table>"
    html_str += f"<p> 通胀风险:{avg_inflation_risk};通缩风险:{avg_deflation_risk}"

    sender = MailSender()
    if html_str != '':
        print("发送数据")
        sender.send_html_data(['905198301@qq.com', '2367243209@qq.com'], ['2394023336@qq.com'], "监控商品最新消息",
                              html_str)
        sender.close()


def daily_monitor_goods():
    goods_configs = [{"goods_type": "能源商品监控", "goods_list": ['WTI原油', 'Brent原油']},
                     {"goods_type": "避险商品监控", "goods_list": ['黄金', '白银']},
                     {"goods_type": "建材商品监控", "goods_list": ['螺纹钢', '玻璃']},
                     {"goods_type": "包装材料商品监控",
                      "goods_list": ['针叶木浆', '白卡纸', '白板纸', '阔叶木浆', '瓦楞原纸', '废纸']},
                     {"goods_type": "有色金属监控", "goods_list": ['铜', '铝', '锡', '氧化镝', '金属镝', '镨钕氧化物']},
                     ]
    all_html_str = ''
    for ele in goods_configs:
        goods_type = ele['goods_type']
        goods_list = ele['goods_list']
        htmlstr = comm_construct_html_data(goods_list, goods_type)
        if htmlstr != '':
            all_html_str += htmlstr
    if all_html_str != '':
        print("发送数据")
        sender = MailSender()
        sender.send_html_data(['905198301@qq.com', '2367243209@qq.com'], ['2394023336@qq.com'], "监控商品最新消息",
                              all_html_str)
        sender.close()


if __name__ == '__main__':
    daily_monitor_goods()
