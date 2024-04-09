import pandas as pd
from utils.actions import show_data
from data.mongodb import get_mongo_table
from utils.send_msg import MailSender
from utils.tool import load_json_data

def monitor_goods_price_change(goods_name_list=None):
    goods = get_mongo_table(database='stock', collection='goods')
    datas = []
    if goods_name_list is None:
        goods_name_list = ['铜', '铝', '锡','氧化镝','金属镝','镨钕氧化物']
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
    html_str += "<tr> <th>名称</th> <th>1日变化率</th> <th>7日变化率</th> <th>14日变化率</th> <th>30日变化率</th> <th>涨跌统计</th></tr>"
    for name in goods_name_list:
        detail_goods_inflation_risk_dict[name] = {"up": 0, "down": 0}
        html_str += "<tr>"
        html_str += f"<td>{name}</td>"
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
    for _,combine_dict in detail_goods_inflation_risk_dict.items():
        inflation_risk += combine_dict['up']/len(pct_change_list)
        deflation_risk += combine_dict['down']/len(pct_change_list)
    avg_inflation_risk = round(inflation_risk/len(list(detail_goods_inflation_risk_dict.keys())),4)
    avg_deflation_risk =  round(deflation_risk/len(list(detail_goods_inflation_risk_dict.keys())),4)
    html_str += "</table>"
    html_str += f"<p> 通胀风险:{avg_inflation_risk};通缩风险:{avg_deflation_risk}"

    sender = MailSender()
    if html_str != '':
        print("发送数据")
        sender.send_html_data(['905198301@qq.com','2367243209@qq.com'], ['2394023336@qq.com'], "监控商品最新消息", html_str)
        sender.close()


if __name__ == '__main__':
    monitor_goods_price_change()
