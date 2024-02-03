import akshare as ak
from datetime import datetime, timedelta

from utils.send_msg import MailSender
from utils.actions import try_get_action


def construct_html_msg_send_to_user(send_list: list, sender, msg_title):
    if len(send_list) > 0:
        html_msg = "<p>事件列表如下，请留意！！！！</p>"
        html_msg += "<table>"
        html_msg += f"<tr> <th>日期</th> <th>地区</th> <th>事件</th> <th>前值</th>  <th>预期</th> <th>重要性</th> </tr>"
        for dict_data in send_list:
            day = str(dict_data['日期'])
            time = str(dict_data['时间'])
            country = dict_data['地区']
            event = dict_data['事件']
            pre_value = dict_data['前值']
            important = dict_data['重要性']
            predict_value = dict_data['预期']
            html_msg += f"<tr><td>{day} {time}</td> <td>{country}</td>  <td>{event}</td> <td>{pre_value}</td> <td>{predict_value}</td> <td>{important}</td></tr>"
        html_msg += "</table>"
        if html_msg != '':
            sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], msg_title, html_msg)


def sender_important_fin_data_to_user():
    # 周六或者周日跑数据
    date_format = "%Y%m%d"
    now_str = datetime.now().strftime(date_format)
    end_date_int = int((datetime.now() + timedelta(days=7)).strftime(date_format))
    weekday = datetime.now().weekday()
    important_countrys = ['中国',"美国"]
    important_keys = ['GDP', 'PMI', 'CPI', 'PPI', 'M0货币供应年率', 'M1货币供应年率', 'M2货币供应年率',
                      '人民币各项贷款余额年率', '新增人民币贷款', '社会融资规模']
    if weekday in [5, 6]:
        sender = MailSender()
        while int(now_str) < end_date_int:
            print(now_str)
            news_economic_baidu_df = try_get_action(ak.news_economic_baidu, try_count=3, date=now_str)
            now_str = (datetime.strptime(now_str, date_format) + timedelta(days=1)).strftime(date_format)
            send_list = []
            for index in news_economic_baidu_df.index:
                dict_data = dict(news_economic_baidu_df.loc[index])
                country = dict_data['地区']
                event = dict_data['事件']
                if country in important_countrys:
                    for key in important_keys:
                        if key in event:
                            send_list.append(dict_data)
            construct_html_msg_send_to_user(send_list, sender, f"{now_str}日重要的经济数据公布，请留意！！！！")


if __name__ == '__main__':
    sender_important_fin_data_to_user()
