import requests
import json


class DingtalkSendMsg(object):
    def __init__(self):
        self.__headers = {'Content-Type': 'application/json;charset=utf-8'}
        self.url = 'https://oapi.dingtalk.com/robot/send?access_token=b166ec191ac5b18c1228c5b8165125bbc629e7dbdf655e6629c73052a6c7cb15'

    def handle_ticker_trigger_msg(self, msg_dict_data_list: list, msg_title=None, comm_info_dict=None):

        if comm_info_dict is None:
            comm_info_dict = {"name": "名称", "close": "C", "open": "O", "high": "H", "low": "L", "pct_chg": "pct_chg"}
        send_msg = ""
        if msg_title:
            send_msg += f"#### {msg_title} \n"

        total_ticker = set()
        [total_ticker.add(ele['row_data']['name']) for ele in msg_dict_data_list]
        total_ticker_str = ",".join(list(total_ticker))
        send_msg += f"#### 触发标的如下：\n >{total_ticker_str} \n"
        for msg_dict_data in msg_dict_data_list:
            row_data = msg_dict_data['row_data']
            info_msg_list = []
            for info_key, info_name in comm_info_dict.items():
                info_val = row_data[info_key]
                info_msg = f"{info_name}={info_val}"
                info_msg_list.append(info_msg)
            html_info_msg = ",".join(info_msg_list)

            html_msg = f"#### {html_info_msg} \n"
            trigger_list = []
            for k, msg in msg_dict_data.items():
                if k not in ['row_data', 'other_show_indicator']:
                    indicator_val = round(row_data[k], 2)
                    trigger_list.append(msg + f" {k}={indicator_val}")
            trigger_msg = ",".join(trigger_list)
            html_msg += f" >{trigger_msg} \n"

            trigger_list = []
            if 'other_show_indicator' in msg_dict_data.keys():
                for key in msg_dict_data['other_show_indicator']:
                    indicator_val = round(row_data[key], 4)
                    trigger_list.append(f"{key}={indicator_val}")
                trigger_msg = ",".join(trigger_list)
                html_msg += f">{trigger_msg}\n"
            else:
                html_msg += f">无\n"

            send_msg += html_msg
        if len(msg_dict_data_list) > 0:
            json_text = {
                "msgtype": "markdown",
                "markdown": {
                    "title": "股票触发指标播报",
                    "text": f"{send_msg}"
                },
                "at": {
                    "isAtAll": False
                }
            }
            return json.dumps(json_text)
        return None

    def send_msg(self, **kwargs):
        if kwargs.get("type", '') == 'ticker_trigger_msg':
            data_list = kwargs.get("data_list", [])
            msg_title = kwargs.get("msg_title", '')
            comm_info_dict = kwargs.get('comm_info_dict', None)
            send_msg = self.handle_ticker_trigger_msg(data_list, msg_title, comm_info_dict)
            if send_msg:
                return requests.post(self.url, send_msg, headers=self.__headers).json()
        return None


if __name__ == '__main__':
    pass
