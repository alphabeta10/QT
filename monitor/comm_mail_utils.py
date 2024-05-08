import pandas as pd


def macro_risk_construct_mail_str(col_dict: dict, df: pd.DataFrame, title: str, day: str, num=6):
    """
    宏观风险构建发送邮件
    :param col_dict:
    :param df:
    :param title:
    :param day:
    :return:
    """
    if len(df) > 0:
        html_str = f"<p>{title}</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr>"
        for k, v in col_dict.items():
            html_str += f"<th>{v}</th>"
        html_str += "</tr>"
        for index in df.tail(num).index:
            html_str += "<tr>"
            dict_data = dict(df.loc[index])
            for col in col_dict.keys():
                if col not in dict_data.keys() and col=='index':
                    val = index
                else:
                    val = round(dict_data[col],4)
                html_str += f"<td>{val}</td>"
            html_str += "</tr>"
            if index == day:
                break
        html_str += "</table>"
        return html_str

def comm_construct_mail_str(col_dict: dict, df: pd.DataFrame, title: str, day: str, num=6,is_tail=True,format_cols=None):
    """
    宏观风险构建发送邮件
    :param col_dict:
    :param df:
    :param title:
    :param day:
    :return:
    """
    if len(df) > 0:
        if format_cols is None:
            format_cols = []
        html_str = f"<p>{title}</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr>"
        for k, v in col_dict.items():
            html_str += f"<th>{v}</th>"
        html_str += "</tr>"
        if is_tail:
            send_data = df.tail(num)
        else:
            send_data = df.head(num)
        for index in send_data.index:
            html_str += "<tr>"
            dict_data = dict(df.loc[index])
            for col in col_dict.keys():
                if col not in dict_data.keys() and col=='index':
                    val = index
                else:
                    val = dict_data[col]
                if col in format_cols:
                    val = round(float(val),4)
                else:
                    val = str(val)
                html_str += f"<td>{val}</td>"
            html_str += "</tr>"
            if index == day:
                break
        html_str += "</table>"
        return html_str
    return ''