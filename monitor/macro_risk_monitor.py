from risk_manager.industry_risk import cn_industry_metric_risk
from risk_manager.macro_risk import cn_fin_risk, cn_electric_risk, cn_pmi_risk, cn_global_wci_risk, cn_traffic_risk, \
    cn_board_risk
from risk_manager.gpr import global_risk
from datetime import datetime
from utils.send_msg import MailSender
from monitor.comm_mail_utils import macro_risk_construct_mail_str


def cn_month_industry_metric_mail_str(day=None):
    risk_df = cn_industry_metric_risk()
    if day is None:
        month = datetime.now().month - 1
        if month == 12:
            year = datetime.now().year - 1
        else:
            year = datetime.now().year
        if month < 10:
            day = f"{year}0{month}"
        else:
            day = f"{year}{month}"
    if len(risk_df) > 0:
        html_str = f"<p>中国工业指标风险如下</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr> <th>风险</th> <th>时间</th></tr>"
        for index in risk_df.tail(6).index:
            risk = risk_df.loc[index]['risk']
            html_str += f"<tr><td>{risk}</td><td>{index}</td></tr>"
            if index == day:
                break
        html_str += "</table>"

        return html_str
    else:
        return None


def cn_month_fin_risk_mail_str(day=None):
    last_risk, last_datas, df = cn_fin_risk()
    if day is None:
        month = datetime.now().month - 1
        if month == 12:
            year = datetime.now().year - 1
        else:
            year = datetime.now().year
        if month < 10:
            day = f"{year}0{month}01"
        else:
            day = f"{year}{month}01"
    cols = ['afre', 'rmb_loans', 'gov_bonds', 'total_risk']

    if len(df) > 0:
        html_str = f"<p>中国社融风险如下</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr> <th>社融增量同比</th> <th>人民币贷款同比</th> <th>政府债券同比</th> <th>总风险</th> <th>时间</th></tr>"
        for index in df.tail(6).index:
            html_str += "<tr>"
            for col in cols:
                val = round(df.loc[index][col], 4)
                html_str += f"<td>{val}</td>"

            html_str += f"<td>{index}</td> </tr>"
            if index == day:
                break
        html_str += "</table>"
        return html_str
    else:
        return None


def cn_month_electric_risk_mail_str(day=None):
    last_risk, last_datas, df = cn_electric_risk()
    if day is None:
        month = datetime.now().month - 1
        if month == 12:
            year = datetime.now().year - 1
        else:
            year = datetime.now().year
        if month < 10:
            day = f"{year}0{month}"
        else:
            day = f"{year}{month}"
    cols = ['A03010G04_yd', 'total_risk']

    if len(df) > 0:
        html_str = f"<p>中国发电量风险如下</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr> <th>发电量同比</th><th>总风险</th> <th>时间</th></tr>"
        for index in df.tail(6).index:
            html_str += "<tr>"
            for col in cols:
                val = round(df.loc[index][col], 4)
                html_str += f"<td>{val}</td>"

            html_str += f"<td>{index}</td> </tr>"
            if index == day:
                break
        html_str += "</table>"
        return html_str
    else:
        return None


def cn_month_pmi_risk_mail_str(day=None):
    last_risk, df = cn_pmi_risk()
    if day is None:
        month = datetime.now().month - 1
        if month == 12:
            year = datetime.now().year - 1
        else:
            year = datetime.now().year
        if month < 10:
            day = f"{year}0{month}"
        else:
            day = f"{year}{month}"
    cols = ['制造业采购经理指数(%)', '生产指数(%)', '新订单指数(%)', '新出口订单指数(%)',
            '综合PMI产出指数(%)', 'total_risk']

    if len(df) > 0:
        html_str = f"<p>中国制造业PMI风险如下</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr> <th>制造业采购经理指数</th> <th>生产指数</th> <th>新订单指数</th> <th>新出口订单指数</th> <th>综合PMI产出指数</th> <th>总风险</th> <th>时间</th></tr>"
        for index in df.tail(6).index:
            html_str += "<tr>"
            for col in cols:
                val = round(df.loc[index][col], 4)
                html_str += f"<td>{val}</td>"

            html_str += f"<td>{index}</td> </tr>"
            if index == day:
                break
        html_str += "</table>"
        return html_str
    else:
        return None


def cn_month_global_wci_risk_mail_str(day=None):
    last_risk, last_data, df = cn_global_wci_risk()
    if day is None:
        month = datetime.now().month - 1
        if month == 12:
            year = datetime.now().year - 1
        else:
            year = datetime.now().year
        if month < 10:
            day = f"{year}0{month}01"
        else:
            day = f"{year}{month}01"
    cols = ["综合指数", "欧洲航线", "美西航线", "地中海航线", "美东航线", "波红航线", "澳新航线", "西非航线",
            "南非航线", "南美航线", "东南亚航线", "日本航线", "韩国航线", 'total_risk'
            ]

    if len(df) > 0:
        html_str = f"<p>中国海外航线指数风险</p>"
        html_str += f"<table border=\"1\">"
        html_str += "<tr>"
        for col in cols:
            html_str += f"<th>{col}</th>"
        html_str += "<th>时间</th> </tr>"
        for index in df.tail(6).index:
            html_str += "<tr>"
            for col in cols:
                val = round(df.loc[index][col], 4)
                html_str += f"<td>{val}</td>"
            html_str += f"<td>{index}</td> </tr>"
            if index == day:
                break
        html_str += "</table>"
        return html_str
    else:
        return None


def cn_moth_board_risk_mail_str(day=None):
    _, _, df = cn_board_risk()
    traffic_mapping_dict = {
        "acc_export_import_amount_cyc": "累计进出口同比",
        "acc_export_amount_cyc": "累计出口同比",
        "acc_import_amount_cyc": "累计进口同比",
        "total_risk": "总风险",
        "index": "时间",
    }
    if day is None:
        day = datetime.now().strftime("%Y%m%d")
    html_str = macro_risk_construct_mail_str(traffic_mapping_dict, df, '中国进出口风险监控', day)
    return html_str


def cn_week_traffic_risk_mail_str(day=None):
    _, _, df = cn_traffic_risk()
    traffic_mapping_dict = {
        "tl_traffic": "铁路运输(万吨)",
        "gk_traffic": "港口吞吐量(万吨)",
        "gk_teu_traffic": "港口集装箱吞吐量(万标箱)",
        "gs_traffic": "货车通行(万辆)",
        "lj_traffic": "邮政揽件(亿件)",
        "td_traffic": "邮政投递(亿件)",
        "total_risk": "总风险",
        "index": "时间",
    }
    if day is None:
        day = datetime.now().strftime("%Y%m%d")
    html_str = macro_risk_construct_mail_str(traffic_mapping_dict, df, '中国运输风险', day)
    return html_str

def global_gpr_mail_str(day=None):
    mapping_dict = {"risk":"风险",
                    "no_risk":"战争结束可能",
                    "index":"时间"}

    df = global_risk()
    if day is None:
        day = datetime.now().strftime("%Y%m%d")
    html_str = macro_risk_construct_mail_str(mapping_dict, df, '地缘风险', day)
    return html_str
def mail_sender(html_str):

    sender = MailSender()
    if html_str != '':
        print("发送数据")
        sender.send_html_data(['905198301@qq.com'], ['2394023336@qq.com'], "宏观监控",
                              html_str)
        sender.close()


def month_monitor_main(arg_fn_dict_data: dict = None):
    fn_mapping_dict = {
        "cn_industry_metric": cn_month_industry_metric_mail_str,
        "cn_fin_risk": cn_month_fin_risk_mail_str,
        "cn_electric_risk": cn_month_electric_risk_mail_str,
        "cn_pmi_risk": cn_month_pmi_risk_mail_str,
        "cn_global_wci_risk": cn_month_global_wci_risk_mail_str,
        "cn_board_risk": cn_moth_board_risk_mail_str,
    }

    if arg_fn_dict_data is None:
        month = datetime.now().month - 1
        if month == 12:
            year = datetime.now().year - 1
        else:
            year = datetime.now().year
        if month < 10:
            day = f"{year}0{month}"
        else:
            day = f"{year}{month}"
        other_day = f"{year}-{month}-01"
        arg_fn_dict_data = {
            "cn_industry_metric": day,
            "cn_fin_risk": day + "01",
            "cn_electric_risk": day,
            "cn_pmi_risk": day,
            "cn_global_wci_risk": day + "01",
            "cn_board_risk": other_day,

        }
    html_str = ""
    for k, day in arg_fn_dict_data.items():
        html_str += fn_mapping_dict.get(k)(day)
    if html_str != '':
        mail_sender(html_str)


def week_monitor_main(arg_fn_dict: str = None):
    fn_mapping_dict = {
        "cn_traffic_risk": cn_week_traffic_risk_mail_str,
    }

    if arg_fn_dict is None:
        day = datetime.now().strftime("%Y%m%d")
        arg_fn_dict = {
            "cn_traffic_risk": day,
        }
    html_str = ""
    for k, day in arg_fn_dict.items():
        html_str += fn_mapping_dict.get(k)(day)
    if html_str != '':
        mail_sender(html_str)

def day_monitor_main(arg_fn_dict: str = None):
    fn_mapping_dict = {
        "gpr": global_gpr_mail_str,
    }

    if arg_fn_dict is None:
        day = datetime.now().strftime("%Y%m%d")
        arg_fn_dict = {
            "gpr": day,
        }
    html_str = ""
    for k, day in arg_fn_dict.items():
        html_str += fn_mapping_dict.get(k)(day)
    if html_str != '':
        mail_sender(html_str)

if __name__ == '__main__':
    month_monitor_main()
