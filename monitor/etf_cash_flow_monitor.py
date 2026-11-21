"""
宽基 ETF 资金流监控（估算净申购）

监控对象：
    沪深300ETF(510300)、上证50ETF(510050)、中证500ETF(510500)

数据来源：
    MongoDB stock.etf_daily，字段 est_net_subscribe 为当日估算净申购（元），
    由 data/etf_daily_data.py 计算并写入。

判定规则（三只合计净申购）：
    合计 >= 200 亿元   -> 强买入信号
    合计 >= 50 亿元    -> 中等
    合计 <= -50 亿元   -> 净赎回
    其余情况           -> 无信号，直接播报合计金额

通知方式：
    monitor.dingtalk_msg.DingtalkSendMsg.send_msg(type='monitor_eft_cash_flow')
"""

import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from data.mongodb import get_mongo_table
from monitor.dingtalk_msg import DingtalkSendMsg

# 监控的宽基 ETF，key 为代码，value 为展示名称
BROAD_ETF_NAMES = {
    "510300": "沪深300ETF",
    "510050": "上证50ETF",
    "510500": "中证500ETF",
}

YI = 100000000  # 1 亿元

STRONG_BUY_THRESHOLD = 200 * YI   # 强买入信号阈值
MEDIUM_THRESHOLD = 50 * YI        # 中等信号阈值
REDEEM_THRESHOLD = -50 * YI       # 净赎回阈值

SIGNAL_STRONG_BUY = "强买入信号"
SIGNAL_MEDIUM = "中等"
SIGNAL_REDEEM = "净赎回"


def get_latest_etf_cash_flow(date=None):
    """
    获取宽基 ETF 的估算净申购数据。

    :param date: 指定日期 YYYY-MM-DD；为空时取库中最新交易日
    :return: (date, [{code, name, est_net_subscribe}])
    """
    etf_daily = get_mongo_table(collection="etf_daily")

    if date is None:
        latest = etf_daily.find_one(sort=[("time", -1)])
        if latest is None:
            return None, []
        date = latest["time"]

    rows = list(
        etf_daily.find(
            {"time": date, "code": {"$in": list(BROAD_ETF_NAMES.keys())}},
            {"_id": False, "code": 1, "name": 1, "est_net_subscribe": 1},
        ).sort("code", 1)
    )
    return date, rows


def judge_cash_flow(total_amount):
    """
    按合计估算净申购金额判定信号。

    :param total_amount: 合计金额（元）
    :return: 信号名称；不满足任何条件时返回 None
    """
    if total_amount >= STRONG_BUY_THRESHOLD:
        return SIGNAL_STRONG_BUY
    if total_amount >= MEDIUM_THRESHOLD:
        return SIGNAL_MEDIUM
    if total_amount <= REDEEM_THRESHOLD:
        return SIGNAL_REDEEM
    return None


def build_etf_cash_flow_msg(date, rows):
    """
    组织钉钉 markdown 消息内容。

    :return: (msg_text, total_amount, signal)
    """
    detail_lines = []
    total_amount = 0.0
    missing_codes = []

    row_map = {row["code"]: row for row in rows}
    for code, name in BROAD_ETF_NAMES.items():
        row = row_map.get(code)
        amount = row.get("est_net_subscribe") if row else None
        if amount is None:
            missing_codes.append(name)
            detail_lines.append(f"> {name}({code})：数据缺失")
            continue
        total_amount += float(amount)
        detail_lines.append(f"> {name}({code})：{amount / YI:.2f} 亿元")

    signal = judge_cash_flow(total_amount)
    total_yi = total_amount / YI

    detail_lines.append(f"> **三只宽基合计估算净申购：{total_yi:.2f} 亿元**")
    if signal is None:
        detail_lines.append(f"> 判定：无信号，合计申购 {total_yi:.2f} 亿元")
    else:
        detail_lines.append(f"> 判定：**{signal}**")

    if missing_codes:
        detail_lines.append(f"> 注意：{'、'.join(missing_codes)} 当日数据缺失，合计未包含该标的")

    msg_text = "\n".join(detail_lines)
    return msg_text, total_amount, signal


def etf_cash_flow_monitor(date=None, is_send=True):
    """
    宽基 ETF 资金流监控入口。

    :param date: 指定日期 YYYY-MM-DD；为空时取库中最新交易日
    :param is_send: 是否发送钉钉通知，False 时仅打印结果
    :return: 监控结果 dict，无数据时返回 None
    """
    date, rows = get_latest_etf_cash_flow(date=date)
    if date is None or not rows:
        print("未找到宽基 ETF 资金流数据，请先执行 data/etf_daily_data.py")
        return None

    msg_text, total_amount, signal = build_etf_cash_flow_msg(date, rows)

    result = {
        "time": date,
        "total_amount": total_amount,
        "total_yi": total_amount / YI,
        "signal": signal,
        "msg_text": msg_text,
    }
    print(f"\n宽基ETF资金流监控 {date}")
    print(msg_text)

    if is_send:
        msg_title = f"宽基ETF资金流监控 {date}" + (f" - {signal}" if signal else "")
        send_result = DingtalkSendMsg().send_msg(
            type="monitor_eft_cash_flow",
            msg_title=msg_title,
            msg_text=msg_text,
        )
        result["send_result"] = send_result
        print(f"钉钉发送结果：{send_result}")

    return result


if __name__ == '__main__':
    etf_cash_flow_monitor()
