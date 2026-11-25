"""
宽基 ETF 日线行情 + 估算净申购数据抓取并写入 MongoDB

覆盖标的：
    510300 沪深300ETF
    510050 上证50ETF
    510500 中证500ETF

数据源：
    行情：akshare.fund_etf_hist_sina，新浪财经 ETF 日线，含 OHLC、成交量、成交额
    份额：akshare.fund_etf_scale_sse，上交所每日 ETF 基金份额（三只 ETF 均在上交所上市）

估算净申购：
    share_change = 今日基金份额 - 上一交易日基金份额
    est_net_subscribe = share_change * 当日收盘价
    单位为元；正数表示估算净申购，负数表示估算净赎回。
    由于使用收盘价而非 IOPV/净值，属于估算值。

MongoDB：
    数据库 stock，集合 etf_daily，业务唯一键 (code, time)，重复执行自动覆盖更新。
"""

import os
import sys
import time
from datetime import datetime, timedelta

# 可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

import akshare as ak
from pymongo import UpdateOne
from tqdm import tqdm

from data.mongodb import get_mongo_table
from utils.actions import try_get_action
from utils.tool import mongo_bulk_write_data

# 宽基 ETF 基础信息：key 为 6 位代码，sina_symbol 为新浪接口使用的代码
ETF_INFO = {
    "510300": {"name": "沪深300ETF", "sina_symbol": "sh510300"},
    "510050": {"name": "上证50ETF", "sina_symbol": "sh510050"},
    "510500": {"name": "中证500ETF", "sina_symbol": "sh510500"},
}


def get_trade_calendar():
    """
    获取完整 A 股交易日历。

    返回:
        list[str]: 交易日列表，格式 YYYY-MM-DD，升序。
    """
    calendar_df = ak.calendar_trade_date_hist_sina()
    trade_dates = [str(x)[:10] for x in calendar_df["trade_date"]]
    return sorted(trade_dates)


def get_trade_dates(start_date, end_date, calendar=None):
    """
    取指定闭区间内的交易日。

    参数:
        start_date: YYYYMMDD
        end_date: YYYYMMDD
        calendar: 可选，完整交易日历，避免重复请求
    """
    if calendar is None:
        calendar = get_trade_calendar()
    start_fmt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_fmt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")
    return [day for day in calendar if start_fmt <= day <= end_fmt]


def fetch_etf_share_data(trade_dates, codes):
    """
    按交易日获取上交所 ETF 基金份额。

    参数:
        trade_dates: 交易日列表，格式 YYYY-MM-DD
        codes: ETF 代码列表

    返回:
        dict: {code: {day: 基金份额(份)}}
    """
    share_data = {code: {} for code in codes}
    for day in tqdm(trade_dates, desc="获取ETF份额"):
        date_str = day.replace("-", "")
        scale_df = try_get_action(
            ak.fund_etf_scale_sse, try_count=2, delay=2, date=date_str
        )
        if scale_df is None or scale_df.empty:
            print(f"{day} 未获取到 ETF 份额数据")
            continue

        # 只保留目标宽基 ETF，降低后续处理量
        scale_df = scale_df[scale_df["基金代码"].isin(codes)]
        for _, row in scale_df.iterrows():
            code = str(row["基金代码"])
            share_data[code][day] = float(row["基金份额"])

        # 轻微限速，避免频繁请求上交所接口
        time.sleep(0.2)
    return share_data


def handle_etf_daily_data(codes=None, start_date=None,
                          end_date=datetime.now().strftime("%Y%m%d")):
    """
    抓取宽基 ETF 日线行情、计算估算净申购并写入 MongoDB。

    参数:
        codes: ETF 代码列表，默认三只宽基 ETF
        start_date: 开始日期 YYYYMMDD，默认最近 5 个自然日
        end_date: 结束日期 YYYYMMDD，默认当天

    写入字段：
        time/code/name/open/high/low/close/volume/amount
        fund_share: 当日基金份额（份）
        share_change: 较上一交易日份额变动（份）
        est_net_subscribe: 估算净申购金额（元）
    """
    if codes is None:
        codes = list(ETF_INFO.keys())
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=5)).strftime("%Y%m%d")

    print(f"ETF日线处理 start={start_date}, end={end_date}, codes={codes}")

    # 交易日历用于过滤非交易日，同时定位每个交易日的上一交易日
    calendar = get_trade_calendar()
    trade_dates = get_trade_dates(start_date, end_date, calendar=calendar)
    if not trade_dates:
        print("指定区间内没有交易日，无需处理")
        return

    # 为计算 start_date 当天的份额变动，需要额外获取前一交易日份额
    first_day_index = calendar.index(trade_dates[0])
    scale_dates = list(trade_dates)
    if first_day_index > 0:
        scale_dates.insert(0, calendar[first_day_index - 1])

    # 上一交易日映射：{当日: 前一交易日}
    prev_trade_day = {
        day: calendar[calendar.index(day) - 1] if calendar.index(day) > 0 else None
        for day in trade_dates
    }

    share_data = fetch_etf_share_data(scale_dates, codes)

    etf_daily = get_mongo_table(collection="etf_daily")
    start_fmt = datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d")
    end_fmt = datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d")

    update_request = []
    for code in codes:
        info = ETF_INFO.get(code, {"name": code, "sina_symbol": f"sh{code}"})

        # 新浪接口返回全历史日线，这里再按日期区间过滤
        hist_df = try_get_action(
            ak.fund_etf_hist_sina, try_count=2, delay=2,
            symbol=info["sina_symbol"]
        )
        if hist_df is None or hist_df.empty:
            print(f"{code} 未获取到日线行情")
            continue

        hist_df["date"] = hist_df["date"].astype(str).str[:10]
        hist_df = hist_df[
            (hist_df["date"] >= start_fmt) & (hist_df["date"] <= end_fmt)
        ]

        for _, row in hist_df.iterrows():
            day = str(row["date"])
            close = float(row["close"])

            # 当日份额与前一交易日份额；任一侧缺失则净申购字段置空
            fund_share = share_data.get(code, {}).get(day)
            prev_day = prev_trade_day.get(day)
            prev_share = (
                share_data.get(code, {}).get(prev_day) if prev_day else None
            )

            if fund_share is not None and prev_share is not None:
                share_change = fund_share - prev_share
                est_net_subscribe = share_change * close
            else:
                share_change = None
                est_net_subscribe = None

            dict_data = {
                "time": day,
                "code": code,
                "name": info["name"],
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": close,
                "volume": int(row["volume"]),          # 成交量，单位：份
                "amount": float(row["amount"]),        # 成交额，单位：元
                "fund_share": fund_share,              # 基金份额，单位：份
                "share_change": share_change,          # 份额变动，单位：份
                "est_net_subscribe": est_net_subscribe # 估算净申购，单位：元
            }
            update_request.append(
                UpdateOne(
                    {"code": code, "time": day},
                    {"$set": dict_data},
                    upsert=True
                )
            )

        if len(update_request) > 500:
            mongo_bulk_write_data(etf_daily, update_request)
            update_request.clear()

    if len(update_request) > 0:
        mongo_bulk_write_data(etf_daily, update_request)
        update_request.clear()


def col_create_index():
    """为 etf_daily 建立 (code, time) 唯一索引，不删除历史数据。"""
    etf_daily = get_mongo_table(collection="etf_daily")
    etf_daily.create_index(
        [("code", 1), ("time", 1)], unique=True, background=True
    )


if __name__ == '__main__':
    handle_etf_daily_data()
