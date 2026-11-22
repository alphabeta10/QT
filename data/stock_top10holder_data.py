"""
按季度抓取 A 股前十大股东数据并写入 MongoDB。

数据源：腾讯自选股 stock_quote_history，每只股票一次请求返回整个日期范围
报告期：每年 03-31、06-30、09-30、12-31
存储位置：stock.top10holder
唯一键：(code, date, 名次)，已入库的股票报告期自动跳过。
"""

import json
import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

import requests
from pymongo import UpdateOne
from tqdm import tqdm

from data.mongodb import get_mongo_table
from data.stock_detail_fin import get_stock_info_data
from utils.actions import try_get_action
from utils.tool import mongo_bulk_write_data

QUARTER_ENDS = ((3, 31), (6, 30), (9, 30), (12, 31))
QQ_API_URL = (
    "https://proxy.finance.qq.com/cgi/cgi-bin/openai/openclaw/proxy"
    "?app=openclaw"
    "&token=99359fcc033b30b5f33a5c825ad9de81fd66a6337781834040af835a2099a553"
    "&dev_id=dev_b1ea22fe6d8deeb8391b398788fa4bd2"
)
QQ_TOKEN = "99359fcc033b30b5f33a5c825ad9de81fd66a6337781834040af835a2099a553"
QQ_SESSION = requests.Session()
QQ_SESSION.headers.update({"User-Agent": "Mozilla/5.0"})


def get_quarter_end_dates(start_date, end_date):
    """
    获取起止日期范围内的季度末报告期。

    参数支持 YYYYMMDD 或 YYYY-MM-DD；起止日期不必恰好是季度末，
    只返回落在闭区间内的 0331、0630、0930、1231。
    """
    start = datetime.strptime(start_date.replace("-", ""), "%Y%m%d")
    end = datetime.strptime(end_date.replace("-", ""), "%Y%m%d")
    if start > end:
        raise ValueError("start_date 不能晚于 end_date")

    dates = []
    for year in range(start.year, end.year + 1):
        for month, day in QUARTER_ENDS:
            quarter_end = datetime(year, month, day)
            if start <= quarter_end <= end:
                dates.append(quarter_end)

    if not dates:
        raise ValueError("指定时间范围内没有季度末报告期")
    return [date.strftime("%Y%m%d") for date in dates]


def get_qq_top10holder(symbol, start_date, end_date):
    """从腾讯自选股获取日期范围内的历史十大股东，按报告期返回。"""
    payload = {
        "token": QQ_TOKEN,
        "route": "stock_quote_history",
        "params": {
            "code": symbol,
            "fields": "Top10Shareholder",
            "start_date": datetime.strptime(start_date, "%Y%m%d").strftime("%Y-%m-%d"),
            "end_date": datetime.strptime(end_date, "%Y%m%d").strftime("%Y-%m-%d"),
        },
        "skill_name": "westock-data",
        "skill_channel": "skillhub",
    }
    response = QQ_SESSION.post(QQ_API_URL, json=payload, timeout=30)
    response.raise_for_status()
    result = response.json()
    if result.get("code") != 0:
        raise RuntimeError(f"腾讯十大股东接口返回错误: {result}")

    holder_map = {}
    for item in result.get("data", {}).get("series", []):
        data = item.get("data", {})
        date = item.get("date") or data.get("EndDate")
        holders = json.loads(data.get("Top10Shareholder") or "[]")
        rows = []
        for holder in holders:
            hold_sum = float(holder["HoldSum"])
            raw_change = holder.get("HoldSumChange")
            if raw_change in (None, ""):
                change_text = "新进"
                change_ratio = None
            else:
                change = float(raw_change)
                if change == 0:
                    change_text = "不变"
                    change_ratio = None
                else:
                    change_text = str(int(change)) if change.is_integer() else str(change)
                    previous_hold_sum = hold_sum - change
                    change_ratio = (
                        round(change / previous_hold_sum * 100, 8)
                        if previous_hold_sum
                        else None
                    )

            rows.append({
                "名次": int(holder["SHNO"]),
                "股东名称": holder["SHList"],
                # 腾讯该接口不提供流通/限售细分，仅标识为 A 股
                "股份类型": "A股",
                "持股数": int(hold_sum),
                "占总股本持股比例": float(holder["PCTOfTotalShares"]),
                "增减": change_text,
                "变动比率": change_ratio,
            })
        holder_map[date] = rows
    return holder_map


def top10holder_data_to_mongo(start_date, end_date, codes=None):
    """
    抓取指定时间范围内全部季度末的前十大股东数据。

    参数:
        start_date: 开始日期，YYYYMMDD 或 YYYY-MM-DD
        end_date: 结束日期，YYYYMMDD 或 YYYY-MM-DD
        codes: 带交易所前缀的股票代码列表，如 ['sh688686']；
               为空时读取 ticker_info 中全部沪深股票

    存储字段:
        code: 6 位股票代码
        date: 报告期，YYYY-MM-DD
        名次/股东名称/股份类型/持股数/占总股本持股比例/增减/变动比率
    """
    quarter_dates = get_quarter_end_dates(start_date, end_date)
    if codes is None:
        codes = get_stock_info_data()

    top10holder = get_mongo_table(database="stock", collection="top10holder")

    # 每个报告期只查询一次已有股票集合，避免重复请求已入库的数据
    db_date_map = {
        date: datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")
        for date in quarter_dates
    }
    existing_codes = {
        date: set(top10holder.distinct("code", {"date": db_date_map[date]}))
        for date in quarter_dates
    }

    request_update = []
    total_count = 0
    for symbol in tqdm(codes, desc="前十大股东"):
        symbol = symbol.lower()
        if not symbol.startswith(("sh", "sz", "bj")):
            continue
        code = symbol[2:]
        missing_dates = [
            date for date in quarter_dates if code not in existing_codes[date]
        ]
        if not missing_dates:
            continue

        # 每只股票只请求一次，避免按报告期重复调用外部接口
        holder_map = try_get_action(
            get_qq_top10holder,
            try_count=2,
            delay=2,
            symbol=symbol,
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
        )
        if holder_map is None:
            continue

        for date in missing_dates:
            rows = holder_map.get(db_date_map[date], [])
            for dict_data in rows:
                dict_data = dict(dict_data)
                dict_data["code"] = code
                dict_data["date"] = db_date_map[date]
                request_update.append(
                    UpdateOne(
                        {"code": code, "date": dict_data["date"], "名次": dict_data["名次"]},
                        {"$set": dict_data},
                        upsert=True,
                    )
                )
                total_count += 1

            if rows:
                existing_codes[date].add(code)
            if len(request_update) >= 500:
                mongo_bulk_write_data(top10holder, request_update)
                request_update.clear()

    if request_update:
        mongo_bulk_write_data(top10holder, request_update)

    print(f"处理完成，共同步 {total_count} 条前十大股东记录")
    return total_count


def create_top10holder_index():
    """创建唯一索引，不删除集合中的历史数据。"""
    top10holder = get_mongo_table(database="stock", collection="top10holder")
    top10holder.create_index(
        [("code", 1), ("date", 1), ("名次", 1)],
        unique=True,
        background=True,
    )


if __name__ == "__main__":
    top10holder_data_to_mongo(
        start_date="20260101",
        end_date="20261231"
    )
