import cn_stock_holidays.data as shsz
from datetime import datetime


class AStockTradingCalendar:
    """A股交易日历与交易时段判断工具"""

    @staticmethod
    def is_trading_day(dt=None):
        if dt is None:
            dt = datetime.now().date()
        return shsz.is_trading_day(dt)

    @staticmethod
    def get_trading_status(dt=None):
        if dt is None:
            dt = datetime.now()

        if not AStockTradingCalendar.is_trading_day(dt):
            return "休市"

        t = dt.strftime("%H:%M")

        if t < "09:15":
            return "未开盘"
        elif t < "09:25":
            return "集合竞价"
        elif t < "09:30":
            return "等待开盘"
        elif t < "11:30":
            return "交易中"
        elif t < "13:00":
            return "午间休市"
        elif t < "14:57":
            return "交易中"
        elif t < "15:00":
            return "收盘集合竞价"
        elif t < "15:30":
            return "盘后交易"
        else:
            return "已收盘"

    @staticmethod
    def is_trading_time(dt=None):
        status = AStockTradingCalendar.get_trading_status(dt)
        return status in ("交易中", "集合竞价", "收盘集合竞价")

    @staticmethod
    def get_latest_trading_day(dt=None):
        if dt is None:
            dt = datetime.now().date()
        return shsz.previous_trading_day(dt)

if __name__ == '__main__':
    calendar = AStockTradingCalendar()
    print(calendar.get_trading_status())