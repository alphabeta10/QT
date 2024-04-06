from data.futures_daily_data import handle_futures_daily_data, handle_futures_inventory_data, \
    handle_futures_receipt_data, handle_futures_czce_warehouse_receipt, \
    handle_futures_dce_warehouse_receipt, handle_futures_shfe_warehouse_receipt, handle_futures_delivery_dce, \
    handle_futures_delivery_czce,enter_futrures_long_short_main
import akshare as ak
from datetime import datetime, timedelta


def future_basic_info_data_main():
    # 行情数据
    symbols = ['B0', 'FG0', 'FG2409', 'EG0', 'EG2409', 'RB0', 'RB2410', 'I0', 'UR2409', 'UR0', 'MA0', 'MA2409', 'BU0']
    handle_futures_daily_data(symbols=symbols)
    # 库存数据
    symbols = ['玻璃', '螺纹钢', '乙二醇', '豆二', '尿素', '沥青', '甲醇']
    symbols = None
    handle_futures_inventory_data(symbols)

    # 注册仓单获取
    codes = ['B', 'FG', 'EG', 'RB', 'I', 'UR']
    handle_futures_receipt_data(codes=codes)
    # 仓单日报数据
    tool_trade_date_hist_sina_df = ak.tool_trade_date_hist_sina()
    trade_dates = []
    month_dates = set()
    now_int = int(datetime.now().strftime("%Y%m%d"))
    before_day_int = int((datetime.now() - timedelta(days=5)).strftime("%Y%m%d"))
    for index in tool_trade_date_hist_sina_df.index:
        trade_date = tool_trade_date_hist_sina_df.loc[index]['trade_date']
        date_str = str(trade_date).replace("-", "")
        if int(date_str) > before_day_int and int(date_str) <= now_int:
            trade_dates.append(date_str)
            month_dates.add(date_str[:6])

    # 和上面注册仓单重复了
    # 郑商所日报数据
    handle_futures_czce_warehouse_receipt(trade_dates)
    # 大商所日报数据
    handle_futures_dce_warehouse_receipt(trade_dates)
    # 上期所日报数据
    handle_futures_shfe_warehouse_receipt(trade_dates)

    # 大商所交割数据
    handle_futures_delivery_dce(month_dates)
    # 郑商所交割数据
    handle_futures_delivery_czce(trade_dates)
    # 金融板块期货商品多空比
    enter_futrures_long_short_main()

if __name__ == '__main__':
    future_basic_info_data_main()