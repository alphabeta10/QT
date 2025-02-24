import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from data.stock_daily_data import save_stock_info_data,handle_stock_daily_data,stock_dzjy_main,handle_stock_cyq_main
from data.stock_index_data import index_data
from data.stock_industry_data import ths_industry_daily_data
from data.stock_margin_data import handle_simple_sz_margin_data,handle_margin_sz_sh_total_data,handle_sz_sh_margin_detail_daily
from data.stock_seq_daily import stock_ggcg
from data.stock_news import stock_em_news_main
from datetime import datetime
def stock_basic_main():
    """
    股票基础信息，股票价格，大宗交易,筹码分布
    :return:
    """
    handle_stock_cyq_main()
    stock_em_news_main()
def stock_index_main():
    """
    A股指数数据
    :return:
    """
    index_data()

def stock_concept_main(start_year=None):
    """
    A股概念数据
    :param start_year:
    :return:
    """
    if start_year is None:
        start_year = int(datetime.now().strftime("%Y"))
    cur_year = int(datetime.now().strftime("%Y"))
    while start_year <= cur_year:
        print(f"handle year={start_year}")
        ths_industry_daily_data(start_year=str(start_year))
        start_year += 1
def stock_margin_main():
    """
    A股融资融券数据
    :return:
    """
    handle_simple_sz_margin_data()
    handle_margin_sz_sh_total_data()
    handle_sz_sh_margin_detail_daily()
def stock_ggcg_main():
    """"
    股票股东减持数据
    """
    stock_ggcg()

if __name__ == '__main__':
    stock_basic_main()