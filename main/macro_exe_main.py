import sys
import os
#可以在该目录之前执行该程序，否则会报引用工程包不存在
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))
from data.global_assets_data import enter_main_gold_position_data



if __name__ == '__main__':
    enter_main_gold_position_data()