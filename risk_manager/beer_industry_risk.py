import pandas as pd

from risk_manager.common_industry_risk import BasicIndustryRisk
from risk_manager.macro_risk import comm_down_or_up_risk
from utils.actions import show_data


class BeerIndustryRisk(BasicIndustryRisk):

    def __init__(self):
        # 上游配置数据信息
        self.up_config = [
            {"data_source": "board", "name": "大麦", "data_type": "import_goods_detail", "unit": "万吨"},
            {"data_source": "goods",
             "goods_list": ['针叶木浆', '白卡纸', '白板纸', '阔叶木浆', '瓦楞原纸', '废纸', '小麦']},
        ]
        # 中游配置数据信息
        self.mid_config = [
            {"data_source": "cn_st", "code_dict": {"A02090A04_yd": "啤酒产量累计增长(%)",
                                                   "A020O0J10_yd": "食品制造业营业利润累计增长(%)",
                                                   "A020O0J13_yd": "酒、饮料和精制茶制造业营业利润_累计增长(%)",
                                                   "A04030F_yd": "食品制造业固定资产投资额_累计增长(%)",
                                                   "A04030G_yd": "酒、饮料和精制茶制造业固定资产投资额_累计增长(%)",
                                                   }}
        ]
        # 下游配置数据信息
        self.down_config = [
            {"data_source": "cn_st", "code_dict": {
                'A07040104_yd': '粮油、食品、饮料、烟酒类商品零售类值_累计增长',
                'A07040108_yd': '粮油、食品类商品零售类值_累计增长',
                'A0704010K_yd': '饮料类商品零售类值_累计增长',
                'A0704010O_yd': '烟酒类商品零售类值_累计增长'}}
        ]
        # 其他配置数据信息
        self.other_config = [
            {"data_source": "cn_st", "code_dict": {'A07040104_yd': '粮油、食品、饮料、烟酒类商品零售类值_累计增长',
                                                   'A07040108_yd': '粮油、食品类商品零售类值_累计增长',
                                                   'A0704010K_yd': '饮料类商品零售类值_累计增长',
                                                   'A0704010O_yd': '烟酒类商品零售类值_累计增长'}}
        ]

        self.name = '啤酒行业'

        self.up_risk = None
        self.mid_risk = None
        self.down_risk = None

        self.up_risk = None
        self.mid_risk = None
        self.down_risk = None

    def up_data(self):
        total_risk = 0
        for ele in self.up_config:
            data_source = ele['data_source']
            if data_source == 'board':
                name = ele['name']
                data_type = ele['data_type']
                unit = ele['unit']
                data = self.get_data_from_board(name, unit, data_type)
                _, risk_data = comm_down_or_up_risk(data, ['acc_price', 'cur_price'], [1, 2, 3, 4, 5, 6],
                                                    {"acc_price": "up", "cur_price": "up"}, 'date')
                risk_data = pd.DataFrame(risk_data)
                total_risk += risk_data.tail(1).iloc[0]['total_risk']
            if data_source == 'goods':
                goods_list = ele.get('goods_list', [])
                col_up_or_down = {ele: 'up' for ele in goods_list}
                data = self.get_data_from_goods(goods_list)
                _, risk_data = comm_down_or_up_risk(data, goods_list, [1, 2, 3, 4, 5, 6], col_up_or_down, 'time')
                risk_data = pd.DataFrame(risk_data)
                total_risk += risk_data.tail(1).iloc[0]['total_risk']
        total_risk = round(total_risk, 4)
        if total_risk < 0.5:
            result = f"原材料端价格上涨风险{total_risk}，对于{self.name}是好事，建议看看中游利润"
        elif total_risk >= 0.5:
            result = f"原材料端价格上涨风险{total_risk}，对于{self.name}是坏事，原材料有上涨风险，建议看下游消费情况"
        else:
            result = "原材料端,没有计算出风险，建议看下游消费情况"
        self.up_risk = total_risk
        return result

    def mid_data(self):
        total_risk = 0
        for ele in self.mid_config:
            data_source = ele['data_source']
            if data_source == 'cn_st':
                code_dict = ele.get('code_dict', {})
                data = self.get_data_from_cn_st(code_dict)
                data = self.tool_filter_month_data(data)
                _, risk_data = comm_down_or_up_risk(data, list(code_dict.keys()), [1, 2, 3, 4, 5, 6],
                                                    {}, 'time')
                risk_data = pd.DataFrame(risk_data)
                total_risk += risk_data.tail(2).iloc[0]['total_risk']
        total_risk = round(total_risk, 4)
        if total_risk < 0.5:
            result = f"中游风险{total_risk}，对于{self.name}是好事，建议看看下游销售"
        elif total_risk >= 0.5:
            result = f"中游风险{total_risk}，对于{self.name}是坏事，建议看下游消费情况"
        else:
            result = "中游风险,没有计算出风险，建议看下游消费情况"
        print(result)
        return result

    def down_data(self):
        total_risk = 0
        for ele in self.down_config:
            data_source = ele['data_source']
            if data_source == 'cn_st':
                code_dict = ele.get('code_dict', {})
                data = self.get_data_from_cn_st(code_dict)
                data = self.tool_filter_month_data(data)
                _, risk_data = comm_down_or_up_risk(data, list(code_dict.keys()), [1, 2, 3, 4, 5, 6],
                                                    {}, 'time')
                risk_data = pd.DataFrame(risk_data)
                total_risk += risk_data.tail(2).iloc[0]['total_risk']
        total_risk = round(total_risk, 4)
        if total_risk < 0.5:
            result = f"下游风险{total_risk}，对于{self.name}是好事，下游消费强劲"
        elif total_risk >= 0.5:
            result = f"中游风险{total_risk}，对于{self.name}是坏事，下游消费萎缩"
        else:
            result = "中游风险,没有计算出风险，建议看下游消费情况"
        print(result)
        return result


if __name__ == '__main__':
    info = BeerIndustryRisk()
    info.up_data()
    info.mid_data()
    info.down_data()
