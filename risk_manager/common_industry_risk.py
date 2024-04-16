from utils.tool import get_data_from_mongo
import pandas as pd
from datetime import datetime, timedelta


class BasicIndustryRisk(object):
    """
    基础工业风险
    """

    def __int__(self, *args, **kwargs):
        pass

    def up_data(self):
        """
        上游数据
        :return:
        """
        pass

    def mid_data(self):
        """
        中游数据
        :return:
        """
        pass

    def down_data(self):
        """
        下游数据
        :return:
        """
        pass

    def get_data_from_board(self, name=None, unit=None, data_type=None):
        """
        从海关获取商品月均价,按年累计价格,金额，数据，数据
        :return:
        """
        database = 'govstats'
        collection = 'customs_goods'
        if name is None:
            name = '尿素'
        if unit is None:
            unit = '万吨'
        if data_type is None:
            data_type = "export_goods_detail"
        projection = {'_id': False}
        condition = {"name": name, "data_type": data_type, "unit": unit}
        sort_key = "date"
        data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                                   sort_key=sort_key)
        dict_fs = {
            "acc_price": {"dnum": "acc_month_volume", "num": "acc_month_amount"},
            "cur_price": {"dnum": "month_volume", "num": "month_amount"}
        }
        dict_name_mapping = {
            "acc_price": "累计价格",
            "cur_price": "当前价格"
        }
        for k,combine_key in dict_fs.items():
            dnum = combine_key['dnum']
            num = combine_key['num']
            try:
                data[dnum] = data[dnum].astype(float)
                data[num] = data[num].astype(float)
                data[k] = round(data[num]/data[dnum],4)
            except Exception as _:
                info_name = dict_name_mapping[k]
                print(f"处理{info_name}数据出错,不计算{info_name}")
                continue
        return data



    def get_data_from_goods(self, goods_list=None, time=None):
        """
        获取商品数据
        :return:
        """
        if time is None:
            time = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")
        if goods_list is None:
            goods_list = ['铜', '铝', '锡', 'WTI原油', 'Brent原油', '氧化镝', '金属镝', '镨钕氧化物']
        condition = {"name": {"$in": goods_list}, "time": {"$gte": time}, "data_type": "goods_price"}
        sort_key = 'time'
        database = 'stock'
        collection = 'goods'
        projection = {"_id": False}
        data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                                   sort_key=sort_key)
        data[['value']] = data[['value']].astype(float)
        data = pd.pivot_table(data, index='time', columns='name', values='value')
        data['time'] = data.index
        return data

    def get_data_from_cn_st(self, code_dict: dict = None, time: str = None):
        """
        从国家统计局获取数据
        :return:
        """
        database = 'govstats'
        collection = 'data_info'
        projection = {'_id': False}
        if code_dict is None:
            code_dict = {'A020O0913_yd': '酒、饮料和精制茶制造业营业收入累计增长率'}
        if time is None:
            time = "201801"
        code_list = {"$in": list(code_dict.keys())}
        condition = {"code": code_list, "time": {"$gte": time}}
        sort_key = 'time'
        data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                                   sort_key=sort_key)
        data['data'] = data['data'].astype(float)
        data = pd.pivot_table(data, values='data', index='time', columns='code')
        data['time'] = data.index
        return data

    def tool_filter_month_data(self, data: pd.DataFrame, key=None, gt_month=2):
        if key is None:
            key = 'time'
        data['month'] = data[key].apply(lambda ele: int(ele[4:6]))
        data = data[data['month'] >= gt_month]
        return data

    def final_result(self):
        """
        最终结果
        :return:
        """
        pass
if __name__ == '__main__':
    info = BasicIndustryRisk()