from utils.tool import get_data_from_mongo
import pandas as pd
from datetime import datetime, timedelta
from pyecharts.charts import Bar,Line
from pyecharts.components import Table
from pyecharts import options as opts

class BasicAnalysis(object):
    """
    基础分析类
    """

    def generator_analysis_html(self):
        """
        实现生成分析的html文件
        :return:
        """
        pass

    def get_data_from_board(self, name=None, unit=None, data_type=None, condition=None, is_cal=True,val_keys:list=None):
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

        if condition is None:
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
        if is_cal:
            for k, combine_key in dict_fs.items():
                dnum = combine_key['dnum']
                num = combine_key['num']
                try:
                    data[dnum] = data[dnum].astype(float)
                    data[num] = data[num].astype(float)
                    data[k] = round(data[num] / data[dnum], 4)
                except Exception as _:
                    info_name = dict_name_mapping[k]
                    print(f"处理{info_name}数据出错,不计算{info_name}")
                    continue
        if val_keys is not None:
            for val_key in val_keys:
                data[val_key] = data[val_key].astype(float)
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
        data.reset_index(inplace=True)
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
        data.reset_index(inplace=True)
        return data

    def get_data_from_seq_data(self, data_type, metric_code_list, time, val_keys):
        database = 'stock'
        collection = 'common_seq_data'
        projection = {'_id': False}

        if time is None:
            time = "201801"
        condition = {"data_type": data_type, "metric_code": {"$in": metric_code_list},
                     "time": {"$gt": time}}
        sort_key = 'time'
        data = get_data_from_mongo(database=database, collection=collection, projection=projection, condition=condition,
                                   sort_key=sort_key)
        for val_key in val_keys:
            data[val_key] = data[val_key].astype(float)
        return data

    def tool_filter_month_data(self, data: pd.DataFrame, key=None, gt_month=2):
        if key is None:
            key = 'time'
        data['month'] = data[key].apply(lambda ele: int(ele[4:6]))
        data = data[data['month'] >= gt_month]
        return data

    def bar_chart(self,x_labels, y_dict_data: dict):

        bar = Bar(init_opts=opts.InitOpts(
            width='1700px', height='1000px'
        ))
        bar.add_xaxis(x_labels)
        bar.set_global_opts(
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-15)),
        )
        # bar.set_global_opts(
        #     datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
        # )
        bar.set_global_opts(
            xaxis_opts=opts.AxisOpts(splitline_opts=opts.SplitLineOpts(is_show=False)),
            yaxis_opts=opts.AxisOpts(
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
        for col_name, list_data in y_dict_data.items():
            bar.add_yaxis(col_name, list_data)
        return bar

    def line_chart(self,x_labels,y_dict_data:dict):
        line = Line(init_opts=opts.InitOpts(
            width='1700px', height='1000px'
        ))
        line.add_xaxis(x_labels)
        for col_name,list_data in y_dict_data.items():
            line.add_yaxis(
                series_name=col_name,
                y_axis=list_data,
                markpoint_opts=opts.MarkPointOpts(
                    data=[
                        opts.MarkPointItem(type_="max", name="最大值"),
                        opts.MarkPointItem(type_="min", name="最小值"),
                    ]
                ),
                markline_opts=opts.MarkLineOpts(
                    data=[opts.MarkLineItem(type_="average", name="平均值")]
                ),
            )
        return line


    def df_to_chart(self, df:pd.DataFrame, chart: list, chart_type=None,cols:list=None,index_col_key:str=None):
        if cols is None:
            cols = df.columns
        if index_col_key is None:
            chart_index = list(df.index)
        else:
            chart_index = list(df[index_col_key].values)
        data_dict = {}
        for col in cols:
            data_dict[col] = [round(ele, 2) for ele in list(df[col].values)]
        if chart_type is None:
            bar_c = self.bar_chart(chart_index, data_dict)
            chart.append(bar_c)
        elif chart_type=='line':
            line_c = self.line_chart(chart_index,data_dict)
            chart.append(line_c)


if __name__ == '__main__':
    basic = BasicAnalysis()
    data = basic.get_data_from_board()
