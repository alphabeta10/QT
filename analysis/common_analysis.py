from utils.tool import get_data_from_mongo, get_mongo_table, sort_dict_data_by
import pandas as pd
from datetime import datetime, timedelta
from pyecharts.charts import Bar, Line, Pie
from pyecharts.components import Table
from pyecharts.commons.utils import JsCode
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

    def get_data_from_mongondb(self, database, collection, projection, condition, sort_key, self_fn=None):
        pd_data = get_data_from_mongo(database=database, collection=collection, projection=projection,
                                      condition=condition,
                                      sort_key=sort_key)
        if self_fn:
            self_fn(pd_data)
        return pd_data

    def get_data_from_board(self, name=None, unit=None, data_type=None, condition=None, is_cal=True,
                            val_keys: list = None):
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
        if len(data) > 0:
            dict_fs = {
                "acc_price": {"dnum": "acc_month_volume", "num": "acc_month_amount"},
                "cur_price": {"dnum": "month_volume", "num": "month_amount"}
            }
            dict_name_mapping = {
                "acc_price": "累计价格",
                "cur_price": "当前价格"
            }
            unit = condition.get('unit','')
            if is_cal and '-' != unit:
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
        convert_data = pd.pivot_table(data, index='time', columns='name', values='value')
        convert_data.reset_index(inplace=True)
        return data,convert_data

    def get_goods_data_aline_and_raw(self, goods_name_list, c_time):
        """
        获取原始商品数据以及对齐天的数据
        :param goods_name_list:
        :param c_time:
        :return:
        """
        goods = get_mongo_table(database='stock', collection='goods')

        datas = []
        other_datas = []
        # 轻质纯碱 铁矿石(澳) 螺纹钢 玻璃 乙二醇 重质纯碱
        goods_condition = {"$in": goods_name_list}
        for ele in goods.find({"name": goods_condition, "data_type": "goods_price", "time": {"$gte": c_time}},
                              projection={'_id': False}).sort(
            "time"):
            time = ele['time']
            value = ele['value']
            name = ele['name']
            year = time[0:4]
            new_name = f"{year}{name}"
            new_time = time[4:9]
            other_datas.append({"name": new_name, "value": value, "time": new_time, "raw_name": name})
            datas.append(ele)
        raw_data = pd.DataFrame(data=datas)
        other_pd_data = pd.DataFrame(data=other_datas)
        return raw_data, other_pd_data

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

    def bar_chart(self, x_labels, y_dict_data: dict):

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

    def table_chart(self, header, rows, page_title):
        table = Table()
        table.add(header, rows, {"max_width": "100px"})
        return table

    def pie_chart(self, name, attr_dict_data:dict, val_dict_data: dict):

        fn = """
            function(params) {
                if(params.name == '其他')
                    return '\\n\\n\\n' + params.name + ' : ' + params.value + '%';
                return params.name + ' : ' + params.value + '%';
            }
            """

        def new_label_opts():
            return opts.LabelOpts(formatter=JsCode(fn), position="center")

        pie = Pie()
        # pie.set_global_opts(
        #     title_opts=opts.TitleOpts(title=name),
        #     legend_opts=opts.LegendOpts(
        #         type_="scroll", pos_top="20%", pos_left="80%", orient="vertical"
        #     )
        # )

        i = 0
        col_size = 2 #画3个图
        px_col,px_row = 20,30
        for ele_name, v_list in val_dict_data.items():
            attr = attr_dict_data.get(ele_name)
            row = i // col_size
            col = i % col_size
            ppx_col = px_col if col==0 else px_col*col+25
            ppx_col = str(ppx_col)+"%"
            ppx_row = px_row if row == 0 else px_row * row + 40
            ppx_row = str(ppx_row)+"%"

            print(row,col,ppx_col,ppx_row)
            pie.add('', [list(z) for z in zip(attr, v_list)],
                    radius=[30, 40],
                    center=[ppx_col,ppx_row],
                    label_opts=new_label_opts())
            i+=1
        # pie.set_series_opts(
        #     tooltip_opts=opts.TooltipOpts(
        #         trigger="item", formatter="{a} <br/>{b}: {c} ({d}%)"
        #     ))
        #pie.set_global_opts(legend_opts=opts.LegendOpts(pos_left="legft", orient="vertical"))
        pie.set_global_opts(
        title_opts=opts.TitleOpts(title=name),
        legend_opts=opts.LegendOpts(
            type_="scroll", pos_top="20%", pos_left="80%", orient="vertical"
        ),
    )
        return pie

    def bar_line_overlap(self, x_labels, bar_y_dict_data: dict, line_y_dict_data: dict):
        bar = Bar(init_opts=opts.InitOpts(
            width='1700px', height='1000px'
        ))
        bar.add_xaxis(x_labels)
        for col_name, list_data in bar_y_dict_data.items():
            bar.add_yaxis(col_name, list_data, z=0)
        bar.extend_axis(
            yaxis=opts.AxisOpts(
                axislabel_opts=opts.LabelOpts(formatter="{value}")
            )
        )
        bar.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
        bar.set_global_opts(
            # title_opts=opts.TitleOpts(title="Overlap-bar+line"),
            yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(formatter="{value}"),is_scale=True),
        )
        line = Line().add_xaxis(x_labels)
        for col_name, list_data in line_y_dict_data.items():
            line.add_yaxis(col_name, list_data, yaxis_index=1)
        bar.overlap(line)
        return bar

    def line_chart(self, x_labels, y_dict_data: dict):
        line = Line(init_opts=opts.InitOpts(
            width='1700px', height='1000px'
        ))
        line.set_global_opts(
            xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=-15)),
        )
        line.set_global_opts(
            yaxis_opts=opts.AxisOpts(
                axistick_opts=opts.AxisTickOpts(is_show=True),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
        line.add_xaxis(x_labels)
        for col_name, list_data in y_dict_data.items():
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

    def df_to_chart(self, df: pd.DataFrame, chart: list, chart_type=None, cols: list = None, index_col_key: str = None):
        if cols is None:
            cols = df.columns
        if index_col_key is None:
            chart_index = list(df.index)
        else:
            chart_index = list(df[index_col_key].values)
        data_dict = {}
        for col in cols:
            data_dict[col] = [round(ele, 2) for ele in list(df[col].values)]
        if chart_type is None or chart_type == 'bar':
            bar_c = self.bar_chart(chart_index, data_dict)
            chart.append(bar_c)
        elif chart_type == 'line':
            line_c = self.line_chart(chart_index, data_dict)
            chart.append(line_c)

    def comm_down_or_up_risk(self, data: pd.DataFrame, cal_cols: list, before_num_list: list, col_up_or_down: dict,
                             time_col: str):
        """
        计算下跌或者上升风险方法
        :param data:数据
        :param cal_cols:列名列表
        :param before_num_list:计算的前一个值对比列表
        :param col_up_or_down:上涨还是下跌类型
        :param time_col:
        :return:
        """
        for i in before_num_list:
            for col in cal_cols:
                data[f'{col}_pct_{i}'] = round(data[col].diff(i), 4)
        all_detail_risk = []
        all_datas = []
        for index in data.index:

            detail_risk = {}
            total_risk = 0
            dict_data = dict(data.loc[index])
            if time_col == 'index':
                time = str(index)
            else:
                time = dict_data[time_col]
            for col in cal_cols:
                up, down = 0, 0
                for i in before_num_list:
                    if dict_data[f'{col}_pct_{i}'] > 0:
                        up += 1
                    else:
                        down += 1
                up_or_down = col_up_or_down.get(col)
                if up_or_down == 'up':
                    ele_risk = round(up / len(before_num_list), 4)
                else:
                    ele_risk = round(down / len(before_num_list), 4)
                detail_risk[col] = {"up": up, "down": down, "total_risk": ele_risk}
                total_risk += (1 / len(cal_cols)) * ele_risk

            detail_risk['time'] = time
            detail_risk['total_risk'] = total_risk
            all_detail_risk.append(detail_risk)
            dict_data['total_risk'] = total_risk
            dict_data['time'] = time
            all_datas.append(dict_data)
        return all_detail_risk, all_datas

    def common_cal_fin_result(self, data: pd.DataFrame, change_config: dict, is_year_end=False, analysis_type='zcfz',
                              def_fn=None):
        """
        计算财报的公共方法
        :param data:
        :param change_config:
        :param is_year_end:
        :param analysis_type:
        :param def_fn:
        :return:
        """
        record_year_data = {}
        res_data = []
        for index in data.index:
            dict_data = dict(data.loc[index])
            if is_year_end:
                if '12-31' in dict_data.get('date'):
                    record_year_data[dict_data['date']] = dict_data
                before_year = str(int(dict_data['date'][0:4]) - 1) + "-12-31"
            else:
                record_year_data[dict_data['date']] = dict_data
                before_year = str(int(dict_data['date'][0:4]) - 1) + dict_data['date'][4:]
            before_data = record_year_data.get(before_year, None)
            if before_data is not None:
                show_list = []
                for key, sub_dict in change_config.items():
                    key_diff = round((float(dict_data.get(key, 0)) - float(before_data.get(key, 0))) / 1e8, 3)
                    temp_sub_diff = {}
                    name = sub_dict['name']
                    for sub_key, sub_name in sub_dict['sub_key'].items():
                        temp_sub_diff[sub_name] = round(
                            (float(dict_data.get(sub_key, 0)) - float(before_data.get(sub_key, 0))) / 1e8, 3)
                    analysis_result = None
                    show_text = f"{name}"
                    if key_diff > 0:
                        show_text += f"增长{key_diff}亿;"
                        analysis_result = sort_dict_data_by(temp_sub_diff, by='value', reverse=True)
                    elif key_diff < 0:
                        show_text += f"减少{key_diff}亿;"
                        analysis_result = sort_dict_data_by(temp_sub_diff, by='value')
                    else:
                        print("么有分析结果")
                    if analysis_result is not None:
                        show_text += "主要原因是:"
                        for key, val in analysis_result.items():
                            if key_diff > 0:
                                if val > 0:
                                    show_text += f"{key}增长{val}亿;"
                            if key_diff < 0:
                                if val < 0:
                                    show_text += f"{key}减少{val}亿;"
                        show_list.append(show_text)
                if len(show_list) > 0:
                    dict_data[analysis_type] = "|".join(show_list)
                res_data.append(dict_data)
                if def_fn is not None:
                    def_fn(dict_data, before_data)
        return res_data


if __name__ == '__main__':
    basic = BasicAnalysis()
    data = basic.get_data_from_board()
