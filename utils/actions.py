import time

def try_get_action(func, try_count=1,delay=10, *args, **kwargs):
    """
    重试调用函数
    :param func: 函数名称
    :param try_count: 重试次数
    :param args: 参数
    :param kwargs: 参数
    :return: 返回调用函数的数据
    """
    index = 0
    while index < try_count:
        try:
            ret = func(*args, **kwargs)
            if ret is None:
                print('ret is None')
                index += 1
                time.sleep(10)
                print("try again")
            else:
                return ret
        except Exception as e:
            print(e)
            index += 1
            time.sleep(delay)
            print("try again")
    return None


def show_data(data):
    """
    打印明细pandas DataFrame数据
    :param data:
    :return:
    """
    for index in data.index:
        dict_data = dict(data.loc[index])
        index_name = 'index'
        if 'index' in dict_data.keys():
            index_name = "pd_index"
        dict_data[index_name] = index
        print(dict_data)
