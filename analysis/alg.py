import pandas as pd
import numpy as np
from hmmlearn import hmm
from sklearn import cluster

class Node(object):
    def __init__(self, value, next=None):
        self.value = value
        self.next = next

    def get_value(self):
        return self.value

    def get_next(self):
        return self.next


class MuNode(object):
    def __init__(self, value):
        self.value = value
        self.children = None


class Graph(object):
    def __init__(self):
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def print(self):
        for node in self.nodes:
            node_list = []
            while node is not None:
                node_list.append(str(node.value))
                node = node.next
            print(",".join(node_list))


def bfs(root):
    queue = []
    if root is not None:
        queue.append(root)
    while len(queue) > 0:
        node = queue.pop(0)
        if node.children is not None:
            for c_node in node.children:
                queue.append(c_node)
        print(node.value)


def dfs(root):
    if root is not None:
        c_root = root.children
        if c_root is not None:
            for root in c_root:
                dfs(root)
        print(root.value)


def dfs1(root):
    if root is not None:
        c_root = root.children
        if c_root is not None:
            for root in c_root:
                dfs1(root)
                print(root.data)


def graph_go1():
    list_node1 = Node(1)
    list_node1.next = Node(2)
    graph = Graph()
    graph.add_node(list_node1)

    list_node2 = Node(2)
    list_node2.next = Node(3)
    list_node2.next.next = Node(5)
    list_node2.next.next.next = Node(4)
    graph.add_node(list_node2)
    graph.print()


def dfs_go():
    root = MuNode(1)
    c1 = MuNode(2)
    c2 = MuNode(3)
    c3 = MuNode(4)
    c1.children = [MuNode(5), MuNode(6), MuNode(7)]
    c2.children = [MuNode(8), MuNode(9), MuNode(10)]
    c3.children = [MuNode(11), MuNode(12), MuNode(13)]
    root.children = [c1, c2, c3]
    dfs(root)


def get_nexts(b):
    m = len(b)
    next = [-1] * m
    k = -1
    i = 1
    while i < m:
        while k != -1 and b[k + 1] != b[i]:
            k = next[k]
        if b[k + 1] == b[i]:
            k += 1
        next[i] = k
        i += 1
    return next


def kpm(a, b):
    n, m = len(a), len(b)
    next = get_nexts(b)
    j = 0
    i = 0
    while i < n:
        while j > 0 and a[i] != b[j]:
            j = next[j - 1] + 1
        if a[i] == b[j]:
            j += 1
        if j == m:
            return i - m + 1
        i += 1


class TriNode(object):
    def __init__(self, data):
        self.data = data
        self.children = None
        self.is_end_char = False


class Trie(object):
    def __init__(self):
        self.root = TriNode('/')
        self.root.children = [TriNode(None)] * 26

    def insert(self, text):
        p = self.root
        for c in text:
            index = ord(c) - ord('a')
            if p.children[index].data is None:
                node = TriNode(c)
                node.children = [TriNode(None)] * 26
                p.children[index] = node
            p = p.children[index]
        p.is_end_char = True

    def find(self, text):
        p = self.root
        for c in text:
            index = ord(c) - ord('a')
            if p.children[index].data is None:
                return False
            p = p.children[index]
        return p.is_end_char


def counting_sort(arr, k):
    c = [0] * (k + 1)
    print(c)
    sorted_index = 0
    arr_len = len(arr)
    for j in range(arr_len):
        c[arr[j]] = c[arr[j]] + 1
    print(c)
    for j in range(k + 1):
        while c[j] > 0:
            arr[sorted_index] = j
            sorted_index += 1
            c[j] -= 1
    print(arr)


def go1():
    def hande(x):
        new_v = ''
        for ele in ['c1', 'c2']:
            new_v += x[ele]
        return new_v

    df1 = pd.DataFrame([['a', 'b', '1'], ['b', 'a', '11']], columns=['c1', 'c2', 'cc1'])
    print(df1)
    df1['cccc2'] = df1.apply(hande, axis=1)
    print(df1)

    df2 = pd.DataFrame([['a', 'b', '2'], ['b', 'a', '11']], columns=['c1', 'c2', 'cc2'])
    df3 = pd.DataFrame([['a', 'b', '3'], ['b', 'a', '12']], columns=['c1', 'c2', 'cc3'])

    combine_data = pd.concat([df1, df2, df3], keys=['c1', 'c2'], join='inner')

    x = 671.2 * 10e7
    dn = 5 * 10e9
    e_kg = (x / dn) * 2.5
    print(e_kg)
    import operator
    dict_data = {"k1": 1, "K2": 2, "k3": 3}
    print(dict(sorted(dict_data.items(), key=operator.itemgetter(1), reverse=True)))

    #


class StockNode(object):
    def __init__(self, value: float = None, child: list = None, name: str = None):
        self.value = value
        self.child = child
        self.name = name


def construct_tree(node: StockNode, map: dict, metric_value: dict):
    for k, v in map.items():
        node.name = k
        node.value = metric_value.get(k)
        if node.child is None:
            node.child = []
        for ele in v:
            name = list(ele.keys())[0]
            value = metric_value.get(name)
            newNode = StockNode(value=value, name=name)
            node.child.append(newNode)
            construct_tree(newNode, ele, metric_value)


def find_node(node, name, list_data: list):
    if node.name is not None and node.name != name:
        if node.child is not None:
            for ele in node.child:
                find_node(ele, name, list_data)
    else:
        list_data.append(node)


def compared_data(node0, node1, name, metric_value0, metric_value1):
    list0 = []
    list1 = []
    find_node(node0, name, list0)
    find_node(node1, name, list1)
    v0 = metric_value0.get(list0[0].name)
    v1 = metric_value1.get(list0[0].name)
    if v0 > v1:
        compared = 0
    else:
        compared = 1
    result = {}
    for node in list0[0].child:
        v0 = metric_value0.get(node.name)
        v1 = metric_value1.get(node.name)
        print(f"name={node.name},node0={v0},node1={v1}")
        if compared == 0:
            if v0 > v1:
                result[node.name] = [v0, v1]
        if compared == 1:
            if v1 > v0:
                result[node.name] = [v1, v0]
    if compared == 0:
        return {"第一个大于第二个分析是": result}
    if compared == 1:
        return {"第二个大于第一个分析是": result}


def stock_tree_go1():
    tree_map = {"净资产收益率": [{"销售净利率": []}, {"权益乘数": []}, {"总资产周转率": []}]}

    metric = ['销售净利率', '权益乘数', '总资产周转率']
    metric_value0 = {"销售净利率": 0.1, "权益乘数": 0.3, "总资产周转率": 0.3, "净资产收益率": 0.3 * 0.3 * 0.1}
    metric_value1 = {"销售净利率": 0.2, "权益乘数": 0.3, "总资产周转率": 0.9, "净资产收益率": 0.9 * 0.3 * 0.2}

    root0 = StockNode()
    construct_tree(root0, tree_map, metric_value0)
    print(root0)
    root1 = StockNode()
    construct_tree(root1, tree_map, metric_value1)
    print(root1)
    for k, v in tree_map.items():
        print(compared_data(root0, root1, k, metric_value0, metric_value1))
        for ele in v:
            print(compared_data(root0, root1, list(ele.keys())[0], metric_value0, metric_value1))


def judge_peak_lower(data):
    index_dict = {}
    for index, ele in enumerate(data):
        if index < len(data) - 1:
            right = data[index + 1]
        else:
            right = None
        if index > 0:
            left = data[index - 1]
        else:
            left = None
        is_peak = None
        if right is not None and left is not None:
            if ele > right and ele > left:
                is_peak = True
            if ele < right and ele < left:
                is_peak = False
        if right is None and left is not None:
            if ele > left:
                is_peak = True
            if left > ele:
                is_peak = False
        if left is None and right is not None:
            if ele > right:
                is_peak = True
            if ele < right:
                is_peak = False
        index_dict[index] = is_peak
    return index_dict


def judge_is_peak_go1():
    data = np.array([9, 4, 3, 2, 8, 7, 100])
    print(judge_peak_lower(data))


def hmm_go1():
    model = hmm.GaussianHMM(n_components=3, covariance_type='full')
    model.startprob_ = np.array([0.6, 0.3, 0.1])
    model.transmat_ = np.array([[0.7, 0.2, 0.1],
                                [0.3, 0.5, 0.2],
                                [0.3, 0.3, 0.4]])
    model.means_ = np.array([[0.0, 0.0], [3.0, -3.0], [5.0, 10.0]])
    model.covars_ = np.tile(np.identity(2), (3, 1, 1))
    X, Z = model.sample(100)
    print(X)
    print(Z)
    print("*" * 10)

    kmeans = cluster.KMeans(n_clusters=3,
                            random_state=None,
                            n_init=1)  # sklearn <1.4 backcompat.
    kmeans.fit(X)
    print("*"*10)
    rmodel = hmm.GaussianHMM(n_components=3, covariance_type="full", n_iter=100)
    rmodel.fit(X)
    z2 = rmodel.predict(X)
    print(z2)


def go():
    """
    因子模型，特征值 cpi 中国利率数据 美国利率数据 汇率数据
    """
    judge_is_peak_go1()
    rate = 2262 / 4214
    print(rate)
    rate = 376 / 487.4
    rate = 1719126404 / 10000000
    print(rate)
    rate = 45 / 2993.3
    rate = (3568.7 * 10000000) / 6000000000
    print(rate)
    rate = (19530 * 14e9) / (913026.5 * 1e9)
    print(rate)
    rate = (689.2 * 10e7 * 2.5) / (2 * 1e3)
    print((rate / 1e5))
    print(7 / (rate / 1e5))

    X = np.array([[6.87254073e+00, 1.11424752e+01],
         [6.46734156e+00,1.08563634e+01],
         [4.55272003e+00, 1.08992707e+01],
         [-3.61808939e-01, 1.32018001e+00]])
    rmodel = hmm.GaussianHMM(n_components=3, covariance_type="full", n_iter=100)
    rmodel.fit(X)
    z2 = rmodel.predict(X)
    print(z2)



if __name__ == '__main__':
    hmm_go1()


