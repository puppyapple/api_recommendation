# -*- coding: utf-8 -*-

import pickle
import pandas as pd
import numpy as np
import configparser
import multiprocessing as mp
import ast
from functools import reduce
from itertools import product
from Code import pipline, data_calculator
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler
from py2neo import Graph


comp_infos, ctag_ctag, ctag_nctag, nctag_nctag, ctag_position, comp_id_name_dict, tag_dict = pipline.data_loader()
config = configparser.ConfigParser()
config.read("../Data/Input/database_config/database.conf")
host = config['NEO4J']['host']
user_name = config['NEO4J']['username']
pass_word = config['NEO4J']['password']


graph = Graph(
    host,
    username=user_name,
    password=pass_word
)

def cal_tag_cartesian(tag_set1, tag_set2, value_dict, tag_link_filter):
    if tag_set1 == 0 or tag_set2 == 0:
        return 0
    else:
        pair_list = list(product(tag_set1, tag_set2))
        value_list = [value_dict.get(t[0] + "-" + t[1], 0) for t in pair_list]
        value_sum = sum([v for v in value_list if v >= tag_link_filter])
        # print(value_sum)
        return value_sum
    
def cal_tags_link(comp_info1, comp_info2, tag_link_filters):
    ctags1 = comp_info1.get("ctags", set())
    ctags2 = comp_info2.get("ctags", set())
    nctags1 = comp_info1.get("nctags", set())
    nctags2 = comp_info2.get("nctags", set())
    num_ctags1 = len(ctags1)
    num_ctags2 = len(ctags2)
    num_nctags1 = len(nctags1)
    num_nctags2 = len(nctags2)
    num_all1 = num_ctags1 + num_nctags1
    num_all2 = num_ctags2 + num_nctags2
    
    coef1 = 1/np.sqrt(1 + (num_ctags1 - num_ctags2)**2)
    coef2 = 1/np.sqrt(1 + (num_nctags1 - num_nctags2)**2)
    coef3 = data_calculator.final_count(ctags1, ctags2)
    coef4 = data_calculator.final_count(nctags1, nctags2)
    
    v1 = coef3 * cal_tag_cartesian(ctags1, ctags2, ctag_ctag, tag_link_filters[0])
    v2 = cal_tag_cartesian(nctags1, ctags2, ctag_nctag, tag_link_filters[1]) + cal_tag_cartesian(nctags2, ctags1, ctag_nctag, tag_link_filters[1])
    v3 = coef4 * cal_tag_cartesian(nctags1, nctags2, nctag_nctag, tag_link_filters[2])
    return (v1, v2, v3)

def cal_company_dis(target_comp_info, part,  weights, tag_link_filters):
    # print("start")
    three_value_list = list(part.comp_property_dict.apply(lambda x: cal_tags_link(target_comp_info, x, tag_link_filters)))
    part["three_values"] = three_value_list
    # print("end")
    return part

def concept_tree_relation(comp_info1, comp_info2):
    top_tag1 = comp_info1.get("top_ctag", set())
    top_tag2 = comp_info2.get("top_ctag", set())
    bottom_tag1 = comp_info1.get("bottom_ctag", set())
    bottom_tag2 = comp_info2.get("bottom_ctag", set())
    is_same_tree = len(top_tag1.intersection(top_tag2)) > 0
    bottom_tag_relation = np.array([(str(t[0]) + "-" + str(t[1])) in list(ctag_position.keys()) for t in list(product(bottom_tag1, bottom_tag2))])
    is_same_link = sum(bottom_tag_relation >= 0) > 0
    return (is_same_tree, is_same_link)
    
def branch_stock_relation(comp_id, graph):
    stock_rel_statement = "match p=(c:company{id:'%s'})-[:ABSOLUTE_HOLDING|:UNKNOWN|:WHOLLY_OWNED|:JOINT_STOCK|:RELATIVE_HOLDING*1..2]-(c2:company) \
        return c2.id as comp_id,TRUE as has_stock_relation" % (comp_id)
    stock_rel_comps = pd.DataFrame(graph.run(stock_rel_statement).data(), columns=["comp_id", "has_stock_relation"])
    branch_rel_statement = "match p=(c:company{id:'%s'})-[:BRANCH*1..2]-(c2:company) return c2.id as comp_id,TRUE as has_branch_relation" % (comp_id)
    branch_rel_comps = pd.DataFrame(graph.run(branch_rel_statement).data(), columns=["comp_id", "has_branch_relation"])
    return (stock_rel_comps, branch_rel_comps)
    
def multi_process_rank(comp_name, comp_info="", graph=graph, weights=(0.5, 0.4, 0.1), response_num=None, tag_link_filters=(0.0, 0.3, 0.3), process_num=6):
    if comp_info == "":
        comp_id_name_dict_reverse = dict(zip(comp_id_name_dict.values(), comp_id_name_dict.keys()))
        comp_id = comp_id_name_dict_reverse.get(comp_name)
        target_comp_info = list(comp_infos[comp_infos.comp_id == comp_id].comp_property_dict)[0]
    else:
        data_dict = ast.literal_eval(comp_info)
        target_comp_info = {k: set([tag_dict.get(t) for t in v]) for k, v in data_dict.items()}
        comp_id = ""
    
    # 如果目标公司具备概念标签，则概念-非概念关系值的权重保留较高，否则提高非概念标签之间值的权重
    print("start!")
    if target_comp_info.get("ctags") == None:
        weights = (weights[0], weights[2], weights[1])
    result_list = []
    split_comp_infos = np.array_split(comp_infos, process_num)
    pool = mp.Pool()
    for i in range(0, process_num):
        result_list.append(pool.apply_async(cal_company_dis, (target_comp_info, split_comp_infos[i], weights, tag_link_filters,)))
    pool.close()
    pool.join()
    result_merged = pd.concat([r.get() for r in result_list])
    print("end!")
    result_merged.drop_duplicates(subset=["comp_id"], inplace=True)
    scaler = MinMaxScaler(feature_range=(0, 100))
    to_transform = np.array(list(result_merged.three_values))
    scaler.fit(to_transform)
    result_merged["sim_value"] = (scaler.transform(to_transform) *  weights).sum(axis=1)
    result_merged = result_merged[result_merged.comp_id != comp_id]
    result_merged.reset_index(drop=True, inplace=True)
    tree_relation = pd.DataFrame(list(result_merged.comp_property_dict.apply(lambda x: concept_tree_relation(target_comp_info, x))), columns=["is_same_tree", "is_same_link"])
    result_merged = pd.concat([result_merged, tree_relation], axis=1)
    
    stock_rel_comps, branch_rel_comps = branch_stock_relation(comp_id, graph)
    result_merged = result_merged.merge(stock_rel_comps, how="left", left_on="comp_id", right_on="comp_id") \
        .merge(branch_rel_comps, how="left", left_on="comp_id", right_on="comp_id").fillna(False)
    if response_num == None:
        response_num = len(result_merged)
    result_sorted = result_merged.sort_values(by="sim_value", ascending=False)[:response_num].copy()
    result_sorted['comp_name'] = result_sorted.comp_id.apply(lambda x: comp_id_name_dict.get(x))
    result_sorted.reset_index(drop=True, inplace=True)
    return result_sorted[["comp_id", "comp_name", "sim_value", "is_same_tree", "is_same_link", "has_stock_relation", "has_branch_relation"]]
    
def sample_test(comp_id, graph, each_num=(20, 20), weights=(0.5, 0.4, 0.1), tag_link_filters=(0.0, 0.3, 0.3)):
    result_raw = multi_process_rank(comp_id, graph, weights=weights, tag_link_filters=tag_link_filters)
    same_tree = result_raw[result_raw.is_same_tree == True][:each_num[0]]
    not_same_tree = result_raw[result_raw.is_same_tree == False][:each_num[1]]
    return pd.concat([same_tree, not_same_tree])
    