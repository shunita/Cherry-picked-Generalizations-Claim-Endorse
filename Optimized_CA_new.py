import Naive
from PartitionQuery import PartitionQuery
import copy
import threading
import time
import numpy as np
import pandas as pd
from tkinter import _flatten
import copy
from multiprocessing import Pool, cpu_count, Manager
import os
import time
from CounterArgument import CounterArgument
from multiprocessing.managers import BaseManager


# single-process
# def ComputeScore(R, Q, Q_count, dic, A, S, scores, inverted_index):
#     score = 0
#     for r in R:
#         att = [a for a in Q.att]
#         for a in A:
#             if not a in att:
#                 att.append(a)
#         cond = {**Q.cond, **r}
#         weight = get_weight(Q_count, cond, dic)
#         support = 0
#         if weight > 0:
#             QQ = PartitionQuery(att, cond, Q.target)
#             support = get_support(QQ, dic, S.groups, inverted_index)
#         score += weight * support
#     scores.append(score)

# multi-process
def ComputeScore(R, Q, Q_count, dic, A, S, scores):
    score = 0
    for r in R:
        att = [a for a in Q.att]
        for a in A:
            if not a in att:
                att.append(a)
        cond = {**Q.cond, **r}
        weight = get_weight(Q_count, cond, dic)
        support = 0
        if weight > 0:
            QQ = PartitionQuery(att, cond, Q.target)
            support = get_support(QQ, dic, S.groups,[])
            # if support > 0:
            #     print(QQ, weight, support)
        score += weight * support
        #print(dic.head())
    scores.append(score)


# to compute the counts given the condition
def GetFromGMByAttribute(cond, dic):
    # condition is a dictionary
    new_dic = dic.reset_index()
    for key in cond:
        value = cond[key]
        new_dic = new_dic[(new_dic[key] == value)]
    return new_dic['count'].sum()

def updateMemoTableFromChild(prev_att,dic_v):
    # remove the first one and the last 3
    selected = list(dic_v.reset_index().head())[:-3]
    selected.remove(prev_att)
    #print(selected)
    # we can do this because according to the rule we always remove the first item in the tuple
    result = dic_v.groupby(selected)[["count", "sum"]].sum()
    result["avg"] = result["sum"] / result["count"]
    result = result[['count', 'avg', 'sum']]
    return result


def get_weight(cond_count, r, table):
    #in this case, table is actually dic_v
    # cond is the r in this partition query and Q.cond is the pred in the original query
    g = cond_count
    g_r = 0
    # we cannot afford computing the g for sooo many times. It will takes a pretty long time.
    # I share the computation for g in each statement
    g_r = GetFromGMByAttribute(r, table)
    if g == 0:
        return 0
    weight = g_r/g
    return weight




# to return a set of aggregation values given the cond

def SubgroupAggSet(cond, dic):
    new_dic = dic.reset_index()
    for key in cond:
        value = cond[key]
        new_dic = new_dic[(new_dic[key] == value)]
    cols = [i for i in new_dic.columns if i not in ['count', 'avg', 'sum']]
    new_dic.set_index(cols, drop = True, append = False, inplace = True)
    return new_dic

def MergeStatement(sorted_groups, target, inverted_index):
    # need to be extend to multiple groups
    for_compare = pd.DataFrame()
    for g in sorted_groups:
        temp_df = g[0]
        temp_df["group_identifier"] = g[1]
        for_compare = pd.concat([for_compare, temp_df], axis=0)
    if target == "":
        for_compare.sort_values(by=["count"], ascending=True, inplace=True)
    else:
        for_compare.sort_values(by=["avg"], ascending=True, inplace=True)
    correct = 0
    CA_list = []
    for_att_names = for_compare.reset_index()
    att_names = [i for i in for_att_names.columns if i not in ['count', 'avg', 'sum', 'group_identifier']]
    passed = [0] * len(sorted_groups) # to count how many groups have passed
    for index, row in for_compare.iterrows():
        # need to be updated: assume g1 is greater than g2, and do not
        if (row["group_identifier"] == 'g2'):
            if len(passed) == 2 or all(val != 0 for val in passed[2:]):
                passed[1] += 1
                if(len(CA_list)!=0):
                    #have not been updated for the 
                    #inverted_index.Add(att_names, index, CA_list)
                    CA_list = []
            #CA_list.append(index)
        if (row["group_identifier"] == 'g3'):
            if len(passed) == 3 or all(val != 0 for val in passed[3:]):
                passed[2] += 1
            CA_list.append(index)
        if (row["group_identifier"] == 'g4'):
            if len(passed) == 4 or all(val != 0 for val in passed[4:]):
                passed[3] += 1
            CA_list.append(index)
        if (row["group_identifier"] == 'g5'):
            if len(passed) == 5 or all(val != 0 for val in passed[5:]):
                passed[4] += 1
            CA_list.append(index)
        if (row["group_identifier"] == 'g6'):
            if len(passed) == 6 or all(val != 0 for val in passed[6:]):
                passed[5] += 1
            CA_list.append(index)
        if (row["group_identifier"] == 'g7'):
            if len(passed) == 7 or all(val != 0 for val in passed[7:]):
                passed[6] += 1
            CA_list.append(index)
        if (row["group_identifier"] == 'g1'):
            correct += np.prod(passed[1:])
            CA_list.append(index)
    return correct



def get_support(QQ, table, groups, inverted_index):
    denominator = 1
    sorted_groups =[]
    group_id = list(groups.keys())
    for k in group_id:
        cond = copy.copy(QQ.cond)
        identifier = k
        cond[QQ.att[0]] = groups[identifier]
        agg_df = SubgroupAggSet(cond, table)
        if(QQ.target == ""):
            agg_df.sort_values(by=["count"], ascending = True, inplace = True)
        else:
            agg_df.sort_values(by=["avg"], ascending = True, inplace = True)
        denominator = denominator * len(agg_df)
        if denominator == 0:
            return 0
        sorted_groups.append([agg_df, identifier])
    numerator = MergeStatement(sorted_groups, QQ.target, inverted_index)
    supp = float(numerator) / (denominator)
    return supp


def get_score_optimized(G, S, mapping, heirerchy, atts,
                        par=True, space_opt=False, tarck_mem = False,verbose=False):
    score = 0
    #start1 = time.time()
    #tab
    table = {}
    L = Naive.get_nodes_buttom_up(G)
    node_num = len(L)
    print("The number of the nodes is")
    print(node_num)
    # add counter argument initialization
    # m2 = Manager2()
    print(atts)
    inverted_index  = CounterArgument(atts, S.df)
    #end1 = time.time()
    #time1 = end1 - start1
    #print("The time before GM traveral is")
    #print(time1)
    # This is to measure the time for computeScore
    time2 = 0
    for v in L:
        print(v)
        # if(v == L[-1]):
        #    continue
        v = v[0]
        #no refintment queries
        if v == '[]':
            ans =  S.check()
            if ans == True:
                return 1
            else:
                return 0
        vv = Naive.node2list(v)
        if all(val == 0 for val in vv):
            start = time.time()
            dic_v = updateMemoTable(S.df, S, atts)
            end1 = time.time()
            # first to compute the weight for the original statment
            Q_count = GetFromGMByAttribute(S.Q.cond, dic_v)
            table[str(vv)] = dic_v
        else:
            vc = getChildnode(vv)
            dic_c = table[str(vc)]
            changed_att, prev_att = getChangedAtt(vc, heirerchy, vv)
            table[str(vv)] = updateMemoTableFromChild(str(prev_att), dic_c)
        A = Naive.get_grouping_att(v, mapping, heirerchy)
        R = Naive.get_refinetment_expressions(v, mapping, heirerchy, S.df)

        ### for parallel
        if par:
            cpu_number = 1#cpu_count()
        else:
            cpu_number = 1
        Rs = np.array_split(R, cpu_number)
        manager = Manager()
        scores = manager.list()
        pool = Pool(processes=cpu_number)
        for Ri in Rs:
            pool.apply_async(ComputeScore, (Ri, S.Q, Q_count, table[str(vv)], A, S, scores))
        pool.close()
        pool.join()
        score = score + sum(scores)
        ## for non-parallel
        # scores = []
        # start_t = time.time()
        # ComputeScore(R, S.Q, Q_count, table[str(vv)], A, S, scores, inverted_index)
        # end_t = time.time()
        # time2 += end_t - start_t
        # print("The time for Compute Score is")
        # print(time2)
        # score = score + sum(scores)
        # end = time.time()
    # print("The currrent set of inverted index is")
    #inverted_index.ShowResult()
    #memo = inverted_index.ShowResult()
    memo = 0
    score = score/(node_num-1)
    print("The score is:")
    print(score)
    return memo, score




def getChildnode(vv):
    ans = [i for i in vv]
    for i in range(len(ans)):
        if not ans[i] == 0:
            ans[i] = ans[i] - 1
            return ans


def updateMemoTable(df, S, atts):
    # get the columns containing the distinct atts value
    #count agg
    if S.Q.target == "":
        counts = df.groupby(atts).size()
        agg = df.groupby(atts).size()
        agg_sum = df.groupby(atts).size()
    #mean agg
    else:
        counts = df.groupby(atts)[S.Q.target].count()
        agg = df.groupby(atts)[S.Q.target].mean()
        agg_sum = df.groupby(atts)[S.Q.target].sum()
    dic_v = pd.concat([counts, agg, agg_sum], axis=1)
    dic_v.columns = ["count", "avg", "sum"]
    #print(dic_v)
    return dic_v


def getChangedAtt(vc, heirerchy, vv):
    for i in range(len(vc)):
        if not vc[i] == vv[i]:
            chnaged_att = heirerchy[i][vv[i]]
            prev_att = heirerchy[i][vc[i]]
            break
    return chnaged_att, prev_att