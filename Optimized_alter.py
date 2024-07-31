import Naive
import Shared
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
from Alternative import Alternative
from multiprocessing.managers import BaseManager
pd.set_option('display.max_rows', None)



def GetScoreFromChildren(cond, count, dic_c):
    # input is the partition query cond, weight for the original query and the child table
    new_dic = dic_c.reset_index()
    if len(cond) != 0:
        for key in cond:
            value = cond[key]
            new_dic = new_dic[(new_dic[key] == value)]
        new_dic['temp'] = new_dic['score'] * new_dic['count']/count
        return new_dic['temp'].sum()
    else:
        new_dic['temp'] = new_dic['score'] * new_dic['count']/count
        return new_dic['temp'].sum()

def GetSupportMissed(cond, count, missed, table):
    sum_value = 0
    for m in missed:
        dic = table[str(m)]
        reset_dic = dic.reset_index()
        for key in cond:
            value = cond[key]
            reset_dic = reset_dic[(reset_dic[key] == value)]
        reset_dic['temp'] = reset_dic['support'] * reset_dic['count']/count
        sum_value += reset_dic['temp'].sum()
    return sum_value


def UpdateScore(dic, cond, score):
    new_dic = dic.reset_index()
    update_dic = dic.reset_index()
    for key in cond:
        value = cond[key]
        new_dic = new_dic[(new_dic[key] == value)]
    indexlist = new_dic.index.tolist()
    for index in indexlist:
        update_dic.loc[index, ['score']] = score
    cols = [i for i in update_dic.columns if i not in ['count', 'avg', 'sum', 'score', 'support']]
    dic = update_dic.set_index(cols, drop = True, append = False)
    return dic


def UpdateSupport(dic, cond, support):
    new_dic = dic.reset_index()
    update_dic = dic.reset_index()
    for key in cond:
        value = cond[key]
        new_dic = new_dic[(new_dic[key] == value)]
    indexlist = new_dic.index.tolist()
    for index in indexlist:
        update_dic.loc[index, ['support']] = support
    cols = [i for i in update_dic.columns if i not in ['count', 'avg', 'sum', 'score', 'support']]
    dic = update_dic.set_index(cols, drop = True, append = False)
    return dic


# multi-process
def ComputeScore(R, Q, Q_count, dic, A, S, dic_c, alternative, threadshold, table, node, missed, node_num):
    # the dic is its own dic
    score = 0
    for r in R:
        att = [a for a in Q.att]
        for a in A:
            if not a in att:
                att.append(a)
        cond = {**Q.cond, **r}
        weight = get_weight(Q_count, cond, table[node])
        support = 0
        if weight > 0:
            QQ = PartitionQuery(att, cond, Q.target)
            support = get_support(QQ, table[node], S.groups)
            table[node] = UpdateSupport(table[node], cond, support)
            # get a set of children scores
            if dic_c is None:
                # when this node is in the bottom layer
                score =  support
            else:
                score = support + GetScoreFromChildren(cond, weight*Q_count, dic_c)
        else:
            score = 0
        table[node] = UpdateScore(table[node], cond, score)
        if(score > 0):
            score = score + GetSupportMissed(cond, weight*Q_count, missed, table)
        if score/node_num > threadshold:
            alternative.Add(cond, att)



# to compute the counts given the condition
def GetFromGMByAttribute(cond, dic):
    # condition is a dictionary
    new_dic = dic.reset_index()
    for key in cond:
        value = cond[key]
        new_dic = new_dic[(new_dic[key] == value)]
    return new_dic['count'].sum()



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


def get_weight_for_current(cond_count, r, table):
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
    cols = [i for i in new_dic.columns if i not in ['count', 'avg', 'sum', 'score', 'support']]
    new_dic.set_index(cols, drop = True, append = False, inplace = True)
    return new_dic




def MergeStatement(sorted_groups, target):
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
    for_att_names = for_compare.reset_index()
    att_names = [i for i in for_att_names.columns if i not in ['count', 'avg', 'sum', 'support', 'group_identifier']]
    passed = [0] * len(sorted_groups) # to count how many groups have passed
    for index, row in for_compare.iterrows():
        # need to be updated: assume g1 is greater than g2, and do not
        if (row["group_identifier"] == 'g2'):
            if len(passed) == 2 or all(val != 0 for val in passed[2:]):
                passed[1] += 1
        if (row["group_identifier"] == 'g3'):
            if len(passed) == 3 or all(val != 0 for val in passed[3:]):
                passed[2] += 1
        if (row["group_identifier"] == 'g4'):
            if len(passed) == 4 or all(val != 0 for val in passed[4:]):
                passed[3] += 1
        if (row["group_identifier"] == 'g5'):
            if len(passed) == 5 or all(val != 0 for val in passed[5:]):
                passed[4] += 1
        if (row["group_identifier"] == 'g6'):
            if len(passed) == 6 or all(val != 0 for val in passed[6:]):
                passed[5] += 1
        if (row["group_identifier"] == 'g7'):
            if len(passed) == 7 or all(val != 0 for val in passed[7:]):
                passed[6] += 1
        if (row["group_identifier"] == 'g1'):
            correct += np.prod(passed[1:])
    return correct



def get_support(QQ, table, groups):
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
    numerator = MergeStatement(sorted_groups, QQ.target)
    supp = float(numerator) / (denominator)
    return supp


def get_score_optimized(G, S, mapping,heirerchy, atts,
                        space_opt = False, verbose = False, threadshold = 0.89):
    score = 0
    table = {}
    L = Naive.get_nodes_buttom_up(G)
    node_num = len(L)
    print("The number of the nodes is")
    print(node_num)
    print(atts)    
    alternative = Alternative(atts, S.df)
    for v in L:
        missed = getSetofMissed(v, L)
        v = v[0]
        print(v)
        #no refintment queries
        if v == '[]':
            ans =  S.check()
            if ans == True:
                return 1
            else:
                return 0
        vv = Naive.node2list(v)
        # layer to indicate if this is the bottom layer
        if all(val == 0 for val in vv):
            #start = time.time()
            dic_v = updateMemoTable(S.df, S, atts)
            # first to compute the weight for the original statment
            Q_count = GetFromGMByAttribute(S.Q.cond, dic_v)
            table[str(vv)] = dic_v
            layer = 0
        else:
            layer = 1
            vc = getChildnode(vv)
            dic_c = table[str(vc)]
            changed_att, prev_att = getChangedAtt(vc, heirerchy, vv)
            table[str(vv)] = updateMemoTableFromChild(str(prev_att), dic_c)
        A = Naive.get_grouping_att(v, mapping, heirerchy)
        R = Naive.get_refinement_expressions(v, mapping, heirerchy, S.df)
        if layer == 0:
            ComputeScore(R, S.Q, Q_count, table[str(vv)], A, S, None, alternative, threadshold, table, str(vv), missed, node_num)
        else:
            ComputeScore(R, S.Q, Q_count, table[str(vv)], A, S, dic_c, alternative, threadshold, table, str(vv), missed, node_num)
    score = table[str(vv)]
    memo = alternative.ShowResult()
    print("The final score is !!!")
    score_num = score.iloc[0]['score']+GetSupportMissed(S.Q.cond, len(S.df), missed, table)
    print(score_num/node_num)
    return memo, score_num/node_num



def updateMemoTableFromChild(prev_att,dic_v):
    # remove the first one and the last 5
    selected = list(dic_v.reset_index().head())[:-5]
    selected.remove(prev_att)
    # we can do this because according to the rule we always remove the first item in the tuple
    result = dic_v.groupby(selected)[["count", "sum"]].sum()
    result["avg"] = result["sum"] / result["count"]
    result = result[['count', 'avg', 'sum']]
    result['score'] = 0
    result['support'] = 0
    return result


def getChildnode(vv):
    ans = [i for i in vv]
    for i in range(len(ans)):
        if not ans[i] == 0:
            ans[i] = ans[i] - 1
            return ans


def updateMemoTable(df, S, atts):
    # get the columns containing the distinct atts value
    if S.Q.target == "":
        counts = df.groupby(atts).size()
        agg = df.groupby(atts).size()
        agg_sum = df.groupby(atts).size()
    else:
        counts = df.groupby(atts)[S.Q.target].count()
        agg = df.groupby(atts)[S.Q.target].mean()
        agg_sum = df.groupby(atts)[S.Q.target].sum()
    dic_v = pd.concat([counts, agg, agg_sum], axis = 1)
    dic_v.columns = ["count", "avg", "sum"]
    dic_v['score'] = 0
    dic_v['support'] = 0
    return dic_v



def getChangedAtt(vc, heirerchy, vv):
    for i in range(len(vc)):
        if not vc[i] == vv[i]:
            chnaged_att = heirerchy[i][vv[i]]
            prev_att = heirerchy[i][vc[i]]
            break
    return chnaged_att, prev_att



def getSetofMissed(v, L):
    # get a set of nodes that are not covered by the node vv
    layer = v[1]
    vv = Naive.node2list(v[0])
    candidate = []
    uncovered = []
    children = getSetofChildren(vv, layer)
    for node in L:
        if node[1] < layer:
            candidate.append(Naive.node2list(node[0]))
        else:
            break
    for c in candidate:
        isdominate = True
        if c in children:
            isdominate = False
        # here we aim to find a set of nodes that are dominated but not choose as children
        for i in range(len(vv)):
            if c[i] > vv[i]:
                isdominate = False
        if isdominate == True:
            uncovered.append(c)
    #print(uncovered)
    return uncovered



def getSetofChildren(vv, layer):
    children = []
    vc = vv
    while(True):
        layer = layer - 1
        if(layer >= 0):
            vc = getChildnode(vc)
            children.append(vc)
        else:
            break
    #print("The set of Children is")
    #print(children)
    return children
                                                                      


    
    
        
        
        