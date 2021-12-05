import Naive
from PartitionQuery import PartitionQuery
import numpy as np
import pandas as pd
import copy
from multiprocessing import Pool, cpu_count, Manager
import sys
from sys import getsizeof, stderr
from itertools import chain
from collections import deque
import time

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
            support = get_support(QQ, dic, S.groups)
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


def get_weight(cond_count, r, table):
    # in this case, table is actually dic_v
    # cond is the r in this partition query and Q.cond is the pred in the original query
    g = cond_count
    g_r = 0
    # we cannot afford computing the g for sooo many times. It will takes a pretty long time.
    # I share the computation for g in each statement
    g_r = GetFromGMByAttribute(r, table)
    if g == 0:
        return 0
    weight = g_r / g
    return weight


# to return a set of aggregation values given the cond
def SubgroupAggSet(cond, dic, target):
    new_dic = dic.reset_index()
    for key in cond:
        value = cond[key]
        new_dic = new_dic[(new_dic[key] == value)]
    #count agg
    if target == "":
        return list(new_dic['count'])
    #avg agg
    return list(new_dic['avg'])


def MergeStatement(sorted_groups):
    # need to be extend to multiple groups
    for_compare = pd.DataFrame(columns=['value', 'group_identifier'])
    for g in sorted_groups:
        temp_df = pd.DataFrame(columns=['value', 'group_identifier'])
        temp_df["value"] = g[0]
        temp_df["group_identifier"] = g[1]
        for_compare = pd.concat([for_compare, temp_df], axis=0)
    for_compare.sort_values(by=["value"], ascending=True, inplace=True)
    correct = 0
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
        agg_g = SubgroupAggSet(cond, table, QQ.target)
        agg_g.sort()
        denominator = denominator * len(agg_g)
        if denominator == 0:
            return 0
        sorted_groups.append([agg_g, identifier])
    numerator = MergeStatement(sorted_groups)
    supp = float(numerator) / (denominator)
    return supp


def get_score_optimized(G, S, mapping, heirerchy, atts,
                        par=True, space_opt=False, tarck_mem = False,verbose=False):
    mem = 0
    score = 0
    table = {}
    L = Naive.get_nodes_buttom_up(G)
    level_to_nodes = {}
    curr_levels = []
    level_to_remove = None
    start = time.time()
    for node in L:
        v = node[0]
        print(v)
        level = node[1]
        if level in level_to_nodes:
            level_to_nodes[level].append(str(v))
        else:
            level_to_nodes[level] = [str(v)]
        if not level in curr_levels:
            if len(curr_levels) < 2:
                curr_levels.append(level)
            else:
                curr_levels.append(level)
                level_to_remove = curr_levels[0]
                curr_levels = curr_levels[1:]
        if space_opt:
            if not level_to_remove == None:
                for node in level_to_nodes[level_to_remove]:
                    if node in table:
                        del table[node]
        # no refintment queries
        if v == '[]':
            ans = S.check()
            if ans == True:
                return 1
            else:
                return 0
        vv = Naive.node2list(v)
        if all(val == 0 for val in vv):
            dic_v = updateMemoTable(S.df, S, atts)
            Q_count = GetFromGMByAttribute(S.Q.cond, dic_v)
            table[str(vv)] = dic_v
        else:
            vc = getChildnode(vv)
            dic_c = table[str(vc)]
            changed_att, prev_att = getChangedAtt(vc, heirerchy, vv)
            table[str(vv)] = updateMemoTableFromChild(str(prev_att), dic_c,S.Q.target)
        A = Naive.get_grouping_att(v, mapping, heirerchy)
        R = Naive.get_refinetment_expressions(v, mapping, heirerchy, S.df)
        # use the number of cores as the number of processes
        if par:
            cpu_number = 8#cpu_count()
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
        if tarck_mem:
            mem = max(mem, total_size(table))
    if tarck_mem:
        return mem
    end = time.time()
    print("finished!! Time:")
    print(end-start)
    print("The score is:")
    print(score)    
    return score


def updateMemoTableFromChild(prev_att, dic_v, target):
    # remove the first one and the last 3
    selected = list(dic_v.reset_index().head())[:-3]
    selected.remove(prev_att)
    # we can do this because according to the rule we always remove the first item in the tuple
    print(selected)
    result = dic_v.groupby(selected)[["count", "sum"]].sum()
    #count agg
    if target == "":
        result["avg"] = result["sum"]
    #avg agg
    else:
        result["avg"] = result["sum"] / result["count"]
    result = result[['count', 'avg', 'sum']]
    return result


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
    return dic_v


def getChangedAtt(vc, heirerchy, vv):
    for i in range(len(vc)):
        if not vc[i] == vv[i]:
            chnaged_att = heirerchy[i][vv[i]]
            prev_att = heirerchy[i][vc[i]]
            break
    return chnaged_att, prev_att


def total_size(o, handlers={}, verbose=False):
    """ Returns the approximate memory footprint an object and all of its contents.
    Automatically finds the contents of the following builtin containers and
    their subclasses:  tuple, list, deque, dict, set and frozenset.
    To search other containers, add handlers to iterate over their contents:
        handlers = {SomeContainerClass: iter,
                    OtherContainerClass: OtherContainerClass.get_elements}
    """
    dict_handler = lambda d: chain.from_iterable(d.items())
    all_handlers = {tuple: iter,
                    list: iter,
                    deque: iter,
                    dict: dict_handler,
                    set: iter,
                    frozenset: iter,
                   }
    all_handlers.update(handlers)     # user handlers take precedence
    seen = set()                      # track which object id's have already been seen
    default_size = getsizeof(0)       # estimate sizeof object without __sizeof__

    def sizeof(o):
        if id(o) in seen:       # do not double count the same object
            return 0
        seen.add(id(o))
        s = getsizeof(o, default_size)

        if verbose:
            print(s, type(o), repr(o), file=stderr)

        for typ, handler in all_handlers.items():
            if isinstance(o, typ):
                s += sum(map(sizeof, handler(o)))
                break
        return s

    return sizeof(o)