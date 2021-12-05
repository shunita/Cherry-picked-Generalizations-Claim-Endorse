import itertools
import networkx as nx
from PartitionQuery import PartitionQuery
from Statement import Statement
import time


def get_hesse_diag(gl, G = None):
    if G == None:
        G = nx.DiGraph()
    v = gl
    G.add_node(str(v), level = sum(gl))
    for i in range(len(v)):
        if v[i] > 0:
            v_c = v.copy()
            v_c[i] = v_c[i] -1
            G.add_node(str(v_c), level = sum(v_c))
            G.add_edge(str(v_c), str(v))
            G = get_hesse_diag(v_c, G)
    return G

def enumerate_generalization_levels(G):
    return list(list(nx.topological_sort(G)))


def get_nodes_buttom_up(G):
    nodes = []
    for node, data in G.nodes(data=True):
        nodes.append((node,data["level"]))
    return sorted(nodes, key=lambda x: x[1])



def node2list(v):
    v = v.strip("[")
    v = v.strip("]")
    v = v.split(",")
    v = [int(vv) for vv in v]
    return v

def get_grouping_att(v,mapping, heirerchy):
    A = []
    v = node2list(v)
    for i in range(len(v)):
        if not i in mapping:
            print("here")
        if mapping[i] == "grp":
            a = heirerchy[i][v[i]]
            if not a == "NI":
                A.append(a)
    return A


def get_refinetment_expressions(v, mapping, heirerchy, df):
    R = []
    temp = []
    v = node2list(v)
    for i in range(len(v)):
        if mapping[i] == "pred":
            a = heirerchy[i][v[i]]
            if not a == "NI":
                temp.append(a)

    dic = {}
    for t in temp:
        dic[t] = list(set(df[t].tolist()))
    vals = [v for k, v in dic.items()]
    if len(vals) == 0:
        R.append({})
        return R
    elif len(vals) == 1:
        for k, v in dic.items():
            for vv in v:
                r = {}
                r[k] = vv
                R.append(r)

    else:
        for element in itertools.product(*vals):
            keys = list(dic.keys())
            r= {}
            for i in range(len(keys)):
                r[keys[i]] = element[i]
            R.append(r)

    return R



def get_weight(S, QQ,A, verbose = False):
    Q = S.Q
    df = S.df
    groups = S.groups

    df1 = df.copy()
    for k, v in Q.cond.items():
        df1 = df1[df1[k] == v]
    groups_Q = df1.groupby(Q.att)

    df1 = df.copy()
    for k, v in QQ.cond.items():
        df1 = df1[df1[k] == v]
    groups_QQ = df1.groupby(Q.att)


    ans_QQ = 0
    for n, g in groups_QQ:
        if n in groups.values():
            ans_QQ += len(g)


    ans_Q = 0
    for n, g in groups_Q:
        if n in groups.values():
            ans_Q += len(g)

    return float(ans_QQ)/ans_Q

def get_support(S, QQ):

    df = S.df
    # case 1: same attributes for grouping
    if QQ.att == S.Q.att:
        SS = Statement(QQ, S.f, df, S.groups)
        c = SS.check()
        if c == True:
            return 1
        else:
            return 0
    # case 2:
    else:
        A = list(set(QQ.att).difference(set(S.Q.att)))
        df2 = df.copy()
        for k, v in QQ.cond.items():
            df2 = df2[df2[k] == v]
        groups_Q = df2.groupby(S.Q.att)
        d = 1
        groups_QQ = []
        for n, g in groups_Q:
            groups_n = g.groupby(A)
            if n in S.groups.values():
                g_n = []
                for nn, gg in groups_n:
                    g_n.append(nn)
                groups_QQ.append(g_n)
                d *= len(groups_n)
        n = 0
        if len(groups_QQ) > 1:
            #assuming only one attribute for grouping
            for element in itertools.product(*groups_QQ):
                df3 = df2.copy()
                df3 = df3[df3[A[0]].isin(element)]
                if S.Q.target == "":
                    temp =  df3.groupby(S.Q.att)
                else:
                    temp = df3.groupby(S.Q.att)[S.Q.target].mean()
                listofgroups = []
                for i in range(len(S.groups)):
                    name = "g" + str(i+1)
                    listofgroups.append(S.groups[name])
                c = S.f(temp, listofgroups)
                if c == True:
                    n = n+1
        supp = float(n)/d
    return supp

def get_score_naive(G, S, mapping,heirerchy, verbose = False):
    #print("start naive")
    score = 0
    L = enumerate_generalization_levels(G)
    start = time.time()
    #print("num of nodes: ", len(L))
    count = 0
    for v in L:
        if v == '[]':
            ans =  S.check()
            if ans == True:
                return 1
            else:
                return 0
        A = get_grouping_att(v, mapping, heirerchy)
        R = get_refinetment_expressions(v, mapping, heirerchy, S.df)
        #print("Num of r exps: ", len(R))
        for r in R:
            Q = S.Q
            att = [a for a in Q.att]
            for a in A:
                if not a in att:
                    att.append(a)

            cond = {**Q.cond, **r}
            #we dont need A for the weight
            QQ = PartitionQuery(Q.att,cond,Q.target)
            weight = get_weight(S, QQ, A,False)
            if weight > 0:
                count = count + 1
            QQ = PartitionQuery(att, cond, Q.target)
            support = get_support(S, QQ)
            if(weight > 0  and support > 0):
                print(QQ, weight, support)
            score += weight*support
    end = time.time()
    print("finished!! Time:")
    print(end-start)
    print("The score is")
    print(score)


    return score


def count_queries(G, S, mapping,heirerchy, verbose = False):
    #print("start naive")
    score = 0
    L = enumerate_generalization_levels(G)
    #print("num of nodes: ", len(L))
    count = 0
    for v in L:
        if v == '[]':
            ans =  S.check()
            if ans == True:
                return 1
            else:
                return 0
        A = get_grouping_att(v, mapping, heirerchy)
        R = get_refinetment_expressions(v, mapping, heirerchy, S.df)
        count = count + len(R)



    return count

