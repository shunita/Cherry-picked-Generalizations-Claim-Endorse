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
    """
    @params G: Maybe - G is a directed graph representing the QRH lattice

    """
    return list(list(nx.topological_sort(G)))


def get_nodes_buttom_up(G):
    nodes = []
    for node, data in G.nodes(data=True):
        nodes.append((node,data["level"]))
    return sorted(nodes, key=lambda x: x[1])



def node2list(v):
    """
    @param v: Maybe- v represents a node in the QRH lattice as a string, e.g. '[0,0,1]'
    return: v as a list of ints, in the example above [0,0,1]
    """
    v = v.strip("[")
    v = v.strip("]")
    v = v.split(",")
    v = [int(vv) for vv in v]
    return v

def get_grouping_att(v, mapping, hierarchy):
    """
    attributes=["Gender","Role","Age"]
    example: v=[0,0,1]
    mapping: {0:pred,1:pred,2:grp}

    @param v: node in the lattice we are currently in
    @param mapping: a dictionary that maps from the attribute's index(0,1,2,....) to whether it is part of the predicate("pred") or the group by("grp")
    @param hierarchy: a dictionary that maps from the attribute's index(0,1,2,....) to the attribute's dimension hierarchy
    returns- a list of attributes of the group by in the refinement query
    """
    A = []
    v = node2list(v)
    for i in range(len(v)):
        if not i in mapping:
            print("here") #this is supposed to be an assert
        if mapping[i] == "grp":
            a = hierarchy[i][v[i]]
            if not a == "NI":
                A.append(a)
    return A


def get_refinement_expressions(v, mapping, hierarchy, df):
    """
        attributes=["Gender","Role","Age"]
        example: v=[0,0,1]
        mapping: {0:pred,1:pred,2:grp}
        hierarchy: {0:["Gender","NI"], 1:["Role","NI"] ,2:["Age","Age_over_35","NI"]}
        @param v: node in the lattice we are currently in
        @param mapping: a dictionary that maps from the attribute's index(0,1,2,....) to whether it is part of the predicate("pred") or the group by("grp")
        @param hierarchy: a dictionary that maps from the attribute's index(0,1,2,....) to the attribute's dimension hierarchy
        returns-all possible refinements for the where clause( Gender= Female and Role= Developer, ... )
        """
    R = []
    temp = []
    v = node2list(v)
    for i in range(len(v)):
        if mapping[i] == "pred":
            a = hierarchy[i][v[i]]
            if a != "NI":
                temp.append(a) #Why are we adding it to temp?

    #creating a mapping from attr -> all possible values
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
    """
    @param QQ: QQ is a refinement query, in relation to S.Q
    """
    Q = S.Q
    df = S.df
    if len(df) == 0:
        return 0    #If the df is empty, we can skip over this refinement

    #e.g. groups={'g1':'AL', 'g2': 'MD'}
    groups = S.groups

    df1 = df.copy()
    for k, v in Q.cond.items():
        #filtering df according to the original where clause
        df1 = df1[df1[k] == v]
    #grouping the df according to the original group by clause
    groups_Q = df1.groupby(Q.att)

    #At this point groups_Q is an evaluation of the original query, without aggregate function

    df1 = df.copy()
    for k, v in QQ.cond.items():
        df1 = df1[df1[k] == v]
    groups_QQ = df1.groupby(Q.att)
    # At this point groups_QQ is an evaluation of the refinement query, without aggregate function

    ans_QQ = 0
    for n, g in groups_QQ:
        #Going over only the subgroups that are used in the statement
        if n[0] in groups.values():
            ans_QQ += len(g)

    #if ans_QQ ==0 we can already return a weight of 0.
    ans_Q = 0
    for n, g in groups_Q:
        if n[0] in groups.values():
            ans_Q += len(g)

    return float(ans_QQ)/ans_Q

def get_support(S, QQ):
    """
    @param S: statement
    @param QQ: refined partition query
    """
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
        # A is The added group by attributes
        A = list(set(QQ.att).difference(set(S.Q.att)))
        df2 = df.copy()
        for k, v in QQ.cond.items():
            #Evaluating the where clause of the refined query
            df2 = df2[df2[k] == v]
        #The original group by
        groups_Q = df2.groupby(S.Q.att)
        #d is the denominator of the support
        d = 1
        groups_QQ = []

        if len(df2)>0:
            print("it's magic")
        # partition each of the original groups according to the new refined group by attributes
        for n, g in groups_Q:
            #g is one of the original groups, a subset of the df.

            groups_n = g.groupby(A)
            #using the refined group by attributes on this subset, g.
            if n[0] in S.groups.values():
                g_n = []
                #in this loop we are looking for the partitions of the statement groups, e.g. all cities in 'AL' and 'MD'
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

def get_score_naive(G, S, mapping, hierarchy, verbose = False):

    #print("start naive")
    score = 0
    sum_of_weights=0
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
        A = get_grouping_att(v, mapping, hierarchy)
        R = get_refinement_expressions(v, mapping, hierarchy, S.df)
        #print("Num of r exps: ", len(R))

        #Running over every possible where clause refinement
        for r in R:
            Q = S.Q
            att = [a for a in Q.att]    #getting a copy of the original group by attributes of the original query
            for a in A:
                if not a in att:
                    att.append(a) #Question- Why are we adding all of A to att instead of just subsets?

            cond = {**Q.cond, **r}
            #we dont need A for the weight
            QQ = PartitionQuery(Q.att,cond,Q.target)
            weight = get_weight(S, QQ, A,False)
            if weight > 0:
                count = count + 1
            #now we do need A for the support, so we need a different partition query
            QQ = PartitionQuery(att, cond, Q.target)
            support = get_support(S, QQ)
            if(weight > 0  and support > 0):
                print(QQ, weight, support)  #output print
            score += weight*support
            sum_of_weights+=weight
    normalized_score=score/sum_of_weights
    end = time.time()
    print("finished!! Time:")
    print(end-start)
    print("The score is")
    print(normalized_score)


    #return normalized_score
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
        R = get_refinement_expressions(v, mapping, heirerchy, S.df)
        count = count + len(R)



    return count

