import Naive
from PartitionQuery import PartitionQuery
import Optimized_new
import time
import numpy as np





def get_exp_group(v, mapping, heirerchy, row,A,groups, att, target):
    temp = []
    v = Naive.node2list(v)
    for i in range(len(v)):
        if mapping[i] == "pred":
            a = heirerchy[i][v[i]]
            if not a == "NI":
                temp.append(a)
    r = {}
    for a in temp:
        r[a] = row[a]
    #print(A, r)
    g = row[att[0]]
    groups = list(groups.values())
    if not g in groups:
        g = None
    else:
        g= {}
        for a in A:
            g[a] = row[a]
    tar = 1
    if not target == "":
        tar = row[target]

    return r, g, tar


def get_dic_weights(dic_R, S,ans_Q):
    dic_weights = {}
    for k, v in dic_R.items():
        dic_weights[k] = float(v)/ans_Q
    return dic_weights


def get_score_shared(G, S, mapping,heirerchy):
        score = 0
        df = S.df
        L = Naive.enumerate_generalization_levels(G)
        for v in L:
            if v == '[]':
                ans = S.check()
                if ans == True:
                    return 1
                else:
                    return 0
            dic_v = Optimized.updateMemoTable(S, df, heirerchy, mapping, v)
            A = Naive.get_grouping_att(v, mapping, heirerchy)
            R = Naive.get_refinetment_expressions(v, mapping, heirerchy, S.df)
            Rs = np.array_split(R, 1)
            cc = 0
            threads = []
            scores = [0] * 1
            for Ri in Rs:
                thread1 = Optimized.myThread(cc, Ri, S.Q, dic_v, A, S, scores)
                # Start new Threads
                thread1.start()
                threads.append(thread1)
                cc = cc + 1
            for thread1 in threads:
                thread1.join()
            for sc in scores:
                score = score + sc

        return score

def get_score_shared_old(G, S, mapping,heirerchy):
    score = 0
    df = S.df
    L = Naive.enumerate_generalization_levels(G)
    for v in L:
        A = Naive.get_grouping_att(v, mapping, heirerchy)
        A = A + S.Q.att
        dic_R = {}
        ans_Q = 0

        for index, row in df.iterrows():
            r,g, tar = get_exp_group(v, mapping, heirerchy, row, A, S.groups,S.Q.att, S.Q.target)
            if not g == None:
                dic_R[str(r)] = dic_R.get(str(r),0)+1
                ans_Q = ans_Q + 1
        dic_weights = get_dic_weights(dic_R, S, ans_Q)

        Q = S.Q
        att = [a for a in Q.att]
        for a in A:
            if not a in att:
                att.append(a)
        for r, weight in dic_weights.items():
            #print(r)
            r = eval(r)
            cond = {**Q.cond, **r}
            QQ = PartitionQuery(att, cond, Q.target)
            support = Naive.get_support(S, QQ)
            #print(QQ, weight, support)
            score += weight*support

    return score