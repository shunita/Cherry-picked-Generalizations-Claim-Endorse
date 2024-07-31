import pandas as pd
import Naive
import Shared
import Optimized_new as Optimized
import Optimized_CA_new
import Optimized_alter
from PartitionQuery import PartitionQuery
from Statement import Statement
import time
from itertools import chain, combinations
import sys
import numpy
from tkinter import _flatten
import tracemalloc
import random
# import datacube
from sqlalchemy import create_engine
import time
import itertools

START_TIME = time.time()

def get_hierarchy(selected, ignore ="IGNORE"):
    ans= {}
    i = 0
    for s in selected:
        if len(s) == 1:
            #if ignores[0] in s or ignores[1] in s or ignores[2] in s:
            if ignore in s:
                continue
            else:
                ss = [item for item in s]
                ss.append("NI")
        else:
            if ignore in s:
                pos = s.index(ignore)
                ss = s[:pos]
                print(ss)
                ss.append("NI")
            else:
                ss = [item for item in s]
                ss.append("NI")
        ans[i] = ss
        i = i + 1
    return ans


def f_so_statment1(groups, Currentgroups):
    """
    @param groups: query result, like a dictionary from group to a numeric value
    @param Currentgroups: the identifiers(a list of strings) of the groups
    """
    for g in Currentgroups:
        if not g in groups:
            return False
    current = True
    for i in range(len(Currentgroups) - 1):
        if (groups[Currentgroups[i]] < groups[Currentgroups[i + 1]]):
            current = False
    return current


def so_statment_3(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore="Country")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)

    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["Country"], {}, "ConvertedSalary", 'mean')

    S = Statement(Q, f_so_statment1, df, {"g1": "United States", "g2": "India"})
    S.string = "Indian programmers are paid less salaries than programmers from the US."
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)


def so_statment_2(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore="UndergradMajorCS")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)

    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)
    Q = PartitionQuery(["UndergradMajorCS"], {}, "ConvertedSalary", 'mean')
    S = Statement(Q, f_so_statment1, df, {"g1": "CS", "g2": "Non CS"})
    S.string = "Programmers whose undergraduate major was computer science earn" \
               " twice as much as those whose major was non-CS, on average."
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)


def so_statment_1(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore ="Gender")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)


    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["Gender"], {}, "ConvertedSalary", 'mean')

    S = Statement(Q, f_so_statment1, df, {"g1": "Male", "g2": "Female"})
    S.string = "Female programmers are paid less salaries than male programmers, globally."
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)

def pk_statment_1(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore ="raceethnicityW")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)


    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["raceethnicityW"], {}, "", 'count')

    S = Statement(Q, f_so_statment1, df, {"g1": "Non White", "g2": "White"})
    S.string = "The majority of pollice killings in U.S. has taken away the lives of non white people."

    ans = S.check()
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)

def usc_statment_1(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore ="SEX")
    print("hhhhhhhhh")
    print(hierarchy)
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)


    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["SEX"], {}, "INCOME", 'mean')

    S = Statement(Q, f_so_statment1, df, {"g1": 0, "g2": 1})
    #S.string = "The majority of pollice killings in U.S. has taken away the lives of non white people."
    S.string = ""
    ans = S.check()
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)

def usc_statment_2(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore ="CITIZEN")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)


    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["CITIZEN"], {}, "INCOME", 'mean')

    S = Statement(Q, f_so_statment1, df, {"g1": 0, "g2": 1})
    #S.string = "The majority of pollice killings in U.S. has taken away the lives of non white people."
    S.string = ""
    ans = S.check()
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)


def usc_statment_3(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore ="IMMIGR")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)


    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["IMMIGR"], {}, "INCOME", 'mean')

    S = Statement(Q, f_so_statment1, df, {"g1": 1, "g2": 0})
    #S.string = "The majority of pollice killings in U.S. has taken away the lives of non white people."
    S.string = ""
    ans = S.check()
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)

def pk_statment_2(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore ="gender")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"
    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)


    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["gender"], {}, "", 'count')

    S = Statement(Q, f_so_statment1, df, {"g1": "Male", "g2": "Female"})
    S.string = "The majority of pollice killings in U.S. has taken away the lives of males."

    ans = S.check()
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)


def pk_statment_3(selected, file, df, algo, count, memo = False):
    hierarchy = get_hierarchy(selected, ignore ="county_bucket")
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)


    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)

    Q = PartitionQuery(["county_bucket"], {}, "", 'count')

    S = Statement(Q, f_so_statment1, df, {"g1": 1, "g2": 5})
    S.string = "low-income areas are more likely to have police killings than high-income areas."

    ans = S.check()
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count)
    else:
        run_exp(G, S, algo, atts, file, hierarchy, mapping, count)


def run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, count):

    if algo == "Optimized":
        #tracemalloc.start()
        t = Optimized.get_score_optimized(G, S, mapping, hierarchy, atts, track_mem= True)
        #current, peak = tracemalloc.get_traced_memory()
        #t = peak / 10 ** 6,
        #tracemalloc.stop()
    elif algo == "Memo":
        #tracemalloc.start()
        t = Optimized.get_score_optimized(G, S, mapping, hierarchy, atts, space_opt = True,
                                          track_mem= True)
        #current, peak = tracemalloc.get_traced_memory()
        #t = peak / 10 ** 6
        #tracemalloc.stop()
    if not algo == "":
        print(str(count)+"\t"+ str(t) + "\t" + algo + "\t" + str(atts))
        file.write(str(count)+"\t"+ str(t) + "\t" + algo + "\t" + str(atts) + "\n")
        file.flush()


def run_exp(G, S, algo, atts, file, hierarchy, mapping, count):
    if algo == "Naive":
        start = time.time()
        score = Naive.get_score_naive(G, S, mapping, hierarchy, False)
        end = time.time()
        memo = 0
        t = end - start
    if algo == "Count":
        start = time.time()
        score = Naive.count_queries(G, S, mapping, hierarchy, False)
        end = time.time()
        memo = 0
        t = end - start
        print("num of ref queries:", score)
    elif algo == "Shared":
        start = time.time()
        score = Shared.get_score_shared(G, S, mapping, hierarchy)
        end = time.time()
        memo = 0
        t = end - start
    elif algo == "Optimized":
        start = time.time()
        score = Optimized.get_score_optimized(G, S, mapping, hierarchy, atts)
        end = time.time()
        memo = 0
        t = end - start
    elif algo == "No-par":
        start = time.time()
        score = Optimized.get_score_optimized(G, S, mapping, hierarchy, atts, parallelized=False)
        end = time.time()
        memo = 0
        t = end - start
    elif algo == "Counter":
        start = time.time()
        memo, score = Optimized_CA_new.get_score_optimized(G, S, mapping, hierarchy, atts, start_time=START_TIME)
        end = time.time()
        t = end - start
    elif algo == "Alter":
        start = time.time()
        memo, score = Optimized_alter.get_score_optimized(G, S, mapping, hierarchy, atts)
        end = time.time()
        t = end - start
    elif algo == "Memo":
        start = time.time()
        score = Optimized.get_score_optimized_memo(G, S, mapping, hierarchy)
        end = time.time()
        t = end - start
    elif algo == "Cube":
        start = time.time()
        memo, score = datacube.get_score_naive(G, S, mapping, hierarchy, atts, False)
        end = time.time()
        t = end - start
    if not algo == "":
        print(str(score)+"\t"+str(count)+"\t"+ str(t) + "\t" + algo + "\t" + str(atts))
        file.write(str(count)+"\t"+ str(memo) + "\t" + str(score) + "\t" + str(t) + "\t" + algo + "\t" + str(atts) + "\n")
        file.flush()


def num_of_groups_so(df,file,algo, memo = False):
    selected = [['Student'],['Gender'], ['Age-Group'], ['Hobby'], ['Country'] ]
    hierarchy = get_hierarchy(selected, ignore="Continent")
    mapping = {}
    for k in hierarchy.keys():
        if k == list(hierarchy.keys())[-1]:
            mapping[k] = "grp"
        else:
            mapping[k] = "pred"
    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)
    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)
    # ========================================================
    # about country
    Q = PartitionQuery(["Continent"], {}, "ConvertedSalary", 'mean')
    conts = ["AF", "AS", "EU", "OC", "SA", "UNKNOWN"]
    for i in range(2, 7):
        curr = conts[0:i]
        groups = {}
        for j in range(len(curr)):
            groups["g"+str(j+1)] = curr[j]
        S = Statement(Q, f_so_statment1, df, groups)
        atts = selected
        atts = list(_flatten(atts))
        atts = list(set(atts).union(set(Q.cond)))
        atts = list(set(atts).union(set(Q.att)))
        if memo:
            run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, i)
        else:
            run_exp(G, S, algo, atts, file, hierarchy, mapping, i)

def num_of_groups_pk(df,file,algo, memo = False):
    selected = [['raceethnicity'], ['armed'],
                 ['lawenforcementagency_fixed'], ['nat_bucket'], ['gender'], ['cause'],
                ['city', 'state']]
    hierarchy = get_hierarchy(selected, ignore="state")
    print(hierarchy)
    mapping = {}
    for k in hierarchy.keys():
        if k == list(hierarchy.keys())[-1]:
            mapping[k] = "grp"
        else:
            mapping[k] = "pred"
    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)
    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)
    # ========================================================
    # about country
    Q = PartitionQuery(["state"], {}, "", 'count')
    conts = ["AL","MD","FL","CO","MI","IL","TX","OH","NC","OK"]
    for i in range(2, 11):
        curr = conts[0:i]
        groups = {}
        for j in range(len(curr)):
            groups["g"+str(j+1)] = curr[j]
        S = Statement(Q, f_so_statment1, df, groups)
        atts = selected
        atts = list(_flatten(atts))
        atts = list(set(atts).union(set(Q.cond)))
        atts = list(set(atts).union(set(Q.att)))
        if memo:
            run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, i)
        else:
            run_exp(G, S, algo, atts, file, hierarchy, mapping, i)


def num_of_groups_usc(df,file,algo, memo = False):
    selected = [ ['Available for Work'], ['CITIZEN'], ['MARITAL'], ['ENGLISH'],
                 ['SEX'],
                ['AGE', "AGE-GROUP"]]
    hierarchy = get_hierarchy(selected, ignore="AGE-GROUP")
    mapping = {}
    for k in hierarchy.keys():
        if k == list(hierarchy.keys())[-1]:
            mapping[k] = "grp"
        else:
            mapping[k] = "pred"
    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)
    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)
    # ========================================================
    # about country
    Q = PartitionQuery(["AGE-GROUP"], {}, "INCOME", 'mean')
    conts = ["Adult","Young","Young Adult","Middle-Aged","UNKNOWN"]
    for i in range(2, 7):
        curr = conts[0:i]
        groups = {}
        for j in range(len(curr)):
            groups["g"+str(j+1)] = curr[j]
        S = Statement(Q, f_so_statment1, df, groups)
        atts = selected
        atts = list(_flatten(atts))
        atts = list(set(atts).union(set(Q.cond)))
        atts = list(set(atts).union(set(Q.att)))
        if memo:
            run_exp_memo(G, S, algo, atts, file, hierarchy, mapping, i)
        else:
            run_exp(G, S, algo, atts, file, hierarchy, mapping, i)



def num_of_queries_so(df, file, algo, memo = False):
    atts = [["Hobby"], ["YearsCoding"], ["Dependents"], ["Student"],
            ["SexualOrientation"],["Age","Age-Group"],["Exercise"], ["HoursComputer"],
            ["RaceEthnicity"], ["FormalEducation"],["UndergradMajor","UndergradMajorCS"],
            ["EducationParents"],["Gender"],["DevType"], ["Country","Continent"]]

    for i in range(1, 10):
        selected = atts[0:i]
        so_statment_1(selected, file, df, algo, len(selected), memo)
        #so_statment_2(selected, file, df, algo, len(selected))
        #so_statment_3(selected, file, df, algo, len(selected))

def num_of_queries_pk(df, file, algo, memo = False):
    # atts = [ ["gender"],["raceethnicity","raceethnicityW"],
    #          ["month"], ["nat_bucket"], ["lawenforcementagency_fixed"],
    #         ["cause"],["armed"],["county_bucket"], ["age-decade", "age-group"],
    #          ["city","state"]]
# this is for statement2
    atts = [["raceethnicity","raceethnicityW"],
             ["month"], ["nat_bucket"], ["lawenforcementagency_fixed"],
             ["cause"],["armed"],["county_bucket"], ["age-decade", "age-group"],
             ["city","state"]]

    for i in range(1,len(atts)):
        selected = atts[0:i]
        pk_statment_3(selected, file, df, algo, len(selected), memo)
        #pk_statment_2(selected, file, df, algo, len(selected), memo)
        # pk_statment_3(selected, file, df, algo, len(selected), memo)


def num_of_queries_usc(df, file, algo, memo = False):
    atts = [ ["SEX"],[ "ENGLISH"],["Available for Work"],["CITIZEN"],["CLASS"],
             ["MARITAL"],["IMMIGR"],["Num of kids"]]
    # atts = [ ["SEX"],[ "ENGLISH"],["Available for Work"],["CLASS"],
    #          ["MARITAL"],["IMMIGR"],["Num of kids"]]
        #,["MEANS"],["YEARSCH"],
         #    ["STATE"]]
    print(df.columns)
    for i in range(1,len(atts)+1):
        selected = atts[0:i]
        usc_statment_1(selected, file, df, algo, len(selected), memo)
        # usc_statment_2(selected, file, df, algo, len(selected), memo)
        #usc_statment_3(selected, file, df, algo, len(selected), memo)


def data_size_usc(df, file, algo, memo = False):
    # here we fixed the number of refinement queries to 85536
    selected = [['CITIZEN'], ['IMMIGR'],
                ['Available for Work'], ['MARITAL'], ['ENGLISH'],
                ['SEX'], ['CLASS']]
    for i in numpy.arange(0.1,1.1,0.1):
        dfs = df.sample(frac=i, replace=False, random_state=1)
        #dfs = df
        count = len(dfs)
        engine = create_engine('postgresql://postgres:dbgroup@localhost/generalization')
        tablename = "usc3_" + str(count)
        dfs.to_sql(tablename, engine)
        usc_statment_3(selected, file, dfs, algo, count, memo)
        #pk_statment_2(selected, file, dfs, algo, count, memo)
        #so_statment_3(selected, file, dfs, algo, count)


def data_size_pk(df, file, algo, memo = False):
    # here we fixed the number of refinement queries to 7056

    selected = [['gender'], ['nat_bucket'], ['month'],['lawenforcementagency_fixed'],
                ['cause'], ["raceethnicity","raceethnicityW"],["armed"]]
    for i in numpy.arange(0.2,1.2,0.2):
        dfs = df.sample(frac=i, replace=False, random_state=1)
        count = len(dfs)
        #pk_statment_2(selected, file, dfs, algo, count, memo)
        #pk_statment_3(selected, file, dfs, algo, count)
        #engine = create_engine('postgresql://postgres:dbgroup@localhost/generalization')
        #tablename = "pk" + str(count)
        #dfs.to_sql(tablename, engine)
        pk_statment_1(selected, file, dfs, algo, count, memo)
    num = [1000,150000, 280000, 420000, 560000, 700000, 840000]
    for i in num:
        to_add = i -len(df)
        dic = {'gender':[], 'raceethnicity':[], 'month':[],
                   'city':[], 'state':[], 'cause':[],'armed':[],
                   'county_bucket':[], 'nat_bucket':[], 'age-decade':[],
                   'age-group':[],'lawenforcementagency_fixed':[], 'raceethnicityW':[]}
        for j in range(to_add):
            add_row_pk(dic)
        newdf = pd.DataFrame.from_dict(dic)
        dfs = df.append(newdf, ignore_index=True)
        count = len(dfs)
        # engine = create_engine('postgresql://postgres:dbgroup@localhost/generalization')
        # tablename = "pk3_" + str(count)
        #dfs.to_sql(tablename, engine)
        pk_statment_1(selected, file, dfs, algo, count, memo)


def add_row_so(dic):

    Hobby, Country,UndergradMajor,UndergradMajorCS, Gender, \
    RaceEthnicity, Age, Age_Group,Continent,ConvertedSalary = get_random_row_so()
    dic["Respondent"].append(1)
    dic["Hobby"].append(Hobby)
    dic["Country"].append(Country)
    dic["Student"].append("No")
    dic["FormalEducation"].append("Some college/university study without earning a degree")
    dic["UndergradMajor"].append(UndergradMajor)
    dic["UndergradMajorCS"].append(UndergradMajorCS)
    dic["DevType"].append("Full-stack developer")
    dic["YearsCoding"].append("24-26 years")
    dic["HoursComputer"].append("5 - 8 hours")
    dic["Exercise"].append("Daily or almost every day")
    dic["Gender"].append(Gender)
    dic["SexualOrientation"].append("Straight or heterosexual")
    dic["EducationParents"].append("Some college/university study without earning a degreeraight or heterosexual")
    dic["RaceEthnicity"].append(RaceEthnicity)
    dic["Age"].append(Age)
    dic["Dependents"].append("Yes")
    dic["Continent"].append(Continent)
    dic["Age-Group"].append(Age_Group)
    dic["ConvertedSalary"].append(ConvertedSalary)


def add_row_pk(dic):
    age_decade, age_group, raceethnicity, raceethnicityW, month, city, state, cause, armed, county_bucket, nat_bucket, gender = get_random_row_pk()
    dic["age-decade"].append(age_decade)
    dic["age-group"].append(age_group)
    dic["raceethnicity"].append(raceethnicity)
    dic["raceethnicityW"].append(raceethnicityW)
    dic["month"].append(month)
    dic["city"].append(city)
    dic["state"].append(state)
    dic["cause"].append(cause)
    dic["armed"].append(armed)
    dic["county_bucket"].append(county_bucket)
    dic["nat_bucket"].append(nat_bucket)
    dic["lawenforcementagency_fixed"].append("Police Department")
    dic["gender"].append(gender)



def get_random_row_so():
    Hobby = random.choice(["Yes", "No"])
    Country = random.choice(["United States", "India", "Sweden", "China"])
    if Country == "United States":
        Continent = "NA"
    elif Country == "India" or Country == "China":
        Continent = "AS"
    else:
        Continent = "EU"
    UndergradMajor = random.choice(["Computer science, computer engineering, or software engineering",
                                    "Mathematics or statistics"])
    if UndergradMajor == "Mathematics or statistics":
        UndergradMajorCS = "Non CS"
    else:
        UndergradMajorCS = "CS"
    Gender = random.choice(["Male", "Female"])
    RaceEthnicity = random.choice(["Black or of African descent","White or of European descent",
                                   "South Asian"]),
    Age = random.choice(["25 - 34 years old","35 - 44 years old","55 - 64 years old",
                         "Under 18 years old"])
    if Age == "55 - 64 years old" or Age == "35 - 44 years old":
        Age_Group = "Middle-Aged"
    elif Age == "25 - 34 years old":
        Age_Group = "Young Adult"
    else:
        Age_Group = "Young"
    ConvertedSalary = random.choice([9480,9540,46992,47004,69504,80521,95780,
                                     125000,125123,127000])
    return Hobby, Country,UndergradMajor,UndergradMajorCS, Gender, \
    RaceEthnicity, Age, Age_Group,Continent,ConvertedSalary

def get_random_row_pk():

    age_decade = random.choice(["age 10-20", "age 20-30",
                                "age 30-40", "age 40-50", "age 50-60",
                                "age 60-70", "age 70-80"])

    if age_decade == "age 10-20":
        age_group = "Teenager"
    elif age_decade == "age 20-30" or age_decade == "age 30-40":
        age_group = "Young"
    elif age_decade == "age 40-50" or age_decade == "age 50-60":
        age_group = "Middle-Aged"
    else:
        age_group = "Adult"
    raceethnicity = random.choice(["Native American", "Hispanic/Latino",
                                   "Black", "White"])
    if raceethnicity == "White":
        raceethnicityW = "White"
    else:
        raceethnicityW = "Non White"
    month = random.choice(["February", "March","April","January", "May"])
    city = "Millbrook"
    state = "AL"
    cause = random.choice(["Gunshot","Unknown", "Taser"])
    armed = random.choice(["No", "Yes"])
    county_bucket = random.choice([1,2,3,4,5])
    nat_bucket = random.choice([1,2,3,4,5])
    gender = random.choice(["Male", "Female"])
    return age_decade, age_group,raceethnicity,raceethnicityW,month,\
           city,state,cause,armed,county_bucket,nat_bucket, gender

def data_size_so(df, file, algo, memo = False):
    # here we fixed the number of refinement queries to 11040

    selected = [['Country'], ['Hobby'], ['Student'],['Age-Group']]
    #selected = [['Hobby'], ['Student']]
    for i in numpy.arange(0.1,1.1,0.1):
        dfs = df.sample(frac=i, replace=False, random_state=1)
        count = len(dfs)
        engine = create_engine('postgresql://postgres:dbgroup@localhost/generalization')
        tablename = "so3_" + str(count)
        dfs.to_sql(tablename, engine)
        # print(dfs)
        # print(tablename)
        so_statment_3(selected, file, dfs, algo, count, memo)
        #so_statment_2(selected, file, dfs, algo, count)
        #so_statment_3(selected, file, dfs, algo, count)
    num = [197710,296565,395420,494275,593130,691985,790840,889695,988550]
    for i in num:
        to_add = i - len(df)
        dic = {'Respondent': [], 'Hobby': [], 'Country': [],
               'Student': [], 'FormalEducation': [], 'UndergradMajor': [], 'UndergradMajorCS': [],
               'DevType': [], 'YearsCoding': [], 'HoursComputer': [],
               'Exercise': [], 'Gender': [], 'SexualOrientation': [],
               'EducationParents':[],'RaceEthnicity':[],'Age':[],'Dependents':[],
               'Continent':[],'Age-Group':[],'ConvertedSalary':[]}
        for j in range(to_add):
            add_row_so(dic)

        newdf = pd.DataFrame.from_dict(dic)
        dfs = df.append(newdf, ignore_index=True)
        count = len(dfs)
        engine = create_engine('postgresql://postgres:dbgroup@localhost/generalization')
        tablename = "so3_" + str(count)
        dfs.to_sql(tablename, engine)
        so_statment_3(selected, file, dfs, algo, count, memo)

def so_exp(ifile,ofile,algo):
    df = pd.read_csv(ifile)
    df = df.fillna("UNKNOWN")
    file = open(ofile, "w")
    #ex1 running times vs. num of refinement queries
    num_of_queries_so(df, file, algo)

    # ex2 running times vs. num of tuples
    #data_size_so(df, file, algo)

    # ex3 memory vs. num of tuples

    #num_of_groups_so(df, file, algo, memo =False)
    file.close()

def pk_exp(ifile,ofile,algo):
    df = pd.read_csv(ifile)
    df = df.fillna("UNKNOWN")
    file = open(ofile, "w")
    #ex1 running times vs. num of refinement queries
    #num_of_queries_pk(df, file, algo)

    # ex2 running times vs. num of tuples
    #data_size_pk(df, file, algo)

    # ex3 memory vs. num of tuples
    num_of_groups_pk(df, file, algo, memo=False)
    file.close()

def usc_exp(ifile,ofile,algo):
    df = pd.read_csv(ifile)
    df = df.fillna("UNKNOWN")
    file = open(ofile, "w")
    #ex1 running times vs. num of refinement queries
    #num_of_queries_usc(df, file, algo, memo=False)
    # ex2 running times vs. num of tuples
    #data_size_usc(df, file, algo, memo=False)

    num_of_groups_usc(df, file, algo, memo=False)

    file.close()

def run_algorithm(algorithm_string, selected, group_by_attr, output_path, df_path, target, aggr, g1, g2, maybe_df=None):
    #algo="Naive"
    memo=False
    num_attributes=len(selected)
    if maybe_df is None:
        if "SO" in df_path:
            df = read_so_with_first_value()
        else:
            df = pd.read_csv(df_path)
        print(f"Read {len(df)} rows from {df_path}.")
    else:
        df = maybe_df
        print("Using pre-read dataset")
    df = df.dropna(subset=[target, group_by_attr]).copy()
    df = df.fillna("UNKNOWN")
    file = open(output_path, "w")
    hierarchy = get_hierarchy(selected, ignore=group_by_attr)
    mapping = {}
    for k in hierarchy.keys():
        mapping[k] = "pred"

    g_level = []
    for k in hierarchy.keys():
        level = hierarchy[k].index("NI")
        g_level.append(level)

    # get Hesse diagram
    G = Naive.get_hesse_diag(g_level)
    Q = PartitionQuery([group_by_attr], {}, target, aggr)
    # Q = PartitionQuery([group_by_attr],{"YearsCode": "0.0-10.0"}, target, aggr)
    # More options: , {"PlatformWantToWorkWith":"AWS"},{"OrgSize" :"10,000 or more employees"}

    S = Statement(Q, f_so_statment1, df, {"g1": g1, "g2": g2})
    #TODO: Correct String below
    S.string = "This String needs to be corrected"
    # get a set of partition attributes (ly), do we need to ignore the gender?
    atts = selected
    atts = list(_flatten(atts))
    atts = list(set(atts).union(set(Q.cond)))
    atts = list(set(atts).union(set(Q.att)))

    if memo:
        run_exp_memo(G, S, algorithm_string, atts, file, hierarchy, mapping, num_attributes)
    else:
        run_exp(G, S, algorithm_string, atts, file, hierarchy, mapping, num_attributes)
    return df

def main(args):
    # input file (so.csv, police_killings.csv)
    ifile = args[1]
    # output file (e.g., SO_scalability_queries_naive.txt)
    ofile = args[2]
    #the algorithm to run: (Naive, Shared, Memo, Optimized, No-par)
    algo = args[3]
    # TODO: for counter examples, add here some more attributes
    #selected = [["Age"], ["LearnCode"]]  # ["DevType"],["ICorPM"],["Country"], ["EdLevel"], ["Employment"],["MainBranch"],["Country"]
    #selected = [['POBP'], ['SCIENGP'], ['MLPH'], ['HISP']] #, ['SCIENGRLP'], ['DDRS'], ['HINS2'], ['OCCP'], ['INDP'], ['MARHT']]
    #selected = [['SEX'], ['GCM'], ['ANC'], ['RAC1P'], ['NATIVITY'], ['MARHD'], ['YOEP'], ['PUBCOV'], ['INDP'], ['WAOB'], ['HINS7'], ['OCCP'], ['AGEP'], ['FHINS5C'], ['ESP'], ['LANP'], ['MARHW'], ['RAC3P'], ['DPHY'], ['DRAT'], ['MIG'], ['POWPUMA'], ['ANC2P'], ['DIVISION'], ['MLPCD'], ['PAOC'], ['SCH'], ['NAICSP_grouped'], ['LANX'], ['FOD1P'], ['RACAIAN'], ['DOUT'], ['JWMNP'], ['RACBLK'], ['RACSOR'], ['NWAV'], ['GCL'], ['MIGPUMA'], ['SFN'], ['FER']]
    selected = [['DEPARTURE_TIME'], ['SCHEDULED_DEPARTURE'], ['WHEELS_OFF']]

    #grp = "Gender"
    #grp = "EdLevel"
    #grp = "SEX"
    grp = "DAY_OF_WEEK"
    #target = "ConvertedCompYearly"
    #target = "PINCP"
    target = "DEPARTURE_DELAY"
    #aggr = "mean"
    aggr = "count"
    #aggr = "median"
    #g1 = "Man"
    #g2 = "Woman"
    #g1 = "Bachelor’s degree"
    #g2 = "Master’s degree"
    #g1 = 1
    #g2 = 2
    g1 = 1
    g2 = 6
    pairs = list(itertools.combinations(selected, 2))
    df = None
    for i, pair in enumerate(pairs):
        print(f"Working on {pair} ({i}/{len(pairs)})")
        pass_target = target
        if aggr == "count":
            pass_target = None
        df = run_algorithm(algorithm_string=algo, selected=pair, group_by_attr=grp, output_path=ofile, df_path=ifile,
                      target=target, aggr=aggr, g1=g1, g2=g2, maybe_df=df)
    #so_exp(ifile,ofile,algo)
    #pk_exp(ifile,ofile,algo)
    #usc_exp(ifile,ofile,algo)


def is_multivalue_attr(df, attr, split_str=";"):
    """
    :param df: the dataframe
    :param attr: the attribute we are checking
    :param split_str: the string we are delineating our values with
    :return: True if the attribute is a multivalue field, because it has values with the split_str between them
    """
    values = df[attr].unique()
    for v in values:
        if split_str in str(v):
            return True
    return False


def safe_split(s):
    if type(s) != str:
        return s
    return s.split(';')[0]


def read_so_with_first_value():
    df = pd.read_csv("datasets/SO_DBversion.csv", index_col=0)
    for col in df.columns:
        if is_multivalue_attr(df, col):
            df[col] = df[col].apply(safe_split)
    return df


if __name__ == '__main__':
    #args = sys.argv
    #main(["","datasets/SO_DBversion.csv", "results/SO_DBversion_output.txt", "Counter"])
    #main(["","datasets/Seven_States_grouped_discretized.csv", "results/ACS_output.txt", "Counter"])
    main(["","datasets/flights_with_airports_discretized_filtered.csv", "results/flights_output.txt", "Counter"])

