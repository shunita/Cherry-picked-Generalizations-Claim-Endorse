import pandas as pd
import numpy as np
import time
import os

class CounterArgument:

    def __init__(self, attributes, df, output_path=None, start_time=None, grp_attr=None):
        # initialized the counter argument columns
        # format of column names: 'attribute:value'
        self.attributes = attributes
        self.df = df
        cols = []
        for att in self.attributes:
            values = df[att].unique()
            for v in values:
                cols.append(str(att) + ':' + str(v))
        cols.append('in_result')
        self.inverted_index = pd.DataFrame(columns = cols, dtype = bool)
        self.output_path = output_path
        if self.output_path is not None:
            if not os.path.exists(self.output_path):
                with open(self.output_path, "w") as out:
                    out.write("Counter argument,Time\n")
        self.start_time = start_time
        self.grp_attr = grp_attr
        if self.grp_attr is None:
            self.grp_attr = "BLABLABLA"
        

    def RemoveChildrenSingle(self, ca, g1_index, g2_index):
        self.inverted_index['temp'] = True
        for col in ca:
            self.inverted_index['temp'] = self.inverted_index[col] & self.inverted_index['temp']
        self.inverted_index['in_result'] = self.inverted_index['in_result'] & ~self.inverted_index['temp']


    def RemoveChildren(self):
        cols = list(self.inverted_index)
        for index, row in self.inverted_index.iterrows():
            
            self.inverted_index['temp'] = True
            for col in cols:
                if(row[col] == True):
                    self.inverted_index['temp'] = self.inverted_index[col] & self.inverted_index['temp']
            self.inverted_index['in_result'] = self.inverted_index['in_result'] & ~self.inverted_index['temp']
            self.inverted_index.loc[index, 'in_result'] = True


    def Add(self, att_names, g1_index, CA_list):
        # first get the column name  
        for g2_index in CA_list:
            ca = []
            for i in range(len(att_names)):
                if (type(g1_index) != tuple):
                    ca.append(str(att_names[i]) + ':' + str(g1_index))
                    ca.append(str(att_names[i]) + ':' + str(g2_index))
                else:
                    ca.append(str(att_names[i]) + ':' + str(list(g1_index)[i]))
                    ca.append(str(att_names[i]) + ':' + str(list(g2_index)[i]))
            ca = list(set(ca))
            # remove all children
            # self.RemoveChildrenSingle(ca, g1_index, g2_index)
            # add the counter arguement into the result set
            insert = pd.Series(np.zeros(self.inverted_index.shape[1], dtype=bool), index = self.inverted_index.columns)
            for col in ca:
                insert[col] = True
            insert['in_result'] = True
            # also output to a file with predicates
            with open(self.output_path, "a") as out:
                ca_preds_only = sorted([x for x in ca if not x.startswith(self.grp_attr)], key=lambda kv: kv.split(":")[0])
                out.write(f'"{ca_preds_only}",{time.time()-self.start_time}\n')
            self.inverted_index = self.inverted_index.append(insert, ignore_index = True)

    def ShowResult(self):
        memory = self.inverted_index.memory_usage().sum()/(1024**2)
        # remove all dominated patterns in one pass
        self.RemoveChildren()
        print("The set of Counter Arguments are:")
        result = self.inverted_index.loc[self.inverted_index['in_result']==True]
        print(result)
        #self.ShowLable(result)
        #self.ShowDistribute(result)
        #return memory consumption in MB
        return memory

    # return the predicate of the children
    def ShowOne(self, result, id):
        explore = result.loc[id]
        predicate = []
        for c in list(result.columns):
            if explore.loc[c] == True:
                predicate.append(c)
        print(predicate)

    def ShowLable(self, result):
        for index, row in result.iterrows():
            predicate = []
            for c in list(result.columns):
                if row.loc[c] == True:
                    predicate.append(c)
            # predicate.remove('Age-Group:Middle-Aged')
            # predicate.remove('Age-Group:Young Adult')
            predicate.remove('in_result')
            print(predicate)
    
    def ShowDistribute(self, result):
        count = []
        for index, row in result.iterrows():
            predicate = []
            for c in list(result.columns):
                if row.loc[c] == True:
                    predicate.append(c)
            # predicate.remove('Age-Group:Middle-Aged')
            # predicate.remove('Age-Group:Young Adult')
            predicate.remove('in_result')
            count.append(len(predicate)-1)
        print(count)


