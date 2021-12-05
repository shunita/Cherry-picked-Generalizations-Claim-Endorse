import pandas as pd
import numpy as np

class Alternative:

    def __init__(self, attributes, df):
        # initialized the counter argument columns
        # format of column names: 'attribute:value'
        #attributes.remove(attribute_to_ignore)
        self.attributes = attributes.copy()
        self.df = df
        cols = []
        for att in self.attributes:
            values = self.df[att].unique()
            for v in values:
                cols.append(str(att) + ':' + str(v))
        cols.append('in_result')
        self.inverted_index = pd.DataFrame(columns = cols, dtype = bool)

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


    def Add(self, cond, att):
        # first get the column name     
        # remove all children
        # self.RemoveChildrenSingle(ca, g1_index, g2_index)
        # add the counter arguement into the result set
        print("Add!")
        insert = pd.Series(np.zeros(self.inverted_index.shape[1], dtype=bool), index = self.inverted_index.columns)
        alter = []
        for key in cond:
            value = cond[key]
            alter.append(str(key) + ':' + str(value))
        for a in att:
            alter.append(str(a))
        for col in alter:
            insert[col] = True
        insert['in_result'] = True
        self.inverted_index = self.inverted_index.append(insert, ignore_index = True)

    def ShowResult(self):
        memory = self.inverted_index.memory_usage().sum()/(1024**2)
        self.RemoveChildren()
        print("The set of alternative statements are:")
        result = self.inverted_index.loc[self.inverted_index['in_result']==True]
        print(result)
        self.ShowLable(result)        
        return memory
        #print(self.inverted_index.loc[self.inverted_index['in_result']==True])

    def ShowOne(self, id):
        explore = self.inverted_index.loc[id]
        print(explore)

    def ShowLable(self, result):
        for index, row in result.iterrows():
            predicate = []
            for c in list(result.columns):
                if row.loc[c] == True:
                    predicate.append(c)
            print(predicate)

