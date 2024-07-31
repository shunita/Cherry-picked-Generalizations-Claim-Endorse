class PartitionQuery:

    def __init__(self, att, cond, target,agg = 'mean'):
        self.att = att
        self.cond = cond
        self.target = target
        self.agg = agg

    def app(self,df):
        for k,v in self.cond.items():
            df = df[df[k] == v]
        #print(df)
        if self.target == "":
            groups =  df.groupby(self.att)
            #groups = df.groupby(["raceethnicity"])
            return groups.size()
        #groups =  df.groupby(self.att)[self.target].mean()#.agg([self.agg])
        groups =  df.groupby(self.att)[self.target].agg(self.agg)
        return groups

    def __repr__(self):
        return "att: "+str(self.att) +" cond: "+ str(self.cond) + " target: " +str(self.target)
