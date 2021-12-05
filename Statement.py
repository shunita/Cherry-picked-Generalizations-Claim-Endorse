class Statement:

    def __init__(self, Q, f, df, groups):
        self.Q = Q
        self.f = f
        self.df = df
        self.groups = groups

    def __repr__(self):
        agg = self.Q.agg
        return "The "+agg +" "+str(self.Q.target) +" of "+str(self.groups["g1"]) +\
               " where: "+ str(self.Q.cond)+ " is grater than that of " +str(self.groups["g2"])

    def check(self):
        listofgroups = []
        for i in range(len(self.groups)):
            name = "g" + str(i+1)
            listofgroups.append(self.groups[name])
        return self.f(self.Q.app(self.df), listofgroups)
