import time, rlcompleter, readline, pandas as pd
from itertools import combinations, compress
class ARL:    
    def __init__(self, sql_string, n_vars):        
        readline.parse_and_bind("tab: complete")
        self.n_vars = n_vars
        self.data = spark.sql(sql_string)
    def support_calculation(self):        
        comb = lambda x: list(combinations(self.data.drop("documento", "segmento", "fraude").columns, x))
        variables = list(map(lambda x: list(x), comb(self.n_vars)))
        def iter_function(i):
            temp = self.data.select(variables[i])
            temp.cache()
            cols = list(map(lambda x: x + "==1", temp.columns))
            supp_X = list(map(lambda x: cols[x] + " and", range(len(cols))))
            supp_X = supp_X[:-1] + [supp_X[len(supp_X) - 1].replace(" and", "")]
            return (float(temp.where(" ".join(supp_X)).count()) / float(temp.count()))*100
        support = pd.concat([pd.DataFrame(list(map(lambda x : iter_function(x), \
                                                   range(len(variables)))), columns = ["support"]), \
                             pd.DataFrame(variables)], axis=1).sort_values(by=["support"], \
                                                                           ascending=False)
        return support[support["support"] > 0]
    def confidence_calculation(self, support):        
        comb = lambda x: list(combinations(self.data.drop("fraude", "segmento", "documento").columns, x))
        variables_full = list(map(lambda x: list(x), comb(self.n_vars)))
        variables_support = list(map(lambda x: list(support.iloc[x][1:]), range(support.shape[0])))
        indices_support = list(map(lambda x: variables_full[x] in variables_support, \
                                             range(len(variables_full))))
        variables_confidence = list(compress(variables_full, indices_support))
        variables_fraud = list(map(lambda x: x + ["fraude"], variables_confidence))
        def iter_function(i):
            temp = self.data.select(variables_fraud[i])
            temp.cache()
            cols = list(map(lambda x: x + "==1", temp.columns))
            supp_X = list(map(lambda x: cols[x] + " and", range(len(cols)-1)))
            supp_X = supp_X[:-1] + [supp_X[len(supp_X) - 1].replace(" and", "")]            
            return (float(temp.where("".join(list(map(lambda x: cols[x] + " and ", \
                                                     range(len(cols))))) + "fraude==1").count()) \
                  / float(temp.where(" ".join(supp_X)).count()))*100
        
        confidence = pd.concat([pd.DataFrame(list(map(lambda x: iter_function(x), \
                                                      range(len(variables_fraud)))), \
                                             columns=["confidence"]), pd.DataFrame(variables_fraud)], \
                               axis=1).sort_values(by=["confidence"], ascending = False)
        return confidence
