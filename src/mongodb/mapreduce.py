from sqlalchemy import text
from itertools import groupby
from operator import itemgetter
from statistics import mean

"""An important consideration when working with the file system functions of this EDFS
    is that they require absolute path much like the Hadoop DFS"""


class MapReduce:

    def map(self, engine, dataset, partition, inputs):

        result = {}
        if(dataset == 'stocks.csv'):
            stock_name = inputs[0]
            agg_method = inputs[1]
            attrib = inputs[2]
            with engine.connect() as connection:
                with connection.begin():
                    query_result = connection.execute(text(f"select substr(date,1,4), {agg_method}({attrib}) from `{dataset}` partition ({partition}) where Name = '{stock_name}' group by substr(date,1,4);"))
                    for row in query_result:
                        result[row[0]] = float(row[1])
            return result

        elif(dataset == 'house.csv'):
            state_name = inputs[0]
            agg_method = inputs[1]
            with engine.connect() as connection:
                with connection.begin():
                    query_result = connection.execute(text(f"select substr(date,1,4), {agg_method}(`{state_name}`) from `{dataset}` partition ({partition}) group by substr(date,1,4);"))
                    for row in query_result:
                        if row[1] != None:
                            result[row[0]] = float(row[1])
                return result
                        
    
    def reduce(self, results, agg_method):
        
        final_dict = results[0]
        for dictionary in results[1:]:
            if agg_method == "avg":
                final_dict =  {x: final_dict.get(x, 0) + dictionary.get(x, 0) for x in set(final_dict).union(dictionary)}
                final_dict = {x: final_dict[x]/len(results) for x in final_dict}
            elif agg_method == "min":
                final_dict =  {x: max(final_dict.get(x, 0),dictionary.get(x, 0)) for x in set(final_dict).union(dictionary)}
            else:
                final_dict =  {x: min(final_dict.get(x, 0),dictionary.get(x, 0)) for x in set(final_dict).union(dictionary)}
        return sorted(final_dict.items(), key= lambda item: item[0])

        
