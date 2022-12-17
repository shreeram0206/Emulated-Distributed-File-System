from importlib.metadata import metadata
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt
from metadata import MongoMetadata
from mapreduce import MapReduce

"""An important consideration when working with the file system functions of this EDFS
    is that they require absolute path much like the Hadoop DFS"""

metadata = MongoMetadata()
mapred = MapReduce()

class SearchAnalytics:
    def __init__(self):
        # MongoDB database's connection string
        conn_str = "mongodb+srv://dsci551:dsci551@cluster0.eks9nfe.mongodb.net/test"
        client = MongoClient(conn_str)

        # Database
        db = client['EDFS_Directory']

        # Collection
        self.collection = db['DSCI551']

        # Azure MySQL database's connection string
        mysql_config = {
            'user': "shreeram@dsci551",
            'password': "dsci#551",
            'host': "dsci551.mysql.database.azure.com",
            'port': 3306
        }

        self.mysql_engine = create_engine(
            f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/dsci551")


    def dropdown(self, path):

        path_list = path.split("/")
        result = []
        if(path_list[-1] == 'stocks.csv'):
            with self.mysql_engine.connect() as connection:
                with connection.begin():
                    stock_names = connection.execute(text(f"select distinct Name from `{path_list[-1]}`;"))
                    stock_list = []
                    for stock in stock_names:
                        stock_list.append(''.join(map(str,stock)))
                    agg_method = ["avg", "min", "max"]
                    attributes = connection.execute(text(f"select column_name from information_schema.columns where table_name = '{path_list[-1]}';"))
                    attrib_list = []
                    for attrib in attributes:
                        attrib_list.append(''.join(map(str,attrib)))
                    attrib_list.remove("id")
                    attrib_list.remove("date")
                    attrib_list.remove("Name")
                    result.extend([stock_list, agg_method ,attrib_list])
                    return result

        elif(path_list[-1] == 'house.csv'):
            with self.mysql_engine.connect() as connection:
                with connection.begin():
                    state_names = connection.execute(text(f"select column_name from information_schema.columns where table_name = '{path_list[-1]}';"))
                    state_list = []
                    for state in state_names:
                        state_list.append(''.join(map(str,state)))
                    state_list.remove("id")
                    state_list.remove("Date")
                    agg_method = ["avg", "min", "max"]
                    result.extend([state_list, agg_method, ["None"]])
                    return result 

    def searchDataset(self, path, inputs):
        
        path_list = path.split("/")
        dataset = path_list[-1]
        reduced = []
        for partition in metadata.getPartitionLocations(path):
            reduced.append(mapred.map(self.mysql_engine, dataset, partition, inputs))
        result = []
        if dataset == "stocks.csv":
            result.append(("Year", inputs[2]))
        elif dataset == "house.csv":
            result.append(("Year", "Price(1000$)"))
        result.extend(list(map(list, mapred.reduce(reduced, inputs[1]))))
        return result

    def analyseDataset(self, dataset, identifier, stock_name=None):
        dataset = dataset.split("/")[-1]
        with self.mysql_engine.connect() as connection:
            with connection.begin():
                columns_result = connection.execute(text(f"select column_name from information_schema.columns where table_name = '{dataset}';"))
                if identifier == "analyze":
                    stock_names = connection.execute(text(f"select * from `{dataset}` limit 10000;"))
                elif identifier == "cat":
                    stock_names = connection.execute(text(f"select * from `{dataset}` limit 200;"))
                stock_list = []
                columns = []
                column_string = []
                for elem in columns_result:
                    column_string.append(elem[0]) # + " "
                columns.append(column_string[1:-1])
                for stock in stock_names:
                    stock = list(stock)[1:]
                    stock_list.append(stock)
                stock_list.insert(0, column_string[1:])
                if dataset == "stocks.csv" and identifier == 'analyze':
                    df = pd.DataFrame(stock_list[1:], columns=column_string[1:])
                    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
                    names_stocks = df['Name'].unique().tolist()
                    self.plotStocksData(df, stock_name)
                    return [names_stocks]
                else:
                    return stock_list    

    def plotStocksData(self, df, stock_name):
        if stock_name is not None:
            company_list = []
            company_list.append(stock_name)
            plt.figure(figsize=(20,12))
            for i, company in enumerate(company_list, 1):
                print(company)
                plt.subplot(2, 2, i)
                df_plot = df[df['Name'] == company]
                plt.plot(df_plot['date'], df_plot['close'], color='red')
                plt.xlabel("Year")
                plt.ylabel("Closing Prices")
                plt.legend(["{0}".format(company)])
                plt.title("Analyse closing price of all the stocks")
                plt.savefig("./images/{0}.png".format(company))

search = SearchAnalytics()
search.searchDataset("/usr/home/stocks.csv",["AAL","min","open"])