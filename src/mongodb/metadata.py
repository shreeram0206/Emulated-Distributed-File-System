from importlib.metadata import metadata
from pymongo import MongoClient
from sqlalchemy import create_engine, text
import pandas as pd
from os.path import exists

"""An important consideration when working with the file system functions of this EDFS
    is that they require absolute path much like the Hadoop DFS"""


class MongoMetadata:
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

    def mkdir(self, path):

        if path[0] != '/':
            return "Require absolute path starting from root"
        path_list = path.split("/")
        doc = self.collection.find({"root": {'$exists': 1}})
        head_itr = doc[0]['root']
        prev_val = head_itr
        for dir in path_list[1:]:
            if dir not in head_itr:
                head_itr[dir] = {}
            head_itr = head_itr[dir]
        self.collection.update_one({"root": {'$exists': 1}}, {
                                   "$set": {"root": prev_val}})
        return ["Success"]

    def ls(self, path):

        if path[0] != '/':
            return "Require absolute path starting from root"
        path_list = path.split("/")
        doc = self.collection.find({"root": {'$exists': 1}})
        head_itr = doc[0]['root']
        if path_list[-1] == '':
            return list(head_itr.keys())
        else:
            for dir in path_list[1:]:
                if dir not in head_itr:
                    return "Invalid path"
                head_itr = head_itr[dir]
            return list(head_itr.keys())

    def cat(self, path):

        if path[0] != '/':
            return "Require absolute path starting from root"
        path_list = path.split("/")
        if path_list[-1][-3:] != "csv":
            return "Invalid file type: Only .csv files allowed"
        doc = self.collection.find({"root": {'$exists': 1}})
        head_itr = doc[0]['root']
        for dir in path_list[1:-1:1]:
            if dir not in head_itr:
                return "Invalid file path"
            head_itr = head_itr[dir]
        if path_list[-1] not in head_itr.keys():
            return "File does not exist"
        else:
            rows = []
            with self.mysql_engine.connect() as connection:
                with connection.begin():
                    result = connection.execute(text(f"select * from `{path_list[-1]}` limit 200"))
                    columns = connection.execute(text(f"select column_name from information_schema.columns where table_name = '{path_list[-1]}';"))
            column_string = ""
            for elem in columns:
                column_string += elem[0] + " "
            rows.append(column_string[:-1])
            for entry in result:
                rows.append(' '.join(map(str, entry)))
            return rows

    def rm(self, path):

        if path[0] != '/':
            return "Require absolute path starting from root"
        path_list = path.split("/")
        doc = self.collection.find({"root": {'$exists': 1}})
        head_itr = doc[0]['root']
        prev_val = head_itr
        for dir in path_list[1:-1:1]:
            if dir not in head_itr:
                return "Invalid path"
            head_itr = head_itr[dir]
        if path_list[-1] not in head_itr.keys():
            return "File does not exist"
        else:
            del head_itr[path_list[-1]]
            self.collection.update_one({"root": {'$exists': 1}}, {
                "$set": {"root": prev_val}})
            return ["Success"]

    def put(self, input_path, output_path, partitions):

        # 1) Check input path
        if not input_path.endswith('.csv'):
            return "Invalid input file type: Only .csv files allowed"
        if not exists(input_path):
            return "Invalid input file path or File does not exist"

        # 2) Check output path
        if output_path[0] != '/':
            return "Require absolute output path starting from root"
        if output_path[-3:] != "csv":
            return "Invalid output file type: Only .csv files allowed"
        path_list = output_path.split("/")
        doc = self.collection.find({"root": {'$exists': 1}})
        head_itr = doc[0]['root']
        prev_val = head_itr
        for dir in path_list[1:-1:1]:
            if dir not in head_itr:
                return "Invalid output path"
            head_itr = head_itr[dir]

        # 3) Upload csv to MySQL
        df = pd.read_csv(input_path)
        df.to_sql(path_list[-1], self.mysql_engine, if_exists='replace', index=True, index_label="id")
        

        with self.mysql_engine.connect() as connection:
            with connection.begin():
                connection.execute(text(f"ALTER TABLE `{path_list[-1]}` PARTITION BY HASH(id) PARTITIONS {partitions};"))

        # 4) Update MongoDB EDFS
        file_dict = {}
        for idx in range(0,partitions):
            file_dict[f"P{idx}"] = f"p{idx}"
        head_itr[path_list[-1]] = file_dict
        self.collection.update_one({"root": {'$exists': 1}}, {
            "$set": {"root": prev_val}})
        return ["Success"]

    def getPartitionLocations(self, path):

        if path[0] != '/':
            return "Require absolute path starting from root"
        path_list = path.split("/")
        if path_list[-1][-3:] != "csv":
            return "Invalid file type: Only .csv files allowed"
        doc = self.collection.find({"root": {'$exists': 1}})
        head_itr = doc[0]['root']
        for dir in path_list[1:-1:1]:
            if dir not in head_itr:
                return "Invalid file path"
            head_itr = head_itr[dir]
        if path_list[-1] not in head_itr.keys():
            return "File does not exist"
        else:
            head_itr = head_itr[path_list[-1]]
            return list(head_itr.values())
        

    def readPartition(self, path, partition_num):
        
        if path[0] != '/':
            return "Require absolute path starting from root"
        path_list = path.split("/")
        if path_list[-1][-3:] != "csv":
            return "Invalid file type: Only .csv files allowed"
        doc = self.collection.find({"root": {'$exists': 1}})
        head_itr = doc[0]['root']
        for dir in path_list[1:-1:1]:
            if dir not in head_itr:
                return "Invalid file path"
            head_itr = head_itr[dir]
        if path_list[-1] not in head_itr.keys():
            return "File does not exist"
        else:
            rows = []
            with self.mysql_engine.connect() as connection:
                with connection.begin():
                    result = connection.execute(text(f"select * from `{path_list[-1]}` partition ({partition_num}) limit 200"))
                    columns = connection.execute(text(f"select column_name from information_schema.columns where table_name = '{path_list[-1]}';"))
            column_string = ""
            for elem in columns:
                column_string += elem[0] + " "
            rows.append(column_string[:-1])
            for entry in result:
                rows.append(' '.join(map(str, entry)))
            return rows
        
