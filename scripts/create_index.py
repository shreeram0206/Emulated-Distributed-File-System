from plistlib import dump
import sys
import json
import pandas as pd
import requests
import re

upload_url = 'https://keywordindex-f2981-default-rtdb.firebaseio.com/keywordIndex.json'

car_df = pd.read_csv(sys.argv[1])
car_names = car_df['CarName'].values

keywords = []
for car in car_names:
    car_keywords = car.split()
    for car_keyword in car_keywords:
        keywords.append(car_keyword.lower())
keywords = set(keywords)

ids_dict = {}
for keyword in keywords:
    ids_dict['{0}'.format(keyword)] = []
    for word in car_names:
        keywords = word.split()
        keyword_lower = (map(lambda x: x.lower(), keywords))
        keywords = list(keyword_lower)
        if keyword in keywords:
            ids = car_df.loc[car_df['CarName']==word, 'car_ID'].values
            for id in ids:
                id = str(id)
                if id not in ids_dict['{0}'.format(keyword)]:
                    ids_dict['{0}'.format(keyword)].append(id)

ids_dict_json = json.dumps(ids_dict)
resp = requests.put(upload_url, ids_dict_json) 