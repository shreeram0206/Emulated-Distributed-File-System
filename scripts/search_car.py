import pandas as pd
import requests
import sys

inputUser = sys.argv[1]
inputNames = [word.lower() for word in inputUser.split()]
inputNames = set(inputNames)

db_url = 'https://keywordindex-f2981-default-rtdb.firebaseio.com/keywordIndex.json'
name_idxs = requests.get(db_url)

ans = {}

for name in inputNames:
    for name_idx in name_idxs.json():
        if name == name_idx:
            for id in name_idxs.json().get('{0}'.format(name)):
                if id not in ans:
                    ans[id] = 1
                else:
                    ans[id] = ans[id] + 1
{k: v for k, v in sorted(ans.keys(), key=lambda item: item[1])}
print(ans)
list = []
for i in ans:
    list.append(i)
print(list)