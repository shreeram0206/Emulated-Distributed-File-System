import requests
import sys
import pandas as pd

db_url = "https://dsci551-79c71-default-rtdb.firebaseio.com/cars.json"

def upload(filepath):
    df = pd.read_csv(filepath)
    df = df.set_index('car_ID')
    parsed = df.to_json(orient = 'index')
    return parsed

result = requests.put(db_url, upload(sys.argv[1]))