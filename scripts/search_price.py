import sys
import firebase_admin
from firebase_admin import db

cred = firebase_admin.credentials.Certificate('dsci551-79c71-firebase-adminsdk-50yb1-8a86c631d5.json')
firebase_admin.initialize_app(cred, {'databaseURL': 'https://dsci551-79c71-default-rtdb.firebaseio.com/cars'})
ref = firebase_admin.db.reference("cars")

def search_by_price(lower_range, higher_range):
    cars = ref.order_by_child('price').start_at(int(lower_range)).end_at(int(higher_range)).get()
    ans = []
    for key, val in cars.items():
        ans.append(int(key))
    if(len(ans) == 0):
        return 'No cars found with the given range'
    else:
        print('IDs for the car price range are:')
        print(ans)

search_by_price(sys.argv[1], sys.argv[2])

 