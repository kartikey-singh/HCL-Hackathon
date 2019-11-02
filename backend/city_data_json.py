import pandas as pd
import os
import apis
import json
from decimal import Decimal

def process():
    data = {}
    cities = os.listdir('backend/Cities')    
    for city in cities:
        data_city = []
        df = pd.read_csv('backend/Cities/' + city)

        for index, row in df.iterrows():            
            datum = {}
            datum["user"] = row['user'] 
            datum["tweet"] =  row['unfiltered_text']
            datum["topic"] = str(row['topics'])
            datum["sentiment"] = str(row['sentiment'])
            # print("safs")
            data_city.append(datum)
        data[city[:-4]] = data_city    

    # with open('result.json', 'w') as fp:
    return json.dumps(data, indent=4)
    
    # return 


            

if __name__ == "__main__":
    process()
    # ss = "afassafsd"
    # print(ss[:-3])
    # df = pd.read_csv('backend/Cities/DELHI.csv')
    # df1 = df.sort_values('popularity', ascending=False).groupby('topics').head(10)
    # df2 = df1.sort_values(
    #     ['topics', 'popularity'], ascending=[True, False])
    # print(df)
