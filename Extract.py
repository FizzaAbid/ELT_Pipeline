from textwrap import indent
from traceback import print_tb
import requests
import json
import pandas as pd
import pprint

def extract_Nobel_Prize_API():
    response_API_nobelPrizes = requests.get('https://api.nobelprize.org/2.1/nobelPrizes')
    if response_API_nobelPrizes.status_code==200:
        nobelPrizes = response_API_nobelPrizes.text
        nobelPrizes_json = json.loads(nobelPrizes)
        return nobelPrizes_json
    else:
        print("data is not fetched")
        return 0;
    
#Extracting json from laureates API
def extract_laureates_API():
    response_API_lauretes = requests.get('https://api.nobelprize.org/2.1/laureates')
    if response_API_lauretes.status_code==200:
        lauretes = response_API_lauretes.text
        lauretes_json = json.loads(lauretes)
        nobelPrizes = pd.json_normalize(data=lauretes_json['laureates'])
        nobelPrizes= nobelPrizes.rename({'links': 'laureates_links'}, axis='columns') #two columns have same name in json
        nobelPrizes['nobelPrizes'] = nobelPrizes['nobelPrizes'].apply(pd.Series)
        df=pd.json_normalize(nobelPrizes['nobelPrizes'], max_level=1)    
        lauretes_nobel_prize = pd.concat([nobelPrizes, df], axis=1)
        df = pd.DataFrame({"lauretes": lauretes_nobel_prize['death.place.cityNow.sameAs']})
        df2 = df.explode('lauretes').reset_index()
        lauretes_nobel_prize['death_citynow_sameas'] =df2['lauretes']
        return lauretes_nobel_prize
    else:
        print("Data is not fetched")
