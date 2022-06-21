import json
import pandas as pd
import sqlite3
import ast
from pandas.io.json import json_normalize
import sqlalchemy

def Load__Nobel_Prize(lauretes_nobel_prize,engine):
    lauretes_nobel_prize = lauretes_nobel_prize.applymap(str)
    #Inserting Records in Staging Table
    lauretes_nobel_prize.to_sql('laureates_staging', engine, if_exists='replace')

def Load_in_Production(df_laureates,engine):    
    engine.execute("DROP TABLE if exists laureates ")
    df_laureates.to_sql('laureates',engine)
    

def Load_nobelPrizes_data(nobelprize_json,conn):
    conn.execute('''Create table if not exists nobelPrizes_staging (
        awardYear varchar(7), 
        category json, 
        categoryFullName json,
        prizeAmount int,
        prizeAmountAdjusted int,
        laureates json,
        links json
        ) ''')

    for child in nobelprize_json['nobelPrizes']:
        sql= ''' Insert into nobelPrizes_staging (awardYear,  category, categoryFullName, 
                 prizeAmount, prizeAmountAdjusted,laureates, links)
                 VALUES  (%s,%s,%s,%s,%s,%s,%s)'''
        
        conn.execute(sql, (child['awardYear'], json.dumps(child['category']),
                           json.dumps(child['categoryFullName']),child['prizeAmount'], child['prizeAmountAdjusted'],
                           json.dumps(child['laureates']), json.dumps(child['links'])))
        
