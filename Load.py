import json
import pandas as pd
import sqlite3
import ast
from pandas.io.json import json_normalize
import sqlalchemy
import pprint

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


#Load laureates data
def Load_laureates_data(laureates_json,conn):
    conn.execute(''' DROP TABLE laureates_dump_staging''')
    conn.execute('''Create table if not exists laureates_dump_staging (
        id varchar(4) CONSTRAINT laureates_details_pk PRIMARY KEY,
        knownName json, 
        familyName varchar(1000),
        givenName varchar(1000),
        fileName varchar(1000),
        gender varchar(100),
        nobelPrizes json,
        fullName json,
        sameAs json,
        links json,
        wikidata json,
        birth json
        ) ''')
    

    for child in laureates_json['laureates']:
        sql= ''' Insert into laureates_dump_staging (id,knownName, familyName, givenName,
                 fileName, gender,nobelPrizes, fullName,
                 sameAs, links, wikidata, birth)
                 VALUES  (%s,%s,%s,%s,%s,%s,%s, %s,%s,%s,%s, %s)'''
        
        conn.execute(sql, (child['id'],
                           (json.dumps(child['knownName'])),json.dumps(child['familyName']),
                           json.dumps(child['givenName']),
                           child['fileName'],
                           child['gender'],
                           json.dumps(child['nobelPrizes']), json.dumps(child['fullName']),
                           json.dumps(child['sameAs']),
                           json.dumps(child['links']),
                           json.dumps(child['wikidata']),
                           json.dumps(child['birth']
                          )
                            ))
        
