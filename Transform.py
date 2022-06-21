import json
import pandas as pd
import ast
from tabulate import tabulate
import pprint


def Transform_Lauretes(conn):
    df_laureates=pd.read_sql_query("SELECT * from laureates_staging",conn) #fetching data from staging table, reading it in pandas
    #Discarding few columns, which are metadata related to normalize the schema
    df_laureates= df_laureates.drop(columns=['laureates_links', 'death.place.countryNow.sameAs','nobelPrizes', 'links', 'sameAs', 'birth.place.cityNow.sameAs'])
    #Transformations#
    df_laureates = df_laureates.applymap(str)
    df_laureates['awardYear'] = df_laureates['awardYear'].astype(int)
    df_laureates['prizeAmount'] = df_laureates['prizeAmount'].astype(int)
    df_laureates['prizeAmountAdjusted'] = df_laureates['prizeAmountAdjusted'].astype(int)
    df_laureates['sortOrder'] = df_laureates['sortOrder'].astype(int)  
    return df_laureates

def Transform_nobelPrizes_data(conn):
    nobel_prize=pd.read_sql_query("select * from nobelPrizes_staging;", conn)
    lauretes_json = json.loads(nobel_prize.to_json(orient='records'))
    category_normalized=[]
    laureates=[]
    categoryFullName=[]
    
    for child in lauretes_json:
        child['category'] = ast.literal_eval(str(child['category']))
        df2 = pd.json_normalize(data=child['category'])
        category_normalized.append(df2)

        child['laureates'] = ast.literal_eval(str(child['laureates']))
        df_new = pd.json_normalize(data=child['laureates'][0])
        laureates.append(df_new)

        child['categoryfullname'] = ast.literal_eval(str(child['categoryfullname']))
        categores_name = pd.json_normalize(data=child['categoryfullname'])
        categoryFullName.append(categores_name)

    categories = pd.concat(category_normalized)
    
    to_rename_categories={'en': 'category_en',
               'se': 'category_se',
               'no': 'category_no'
               }
    
    categories = categories.rename(columns=to_rename_categories)

    laureates_records = pd.concat(laureates)
    category_FullName = pd.concat(categoryFullName)
    
    to_rename_fullname={'en': 'category_fullname_en',
               'se': 'category_fullname_se',
               'no': 'category_fullname_no'
               }

    category_FullName = category_FullName.rename(columns=to_rename_fullname)
    categories = categories.reset_index(drop=True)
    laureates_records = laureates_records.reset_index(drop=True)
    category_FullName = category_FullName.reset_index(drop=True)


    transformed_data = pd.concat([ category_FullName, nobel_prize['prizeamountadjusted'], categories, laureates_records, nobel_prize['prizeamount'],nobel_prize['awardyear']], axis=1)
    transformed_data=transformed_data.drop(columns='links') #dropping metadata columns

    transformed_data.to_sql('laureates_nobel_prize', conn, if_exists='replace')
    
    
def run_query(engine):
       filter_columns_query = ''' select id, "fullName.en", "birth.date", gender, count(*) as
       PrizeWon from laureates where laureates."prizeStatus"='received'
       group by id, "fullName.en", "birth.date", gender '''
       result=engine.execute(filter_columns_query)
       result = result.fetchall()
       print(pd.DataFrame(result))

def common_records(engine):
       common_records = ''' select * from laureates_nobel_prize
         inner join laureates on (laureates_nobel_prize.id=laureates.id) '''
       result=engine.execute(common_records)
       result = result.fetchall()
       print(pd.DataFrame(result))


       
       