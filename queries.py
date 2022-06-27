import pandas as pd
def run_query(engine):
       filter_columns_query = '''  select nobel_prize.id, name_en, gender, birth_date, count(*) as PrizeWon
              from production.laureates_details
              inner join production.nobel_prize on (nobel_prize.id=laureates_details.id) 
              where production.nobel_prize."prizeStatus"='received'
              group by nobel_prize.id, laureates_details.name_en, laureates_details.birth_date, laureates_details.gender;
       '''
       result=engine.execute(filter_columns_query)
       result = result.fetchall()
       print(pd.DataFrame(result))

def common_records(engine):
       common_records = ''' select * from production.laureates_details
         inner join production.laureates on (laureates_details.id=laureates.id) '''
       result=engine.execute(common_records)
       result = result.fetchall()
       return pd.DataFrame(result)