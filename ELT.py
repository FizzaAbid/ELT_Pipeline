from Extract import *
from Load import *
from Transform import *
import sqlalchemy
import pandas as pd

if __name__ == "__main__":
    
    engine = sqlalchemy.create_engine('postgresql://docker:docker@localhost:5432/secret_escapes')
    
    lauretes_nobel_prize=extract_laureates_API() #extract data from api
    Load__Nobel_Prize(lauretes_nobel_prize,engine)
    df_laureates=Transform_Lauretes(engine)
    Load_in_Production(df_laureates, engine)
    run_query(engine)

    # Extracting Nobel Prize API Data#
    nobelprize_json=extract_Nobel_Prize_API()
    Load_nobelPrizes_data(nobelprize_json,engine)
    Transform_nobelPrizes_data(engine)
    common_records(engine)
    print("DATA PROCESSED SUCCESSFULLY")
    
    
