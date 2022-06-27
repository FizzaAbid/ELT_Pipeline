#!/usr/bin/python
# -*- coding: utf-8 -*-
from Extract import *
from Load import *
from Transform import *
from queries import *
import sqlalchemy
from constants.constants import *

if __name__ == '__main__':
    engine = sqlalchemy.create_engine('postgresql://' + username + ':'
            + password + '@localhost:' + port + '/postgres')

    laureates_extracted = extract_laureates_Prize_API()
    if laureates_extracted != 0:
        Load_laureates_data(laureates_extracted, engine)
        Transform_laureates_data(engine)
    else:
        print ('API did not return response')

    nobelprize_json = extract_Nobel_Prize_API()
    if nobelprize_json != 0:
        Load_nobelPrizes_data(nobelprize_json, engine)
        Transform_nobelPrizes_data(engine)

        # For executing queries on production intensive data

        common_records = common_records(engine)
        print (common_records)
        run_query(engine)
        print ('ELT performed successfully on laureates and nobel prize dataset')
    else:
        print ('API did not return response')
