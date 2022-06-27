#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import pandas as pd
import ast


def Transform_nobelPrizes_data(conn):
    nobel_prize = pd.read_sql_query('select * from nobelPrizes_staging;'
                                    , conn)
    lauretes_json = json.loads(nobel_prize.to_json(orient='records'))
    (category_normalized, laureates, categoryFullName) = ([], [], [])

    for child in lauretes_json:
        child['category'] = ast.literal_eval(str(child['category']))
        catgry = pd.json_normalize(data=child['category'])
        category_normalized.append(catgry)

        child['laureates'] = ast.literal_eval(str(child['laureates']))
        df_new = pd.json_normalize(data=child['laureates'][0])
        laureates.append(df_new)

        child['categoryfullname'] = \
            ast.literal_eval(str(child['categoryfullname']))
        categores_name = pd.json_normalize(data=child['categoryfullname'
                ])
        categoryFullName.append(categores_name)

    categories = pd.concat(category_normalized)

    to_rename_categories = {'en': 'category_en', 'se': 'category_se',
                            'no': 'category_no'}

    categories = categories.rename(columns=to_rename_categories)

    laureates_records = pd.concat(laureates)
    category_FullName = pd.concat(categoryFullName)

    to_rename_fullname = {'en': 'category_fullname_en',
                          'se': 'category_fullname_se',
                          'no': 'category_fullname_no'}

    category_FullName = \
        category_FullName.rename(columns=to_rename_fullname)
    categories = categories.reset_index(drop=True)
    laureates_records = laureates_records.reset_index(drop=True)
    category_FullName = category_FullName.reset_index(drop=True)

    transformed_data = pd.concat([
        category_FullName,
        nobel_prize['prizeamountadjusted'],
        categories,
        laureates_records,
        nobel_prize['prizeamount'],
        nobel_prize['awardyear'],
        ], axis=1)
    transformed_data = transformed_data.drop(columns='links')  # dropping metadata columns

    conn.execute(''' CREATE SCHEMA IF NOT EXISTS production;  ''')
    transformed_data.to_sql('laureates', conn, if_exists='replace',
                            schema='production')


### Transforming laureates API data ####

def Transform_laureates_data(conn):
    conn.execute(''' CREATE SCHEMA IF NOT EXISTS production;  ''')
    laureates_data = \
        pd.read_sql_query('select * from laureates_dump_staging;', conn)
    lauretes_json = json.loads(laureates_data.to_json(orient='records'))

    (
        nobelprize_normalized,
        fullName,
        sameas,
        links,
        wikidata,
        birth,
        familyname,
        knownName,
        givenName,
        ) = (
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        [],
        )

    for child in lauretes_json:
        child['nobelprizes'] = ast.literal_eval(str(child['nobelprizes'
                ]))
        nobel_prize = pd.json_normalize(data=child['nobelprizes'])
        nobelprize_normalized.append(nobel_prize)

        child['fullname'] = ast.literal_eval(str(child['fullname']))
        df_new = pd.json_normalize(data=child['fullname'])
        fullName.append(df_new)

        child['sameas'] = ast.literal_eval(str(child['sameas']))
        same_as = pd.json_normalize(data=child['sameas'])
        sameas.append(same_as)

        child['links'] = ast.literal_eval(str(child['links']))
        links_ = pd.json_normalize(data=child['links'])
        links.append(links_)

        child['wikidata'] = ast.literal_eval(str(child['links']))
        wikidata_ = pd.json_normalize(data=child['wikidata'])
        wikidata.append(wikidata_)

        child['birth'] = ast.literal_eval(str(child['birth']))
        birth_ = pd.json_normalize(data=child['birth'])
        birth.append(birth_)

        child['knownname'] = ast.literal_eval(str(child['knownname']))
        knownName_ = pd.json_normalize(data=child['knownname'])
        knownName.append(knownName_)

        child['familyname'] = ast.literal_eval(str(child['familyname']))
        familyname_ = pd.json_normalize(data=child['familyname'])
        familyname.append(familyname_)

        child['givenname'] = ast.literal_eval(str(child['givenname']))
        givenName_ = pd.json_normalize(data=child['givenname'])
        givenName.append(givenName_)

    nobelprize_ = pd.concat(nobelprize_normalized)
    fullName = pd.concat(fullName)
    sameas = pd.concat(sameas)
    links = pd.concat(links)
    wikidata = pd.concat(wikidata)
    birth = pd.concat(birth)
    knownName = pd.concat(knownName)
    familyname = pd.concat(familyname)
    givenName = pd.concat(givenName)

    dataframes = [
        nobelprize_,
        fullName,
        sameas,
        links,
        wikidata,
        birth,
        knownName,
        familyname,
        givenName,
        ]
    for df in dataframes:
        df = df.reset_index(inplace=True)

    transformed_data = pd.concat([
        fullName,
        birth,
        laureates_data['id'],
        knownName,
        familyname,
        givenName,
        laureates_data['gender'],
        laureates_data['filename'],
        ], axis=1)

    new_names = {
        'en': 'name_en',
        'se': 'name_se',
        'date': 'birth_date',
        'place.city.en': 'city_en',
        'place.locationString.no ': 'location_no',
        'place.locationString.se': 'location_se',
        'place.city.no': 'city_no',
        'place.city.se': 'city_se',
        'place.country.en': 'country_en',
        'place.country.no': 'country_no',
        'place.cityNow.en': 'cityNow_en',
        'place.cityNow.no': 'cityNow_no',
        'place.cityNow.se': 'cityNow_se',
        'place.cityNow.sameAs': 'cityNow_sameAs',
        'place.countryNow.en': 'countryNow_en',
        'place.countryNow.no': 'countryNow_no',
        'place.countryNow.se': 'countryNow_se',
        'place.countryNow.sameAs': 'countryNow_sameAs',
        'place.continent.en': 'continent_en',
        'place.continent.no': 'country_no',
        'place.continent.se': 'continent_se',
        'place.locationString.en': 'locationstring_en',
        'place.locationString.no': 'location_no',
        }

    transformed_data.rename(columns=new_names, inplace=True)

    transformed_nobelprize_data = pd.concat([nobelprize_,
            laureates_data['id']], axis=1)

    transformed_data.to_sql('laureates_details', conn,
                            if_exists='replace', schema='production')

    nobel_prize = transformed_nobelprize_data[[
        'awardYear',
        'sortOrder',
        'portion',
        'dateAwarded',
        'prizeStatus',
        'prizeAmount',
        'prizeAmountAdjusted',
        'category.en',
        'category.no',
        'category.se',
        'categoryFullName.en',
        'categoryFullName.no',
        'categoryFullName.se',
        'motivation.en',
        'motivation.se',
        'motivation.no',
        'id',
        ]]

    to_rename = {
        'category.en': 'category_en',
        'category.se': 'category.se',
        'categoryFullName.en': 'fullname_category_en',
        'categoryFullName.no': 'fullname_category_no',
        'categoryFullName.se': 'category_fullname_se',
        'motivation.en': 'motivation_en',
        'motivation.se': 'motivation_se',
        'motivation.no': 'motivation_no',
        }
    nobel_prize.rename(columns=to_rename, inplace=True)
    nobel_prize.to_sql('nobel_prize', conn, if_exists='replace',
                       schema='production')

    conn.execute('ALTER TABLE production.laureates_details ADD PRIMARY KEY (id);'
                 )

