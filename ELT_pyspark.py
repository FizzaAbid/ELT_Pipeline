#!/usr/bin/python
# -*- coding: utf-8 -*-

import findspark
findspark.init()
import requests
import json
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


def flatten_nobel_prize_df(nobel_prize_df):
    nested_fields = dict([(field.name, field.dataType)
                          for field in nobel_prize_df.schema.fields
                          if type(field.dataType) == ArrayType or type(field.dataType) == StructType])

    while len(nested_fields) != 0:
        column_name = list(nested_fields.keys())[0]
        print('Normalizing Field :' + column_name + ' of data-type : ' \
              + str(type(nested_fields[column_name])))

        if type(nested_fields[column_name]) == StructType:
            expanded = [col(column_name + '.' + k).alias(column_name + '_'
                                                         + k) for k in [n.name for n in
                                                                        nested_fields[column_name]]]
            nobel_prize_df = nobel_prize_df.select('*', *expanded).drop(column_name)
        elif type(nested_fields[column_name]) == ArrayType:

            nobel_prize_df = nobel_prize_df.withColumn(column_name, explode_outer(column_name))

        nested_fields = dict([(field.name, field.dataType)
                              for field in nobel_prize_df.schema.fields
                              if type(field.dataType) == ArrayType
                              or type(field.dataType) == StructType])

    return nobel_prize_df


def extract(sc, spark):
    response_API_nobelPrizes = \
        requests.get('https://api.nobelprize.org/2.1/nobelPrizes')
    if response_API_nobelPrizes.status_code == 200:
        nobelPrizes = response_API_nobelPrizes.text
        nobelPrizes_json = json.loads(nobelPrizes)
        nobelPrizes = \
            spark.read.json(sc.parallelize([nobelPrizes_json]))
    return nobelPrizes


def Load(spark, nobel_prize):
    nobel_prize.createOrReplaceTempView('nobelPrizes')
    nobelPrizes_res = spark.sql('SELECT * FROM nobelPrizes')
    return nobelPrizes_res


def Transform(nobelPrizes_res):
    res = flatten_nobel_prize_df(nobelPrizes_res)
    res.printSchema()
    #Next step is to write this to the database

if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('My App')
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.appName('secret escape ELT Pipeline'
            ).config('spark.some.config.option', 'start').getOrCreate()
    nobelprize = extract(sc, spark)
    nobelprize.show()
    nobelPrizes_res = Load(spark, nobelprize)
    Transform(nobelPrizes_res)
