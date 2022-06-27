#!/usr/bin/python
# -*- coding: utf-8 -*-
import findspark
findspark.init()
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col
from pyspark import SparkConf, SparkContext


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
    nobelPrizes = nobelPrizes_res.withColumn('nobelPrizes_',
            explode(col('nobelPrizes')))
    nobelPrizes.printSchema()
    normalized_nobelprize = nobelPrizes.withColumn('awardYear',
            nobelPrizes.nobelPrizes_.awardYear).withColumn('prizeAmountAdjusted'
            ,
            nobelPrizes.nobelPrizes_.prizeAmountAdjusted).withColumn('prizeAmount'
            ,
            nobelPrizes.nobelPrizes_.prizeAmount).withColumn('laureates_fullName'
            ,
            nobelPrizes.nobelPrizes_.laureates.fullName).withColumn('category'
            ,
            nobelPrizes.nobelPrizes_.category).withColumn('laureates_knownName'
            ,
            nobelPrizes.nobelPrizes_.laureates.knownName).withColumn('laureates_motivation'
            ,
            nobelPrizes.nobelPrizes_.laureates.motivation).withColumn('laureates_orgName'
            ,
            nobelPrizes.nobelPrizes_.laureates.orgName).withColumn('categoryFullName'
            , nobelPrizes.nobelPrizes_.categoryFullName)
    normalized_nobelprize.show()

    normalized_nobelprize = normalized_nobelprize.drop('nobelPrizes',
            'meta', 'links')
    normalized_nobelprize.printSchema()

    normalized_nobelprize = \
        normalized_nobelprize.withColumn('categoryFullName_en',
            normalized_nobelprize.categoryFullName.en).withColumn('laureates_orgName_en'
            ,
            normalized_nobelprize.laureates_orgName.en).withColumn('laureates_motivation_en'
            ,
            normalized_nobelprize.laureates_motivation.en).withColumn('laureates_knownName_en'
            , normalized_nobelprize.laureates_knownName.en)

    normalized_nobelprize.show()


if __name__ == '__main__':
    conf = SparkConf().setMaster('local').setAppName('My App')
    sc = SparkContext(conf=conf)

    spark = SparkSession.builder.appName('secret escape ELT Pipeline'
            ).config('spark.some.config.option', 'start').getOrCreate()
    nobelprize = extract(sc, spark)
    nobelprize.show()
    nobelPrizes_res = Load(spark, nobelprize)
    Transform(nobelPrizes_res)
