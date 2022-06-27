#!/usr/bin/python
# -*- coding: utf-8 -*-
import requests
import json
import pandas as pd
from constants.constants import *


def extract_Nobel_Prize_API():
    response_API_nobelPrizes = requests.get(nobel_prize_api)
    if response_API_nobelPrizes.status_code == 200:
        nobelPrizes = response_API_nobelPrizes.text
        nobelPrizes_json = json.loads(nobelPrizes)
        return nobelPrizes_json
    else:
        print ('data is not fetched')
        return 0


def extract_laureates_Prize_API():
    response_API_laureates = requests.get(laureates_api)
    if response_API_laureates.status_code == 200:
        laureates = response_API_laureates.text
        laureates_json = json.loads(laureates)
        return laureates_json
    else:
        print ('data is not fetched')
        return 0
