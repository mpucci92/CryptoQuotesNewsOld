import json
import time
import os
import numpy as np
import pandas as pd
import investpy

def recentData(companyname):
    search_result = investpy.search_quotes(text=companyname,n_results=1)
    return search_result.retrieve_recent_data()

def historicalData(companyname, startTime, endTime):
    # startTime: dd/mm/yyyy
    # endTime: dd/mm/yyyy
    search_result = investpy.search_quotes(text=companyname, n_results=1)
    return search_result.retrieve_historical_data(from_date=startTime, to_date=endTime)

count = 0

# Opening JSON file
with open(os.getcwd() + "\crypto_availableCoins.json", 'r') as openfile:
    # Reading from json file
    availablecoins = json.load(openfile)

# Parent Directory path
parent_dir = os.getcwd() + f"\\Data\\Historical\\"
path_to_save = os.getcwd() + f"\\Data\\Historical\\"

availableCoins = availablecoins['availableCoins']

for coin in availableCoins:
    try:
        df = historicalData(coin, "01/12/2015", "25/12/2021")
        directory = coin
        path = os.path.join(parent_dir, directory)

        if os.path.exists(path) == False:
            os.mkdir(path)

        df.to_csv(path_to_save + f"{coin}\\{coin}.csv")

    except Exception as e:
        print(e)
        print(coin)

    time.sleep(2)
    count += 1
    print(count)
