import pytz
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import os
import json
from datetime import datetime, timedelta,tzinfo
import pandas as pd

local_tz = 'US/Eastern'
fmt = '%Y-%m-%dT%H:%M:%S.%fZ'

# Opening Crypto Config File
with open(os.getcwd() + "\crypto_config.json", 'r') as openfile:
    # Reading from json file
    coinmarketcapConfig = json.load(openfile)

# Adjusting Timestamp of the file - Used to apply timestamp to files
def timeAdjuster(timestamp,local_tz,fmt):
    """
    timestamp: Timestamp from API Call
    local_tz: Specify timezone for timestamp conversion
    fmt: Specify how input time is formatted
    """
    date = pytz.timezone(local_tz).localize(datetime.strptime(timestamp,fmt))
    tzAdjust = int(str(date).split('-')[-1].split(':')[0])
    adjustDate = date - timedelta(hours=tzAdjust)
    adjustDate = str(adjustDate).split('.')[0]
    return adjustDate

def API_Call(api_key,url):

    parameters = {
      'start':'1',
      'limit':'2000'
      #'convert':'USD'
}

    headers = {
      'Accepts': 'application/json',
      'X-CMC_PRO_API_KEY': api_key,
    }

    session = Session()
    session.headers.update(headers)

    try:
        response = session.get(url, params=parameters)
        data = json.loads(response.text)

    except (ConnectionError, Timeout, TooManyRedirects) as e:
        print(e)

    return data

def GeneralCrypto(data):
    # 1. Generate Crypto Information DataFrame
    columns = list(data['data'][0].keys())[0:-1]
    values = list(data['data'][0].values())[0:-1]
    df = pd.DataFrame.from_dict(dict(zip(columns, values)))
    df = df.drop(columns=['platform'])

    for i in range(1, len(data['data'])):
        try:
            values = list(data['data'][i].values())[0:-1]
            columns = list(data['data'][i].keys())[0:-1]
            df2 = pd.DataFrame.from_dict(dict(zip(columns, values)))
            df2 = df2.drop(columns=['platform'])
            df = pd.concat([df, df2], ignore_index=True)

        except Exception as e:
            adjValues = []
            columns = ['id', 'name', 'symbol', 'slug', 'num_market_pairs', 'date_added', 'tags', 'max_supply',
                       'circulating_supply', 'total_supply', 'cmc_rank', 'last_updated']
            for value in values:
                if type(value) != dict:
                    adjValues.append(value)
            df3 = pd.DataFrame.from_dict(dict(zip(columns, adjValues)))
            df = pd.concat([df, df3], ignore_index=True)

    # 2. Post-Processing Data Cleanse on MaxSupply,CirculatingSupply and TotalSupply Columns
    maxSupply = []
    circulatingSupply = []
    totalSupply = []

    for i in range(len(df)):

        try:
            max_supplyAdj = "{:,}".format(df['max_supply'][i])
            maxSupply.append(max_supplyAdj)
        except Exception as e:
            maxSupply.append(None)

        try:
            circulating_supplyAdj = "{:,}".format(df['circulating_supply'][i])
            circulatingSupply.append(circulating_supplyAdj)
        except Exception as e:
            circulatingSupply.append(None)

        try:
            total_supplyAdj = "{:,}".format(df['total_supply'][i])
            totalSupply.append(total_supplyAdj)
        except Exception as e:
            totalSupply.append(None)

    df["max_supply"] = maxSupply
    df["total_supply"] = totalSupply
    df["circulating_supply"] = circulatingSupply

    return df

if __name__ == '__main__':
    api_key = coinmarketcapConfig['coinMarketCapKey']
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    data = API_Call(api_key, url)
    fileDate = timeAdjuster(data['status']['timestamp'], local_tz, fmt)
    fileDate = fileDate.replace(':', '-')

    # Save General Crypto Information Data
    generalFilePath = os.getcwd() +f"\\Data\\General\\{fileDate}_GeneralCryptos.csv"
    df = GeneralCrypto(data)
    df.to_csv(generalFilePath, index=False)

    # Save Crypto Coin List
    listFilePath = os.getcwd() + f"\\Data\\List\\{fileDate}_CryptoList.csv"
    df = GeneralCrypto(data)
    cryptoList = pd.DataFrame()
    cryptoList['Coin'] = list(set(df.name))
    cryptoList.to_csv(listFilePath, index=False)




