import os
from datetime import datetime
from pytz import timezone
import pandas as pd
from requests import Request, Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects
import json

tz = timezone('EST')
currentDate = (str(datetime.now(tz)).split(" ")[0])
currentFullDate = (str(datetime.now(tz)).split(".")[0]).replace(':','-')

# Directory
directory = currentDate
# Parent Directory path
parent_dir = os.getcwd() +f"\\Data\\Raw\\"
path = os.path.join(parent_dir, directory)

# Opening Crypto Config File
with open(os.getcwd() + "\crypto_config.json", 'r') as openfile:
    # Reading from json file
    coinmarketcapConfig = json.load(openfile)

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

def cryptoDataGenerator(data,path_to_save,currentFullDate):
    for i in range(len(data['data'])):
        try:
            values = list(data['data'][i]['quote']['USD'].values())
            columns = list(data['data'][i]['quote']['USD'].keys())
            symbol = data['data'][i]['name']
            quote_df = pd.DataFrame.from_dict(zip(columns,values))
            quote_df.columns = [symbol,'Value']
            quote_df.iloc[1]['Value'] = "{:,}".format(quote_df[quote_df[symbol] == 'volume_24h'].Value.values[0])
            quote_df.iloc[9]['Value'] = "{:,}".format(quote_df[quote_df[symbol] == 'market_cap'].Value.values[0])
            quote_df.iloc[11]['Value'] = "{:,}".format(quote_df[quote_df[symbol] == 'fully_diluted_market_cap'].Value.values[0])
            quote_df.to_csv(path_to_save+f"{symbol}_{currentFullDate}.csv",index=False)
        except Exception as e:
            print(e)
            print(symbol)
            pass

if __name__ == '__main__':
    if os.path.exists(path) == False:
        os.mkdir(path)

    currentFullDate = (str(datetime.now(tz)).split(".")[0]).replace(':', '-')
    path_to_save = f"D:\\CryptoNews\\Data\\Raw\\{currentDate}\\"
    api_key = coinmarketcapConfig['coinMarketCapKey']
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    data = API_Call(api_key, url)

    cryptoDataGenerator(data, path_to_save,currentFullDate)


