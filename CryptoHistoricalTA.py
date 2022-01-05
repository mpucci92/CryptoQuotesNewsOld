import numpy
import talib as ta
import glob as glob
import os
import json
import pandas as pd

timeperiods = {'MA': [2, 5, 10, 20, 50, 200],
               'MACD': [8, 10, 10],
               'ROC': [1, 2, 3, 4, 5, 10, 50, 200],
               'STD': [20, 30, 60, 90, 180]}

# Opening JSON file
with open(os.getcwd() + "\crypto_availableCoins.json", 'r') as openfile:
    # Reading from json file
    availablecoins = json.load(openfile)


def SMA(close, time):
    """
    SMA: Simple Moving Average
    """
    return ta.SMA(close, timeperiod=time)


def EMA(close, time):
    """
    EMA: Exponential Moving Average
    """
    return ta.EMA(close, timeperiod=time)


def WMA(close, time):
    """
    WMA: Weighted Moving Average
    """
    return ta.WMA(close, timeperiod=time)


def MACD(close, fast, slow, signalAverage):
    """
    MACD Convergence Divergence
    """
    macd, macdsignal, macdhist = ta.MACD(close, fastperiod=fast, slowperiod=slow, signalperiod=signalAverage)
    return macd, macdsignal


def ROC(close, time):
    """
    ROC: Rate of change Percentage: (price-prevPrice)/prevPrice
    """
    return ta.ROCP(close, timeperiod=time)


def ROCR(close, time):
    """
    ROCR: Rate of change ratio: (price/prevPrice)
    """
    return ta.ROCR(close, timeperiod=time)


def RollingSTD(df_col, time):
    """
    RollingSTD: Rolling Standard Deviation of N Periods
    """
    return df_col.rolling(time).std()


def PriceRank(df):
    """
    Price Rank as a percentile
    """
    df['Rank'] = 100 * df['Close'].rank(pct=True)

def timeProcessor(df):
    for i in range(len(df)):
        df['Date'].iloc[i] = df['Date'].iloc[i] + ' 16:00:00'
    df['Date'] = pd.to_datetime(df['Date'])


# Parent Directory path
parent_dir = os.getcwd() + f"\\Data\\Historical TA\\"
path_to_save = os.getcwd() + f"\\Data\\Historical TA\\"

availableCoins = availablecoins['availableCoins']

files_to_process = glob.glob(r"D:\CryptoNews\Data\Historical\*")

for file in files_to_process:
    print(file)

    for data in glob.glob(file + '\*'):
        cryptodf = pd.read_csv(rf"{data}")

        for time_ma in timeperiods['MA']:
            try:
                title = f"SMA_{time_ma}D"

                cryptodf[title] = SMA(cryptodf['Close'], time_ma)

            except Exception as e:
                cryptodf[title] = [None] * len(cryptodf)

            try:
                title = f"EMA_{time_ma}D"

                cryptodf[title] = EMA(cryptodf['Close'], time_ma)
            except Exception as e:
                cryptodf[title] = [None] * len(cryptodf)

        try:
            cryptodf['MACD'] = MACD(timeperiods['MACD'][0], timeperiods['MACD'][1], timeperiods['MACD'][2])[0]
        except:
            cryptodf['MACD'] = [None] * len(cryptodf)

        try:
            cryptodf['MACD Signal'] = MACD(timeperiods['MACD'][0], timeperiods['MACD'][1], timeperiods['MACD'][2])[1]
        except:
            cryptodf['MACD Signal'] = [None] * len(cryptodf)

        for time_roc in timeperiods['ROC']:
            try:
                title = f"ROC_{time_roc}D"

                cryptodf[title] = ROC(cryptodf['Close'], time_roc)
            except Exception as e:
                cryptodf[title] = [None] * len(cryptodf)
            try:
                title = f"ROCR_{time_roc}D"

                cryptodf[title] = ROCR(cryptodf['Close'], time_roc)
            except Exception as e:
                cryptodf[title] = [None] * len(cryptodf)

        for time_std in timeperiods['STD']:
            try:
                title = f"STD_{time_roc}D"

                cryptodf[title] = RollingSTD(cryptodf['Close'], time_std)
            except Exception as e:
                cryptodf[title] = [None] * len(cryptodf)

        PriceRank(cryptodf)
        timeProcessor(cryptodf)

        try:
            directory = file.split("\\")[-1]
            path = os.path.join(parent_dir, directory)

            if os.path.exists(path) == False:
                os.mkdir(path)

            cryptodf.to_csv(path_to_save + f"{directory}\\{directory}.csv")

        except Exception as e:
            print(e)
            print(directory)

        print('DONE')