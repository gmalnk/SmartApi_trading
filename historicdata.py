import PgConnection
from tokens import tokens
import time
from datetime import date
from datetime import datetime
from datetime import timedelta
import yfinance as yf
from config import *
from smartapi import SmartConnect
import pyotp

def get_today_candle_data():
    data = []
    for stock_tocken in tokens:
        start_date = date.today()-timedelta(1)
        symbol = tokens[stock_tocken] + ".NS"

        # End date is today i.e the date of analysis
        end_date = date.today()

        # This line is responsible for the data fetching part
        symboldata = yf.download(
            symbol, start=start_date, end=end_date, interval='1d')
        for i in symboldata.index:
            data.append([stock_tocken, start_date.strftime("%Y-%m-%d %H:%m"), symboldata['Open']
                        [i], symboldata['High'][i], symboldata['Low'][i], symboldata['Close'][i]])
        print(stock_tocken)
    print(data)
    PgConnection.add_market_data_daily(data)

# this method get past candle data fetches all stocks past data in daily time frame
# this method uses yfinance to fetch data
# this method fetcheds data from past twenty years till today data
# passes this data to add past data method in pg connect module
# symboldata is a pandas data frame


def get_past_candle_data(stock_token):
    # local parameters
    # start date is twenty years back
    start_date = date(date.today().year-20,
                        date.today().month, date.today().day)
    # end date is today i.e the day we are fetching the data
    end_date = date(date.today().year,
                    date.today().month, date.today().day-1)
    print(start_date, end_date)
    symbol = tokens[stock_token] + ".NS"

    # This line is responsible for fetching the data using yfinance in daily time frame
    symboldata = yf.download(
        symbol, start=start_date, end=end_date, interval='1d')
    # creating a new column called Date and it has the date of the candle data
    symboldata['Date'] = symboldata.index
    print("stock data collection done" + ":" + tokens[stock_token])
    PgConnection.add_past_data(stock_token, symboldata)


obj = SmartConnect(api_key=APIKEY)
data = obj.generateSession(USERNAME, APIPASSWORD, pyotp.TOTP(TOKEN).now())
# print(data)
authToken = data['data']['refreshToken']
refreshToken = data['data']['refreshToken']

# fetch the feedtoken
feedToken = obj.getfeedToken()
userProfile = obj.getProfile(refreshToken)
# print(userProfile)


print('session generated')
def historic_api(stock_token, time_frame, fromdate, todate):
    try:
        historicParam={
        "exchange": "NSE",
        "symboltoken": stock_token,
        "interval": time_frame,
        "fromdate": fromdate, 
        "todate": todate
        # "fromdate": "2021-02-08 09:00", 
        # "todate": "2021-02-08 09:16"
        }
        data = obj.getCandleData(historicParam)
        return data["data"]
    except Exception as e:
        print(f"Historic Api failed: {e.message}\nparams------\n stock_token: {stock_token}\n time_frame: {time_frame}\n fromdate: {fromdate}\n todate: {todate}\n  ")


# historic_api('7', 'ONE_DAY', '2023-03-03 00:00', '2023-05-28 00:00')

def get_data(time_frame):
    for stock_token in tokens:
        data =[]
        for i in range(23):
            time.sleep(1)
            fromdate = date(date.today().year-23+i,date.today().month,date.today().day).strftime("%Y-%m-%d %H:%M")
            todate = date(date.today().year-22+i,date.today().month-1,30).strftime("%Y-%m-%d %H:%M")
            rows =historic_api(stock_token, time_frame, fromdate, todate)
            if (rows):
                data.extend(rows)
        data.sort()
        for i in range(len(data)-1):
            if(data[i][0] == data[i+1][0]):
                print(f"date : {data[i][0]}")
        PgConnection.add_past_data_from_smart_api(stock_token, time_frame, data)
        print(f"done for stock_token : {stock_token}") 
        
# get_data("ONE_DAY")

def get_LT_data(time_frame):
    for stock_token in ['11483']:
        data =[]
        fromdate = datetime.now() - timedelta(days=30)
        todate = datetime.now()
        rows =historic_api(stock_token, time_frame, fromdate.strftime("%Y-%m-%d %H:%M"), todate.strftime("%Y-%m-%d %H:%M"))
        if (rows):
            data.extend(rows)
        data.sort()
        for i in range(len(data)-1):
            if(data[i][0] == data[i+1][0]):
                print(f"date : {data[i][0]}")
        PgConnection.add_past_data_from_smart_api(stock_token, time_frame, data)
        print(f"done for stock_token : {stock_token}")

get_LT_data("FIFTEEN_MINUTE")