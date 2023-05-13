import psycopg2
import datetime
from tokens import tokens
from config import *
from Candle import Candle
import Utility
from time import time
from datetime import timedelta
from datetime import datetime
from datetime import date
import pandas as pd
import numpy
from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)


def connect_to_database():
    return psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )


def close_connection():
    conn.close()


conn = connect_to_database()
cur =  conn.cursor()

# this method inserts ticks data into ticks_data table
# this method gets triggered when the on_data method gets a message


def add_ticks_data(token, data):
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO ticks_data (token, time_stamp, ltp) values(%s, %s, %s)""", [token, datetime.fromtimestamp(data['exchange_timestamp']/1000.0).strftime('%Y-%m-%d %H:%M'), data['last_traded_price']/100.0])
        conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into ticks_data table", error)

def get_ticks_candles(token, time_frame, start_time, end_time = None):
    time_frame = convert_timeframe(time_frame)
    if end_time == None:
        end_time = start_time
        start_time = end_time - timedelta(minutes = no_of_minutes(time_frame)) 
    cur = conn.cursor()
    cur.execute(f"select * from ticks_data where token = {token} and time_stamp >= '{start_time} and time_stamp <= '{end_time}'")
    rows = cur.fetchall()
    rows = convert_ltp_to_ohlc(time_frame, rows)
    candles = []
    for i in rows.index:
        candles.append(Candle(0, 0, token, i, rows['open'][i], rows['high'][i], rows['low'][i], rows['close'][i], ""))
    return candles
# this method is responsible for storing each day data, of all stocks, everyday
# data is a array of array having elements as follows symbol_token, time_stamp, open_price, high_price, low_price, close_price, high_low


def add_market_data_daily(data):
    try:
        conn = connect_to_database()
        cur = conn.cursor()
        for row in data:
            cur.execute("""INSERT INTO daily_data (symbol_token, time_stamp, open_price, high_price, low_price, close_price, high_low) values (%s,%s,%s,%s,%s,%s,'')""", [
                        row[0], row[1], row[2], row[3], row[4], row[5]])
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into daily_data table while inserting todays data (error):", error)
    finally:
        cur.close()
        conn.close()

# this method is responsible for inserting all the past data of a stock
# parameter data is a pandas dataframe it has columns Date, Open, High, Low, Close respectively


def add_past_data_from_yfinance(stock_token, data):
    try:
        conn = connect_to_database()
        cur = conn.cursor()
        counter = 0
        for i in data.index:
            cur.execute("""INSERT INTO daily_data (token, time_stamp, open_price, high_price, low_price, close_price, index) values (%s,%s,%s,%s,%s,%s,%s)""", [
                        stock_token, data['Date'][i], data['Open'][i], data['High'][i], data['Low'][i], data['Close'][i], counter])
            counter = counter + 1
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into market_data_daily table", error)
    finally:
        cur.close()
        conn.close()

def add_past_data_from_smart_api(stock_token, time_frame, data):
    try:
        table = get_table(time_frame)
        conn = connect_to_database()
        cur = conn.cursor()
        counter = 0
        for row in data:
            row[0] = row[0].replace("T", " ")
            cur.execute(f"INSERT INTO {table} (token, time_stamp, open_price, high_price, low_price, close_price, index) values ({stock_token}, '{row[0]}', {row[1]}, {row[2]}, {row[3]}, {row[4]}, {counter})")
            counter = counter + 1
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print(f" at add_past_data_from_smart_api method failed for stock_token : {stock_token}", error)
    finally:
        cur.close()
        conn.close()

def initialize_high_low(stock_token, time_frame):
    try:
        conn = connect_to_database()
        cur = conn.cursor()
        candles = fetch_candles(stock_token, time_frame)
        
        candles = Utility.find_highs_and_lows(candles)
        for candle in candles:
            cur.execute(
                """insert into highlow_data (index, token, time_stamp, open_price, high_price, low_price, close_price, high_low, tf) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""", [candle.Index, stock_token, candle.Date, candle.Open, candle.High, candle.Low, candle.Close, candle.High_Low, time_frame])
            conn.commit()
        print("inserted ***********************************")
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into market_data_daily table", error)
    finally:
        cur.close()
        conn.close()

def get_trendLines(stock_token, time_frame):
    try:
        highs = fetch_highs(stock_token, time_frame)
        lows = fetch_lows(stock_token, time_frame)
        priceData = Utility.PriceData(highs, lows)
        trendlines = priceData.TrendlinesToDraw
        for trendline in trendlines:
            query = f"insert into trendline_data (token, tf, slope, intercept, startdate, enddate, hl, index1, index2, index) values ({stock_token}, '{time_frame}', {trendline[1]}, {trendline[2]},'{trendline[0][0].Date}','{trendline[0][-1].Date}','h' ,{trendline[0][0].Index},{trendline[0][-1].Index},500)"
            cur.execute(query)
            conn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed at get trendlines method  error message : ", error)
    finally:
        print("generated trendlines successfully for ",tokens[stock_token]," stock")

def fetch_highs(stock_token, time_frame):
    try:
        conn = connect_to_database()
        cur = conn.cursor()
        start_time = get_starttime_of_analysis(time_frame)
        cur.execute(
            f"select * from highlow_data where token = {stock_token} and tf = '{time_frame}' and high_low like 'high%' and time_stamp > '{start_time}' order by index asc")
        rows = cur.fetchall()
        candles = []
        for row in rows:
            candles.append(
                Candle(0, row[1], row[2], row[3], row[4], row[5], row[6], row[7], ""))
        return candles
    except (Exception, psycopg2.Error) as error:
        print("Failed at fetch highs : ", error)
    finally:
        cur.close()
        conn.close()
        print("fetched highs successfully for ",tokens[stock_token]," stock")
    
def fetch_lows(stock_token, time_frame):
    try:
        conn = connect_to_database()
        cur = conn.cursor()
        start_time = get_starttime_of_analysis(time_frame)
        cur.execute(
            f"select * from highlow_data where token = {stock_token} and tf = '{time_frame}' and high_low like '%low' and time_stamp > '{start_time}' order by index asc")
        rows = cur.fetchall()
        candles = []
        for row in rows:
            candles.append(
                Candle(0, row[1], row[2], row[3], row[4], row[5], row[6], row[7], ""))
        return candles
    except (Exception, psycopg2.Error) as error:
        print("Failed at fetch highs : ", error)
    finally:
        cur.close()
        conn.close()
        print("fetched lows successfully for ",tokens[stock_token]," stock")

def fetch_candles(stock_token, time_frame, limit = 0):
    try:
        conn = connect_to_database()
        cur = conn.cursor()
        table = get_table(time_frame)
        start_time = get_starttime_of_analysis(time_frame)
        query = f"select * from {table} where token = {stock_token} and time_stamp >= '{start_time.strftime('%Y-%m-%d %H:%M')}'"
        if limit > 0:
            query += f" order by index desc limit {limit}"
        cur.execute(query)
        rows = cur.fetchall()
        rows = convert_data_timeframe(time_frame, rows)
        if rows.empty:
            return None
        candles = []
        counter = 0
        for i in rows.index:
            candles.append(
                Candle(rows['id'][i], counter, rows['token'][i], rows['time_stamp'][i], rows['open_price'][i], rows['high_price'][i], rows['low_price'][i], rows['close_price'][i], ""))
            counter += 1
        return candles
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into market_data_daily table : ", error)
    finally:
        cur.close()
        conn.close()
        print("fetched candles successfully for ",tokens[stock_token]," stock for ",time_frame," timeframe" )
    
def get_starttime_of_analysis(time_frame):
    match time_frame:
        case 'FIFTEEN_MINUTE':
            return date.today()-timedelta(days = 30)
        case 'THIRTY_MINUTE':
            return date.today()-timedelta(days = 60)
        case 'ONE_HOUR':
            return date.today()-timedelta(weeks = 14)
        case 'TWO_HOUR':
            return date.today()-timedelta(weeks = 28)
        case 'FOUR_HOUR':
            return date(date.today().year-1, date.today().month, date.today().day)
        case 'ONE_DAY':
            return date(date.today().year-2, date.today().month, date.today().day)
        case 'ONE_WEEK':
            return date(date.today().year-20, date.today().month, date.today().day)
        case 'ONE_MONTH':
            return date(date.today().year-20, date.today().month, date.today().day)
        case default:
            return date.today()

def get_table(time_frame):
    match time_frame:
        case 'FIFTEEN_MINUTE':
            return "fifteentf_data"
        case 'THIRTY_MINUTE':
            return "fifteentf_data"
        case 'ONE_HOUR':
            return "fifteentf_data"
        case 'TWO_HOUR':
            return "fifteentf_data"
        case 'FOUR_HOUR':
            return "fifteentf_data"
        case 'ONE_DAY':
            return "dailytf_data"
        case 'ONE_WEEK':
            return "dailytf_data"
        case 'ONE_MONTH':
            "dailytf_data"
        case default:
            "dailytf_data"

def convert_data_timeframe(time_frame, rows):
    df = pd.DataFrame(rows, columns =['id', 'index', 'token', 'time_stamp', 'open_price', 'high_price', 'low_price', 'close_price'])
    df['Date'] = df['time_stamp']
    df = df.set_index('Date')
    if time_frame == "ONE_DAY" or time_frame == "FIFTEEN_MINUTE":
        return df
    df = df.resample(convert_timeframe(time_frame), base = 15).apply(OHLC)
    return df

def convert_timeframe(time_frame):
    match time_frame:
        case 'FIFTEEN_MINUTE':
            return "15T"
        case 'THIRTY_MINUTE':
            return "30T"
        case 'ONE_HOUR':
            return "1H"
        case 'TWO_HOUR':
            return "2H"
        case 'FOUR_HOUR':
            return "4H"
        case 'ONE_DAY':
            return "D"
        case 'ONE_WEEK':
            return "W"
        case 'ONE_MONTH':
            return "M"
        case default:
            return "D"

def no_of_minutes(time_frame):
    match time_frame:
        case 'FIFTEEN_MINUTE':
            return 15
        case 'THIRTY_MINUTE':
            return 30
        case 'ONE_HOUR':
            return 60
        case 'TWO_HOUR':
            return 120
        case 'FOUR_HOUR':
            return 240
        case default:
            return 0

def convert_ltp_to_ohlc(time_frame, rows):
    df = pd.DataFrame(rows, columns =['id', 'token', 'time_stamp', 'ltp'])
    df['Date'] = df['time_stamp']
    df = df.set_index('Date')
    df = df['ltp'].resample(time_frame).ohlc(_method='ohlc')
    print(df)
    return df

def data_handler(time_frame, start_time):
    stock_token = '18944'
    candles  = fetch_candles(stock_token,time_frame, 10)
    candles.append(get_ticks_candles(stock_token,time_frame,start_time))
    candles = Utility.find_highs_and_lows(candles)
    for candle in candles:
        cur.execute(
            """insert into highlow_data (index, token, time_stamp, open_price, high_price, low_price, close_price, high_low, tf) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""", [candle.Index, stock_token, candle.Date, candle.Open, candle.High, candle.Low, candle.Close, candle.High_Low, time_frame])
        conn.commit()
    print("inserted ***********************************")



get_trendLines('11483', 'FIFTEEN_MINUTE')































