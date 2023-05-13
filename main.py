from smartapi import SmartConnect
from web_socketV2 import SmartWebSocketV2
from tokens import tokens
import pyotp
import PgConnection
import time
from config import *
import threading

obj = SmartConnect(api_key=APIKEY)

# login api call
data = obj.generateSession(USERNAME, APIPASSWORD, pyotp.TOTP(TOKEN).now())
authToken = data['data']['refreshToken']
refreshToken = data['data']['refreshToken']

# fetch the feedtoken
feedToken = obj.getfeedToken()
print(feedToken)

# fetch User Profile
userProfile = obj.getProfile(refreshToken)


def log_out_session():
    try:
        obj.terminateSession('G151503')
        print("Logout Successfull")
    except Exception as e:
        print("Logout failed: {}".format(e.message))


AUTH_TOKEN = authToken
API_KEY = APIKEY
CLIENT_CODE = USERNAME
FEED_TOKEN = feedToken
correlation_id = "priceaction"
action = 1
mode = 1

token_list = [{"exchangeType": 1, "tokens": ['2885', '21091', '3456','11536', '312', '9685', '18921', '3718', '3045', '20302', '13376', '14299']}]

sws = SmartWebSocketV2(AUTH_TOKEN, API_KEY, CLIENT_CODE, FEED_TOKEN)


def on_data(wsapp, message):
    print("Ticks: {}".format(message))
    PgConnection.add_ticks_data(message['token'], message)


def on_open(wsapp):
    print("on open")
    sws.subscribe(correlation_id, mode, token_list)


def on_error(wsapp, error):
    print(error)


def on_close(wsapp):
    print("Close")


# Assign the callbacks.
sws.on_open = on_open
sws.on_data = on_data
sws.on_error = on_error
sws.on_close = on_close


# threading.Thread(target=sws.connect).start()
# time.sleep(3)
# log_out_session()
