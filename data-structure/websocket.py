import json
import websocket
import datetime
from DataStructures import *
import credentials
from binance.client import Client
api_key = credentials.api_key
api_secret = credentials.api_secret
client = Client(api_key, api_secret)

def on_message(dollar_bar, volume_bar, tick_bar, wsapp, message):
    json_message = json.loads(message)
    trade = {
        'Date': datetime.datetime.fromtimestamp(json_message['E'] / 1000).strftime('%Y-%m-%d'),
        'Time': datetime.datetime.fromtimestamp(json_message['E'] / 1000).strftime('%H:%M:%S.%f')[:-3],
        'Price': float(json_message['p']),
        'Volume': float(json_message['q'])
    }

    dollar_thread = threading.Thread(target=dollar_bar.handle_trade, args=(trade,))
    volume_thread = threading.Thread(target=volume_bar.handle_trade, args=(trade,))
    tick_thread = threading.Thread(target=tick_bar.handle_trade, args=(trade,))

    dollar_thread.start()
    volume_thread.start()
    tick_thread.start()

    dollar_thread.join()
    volume_thread.join()
    tick_thread.join()


def ws_trades(dollar_bar, volume_bar, tick_bar):
    socket = f'wss://stream.binance.com:9443/ws/ethusdt@trade'

    wsapp = websocket.WebSocketApp(
        socket,
        on_message=lambda wsapp, message: on_message(dollar_bar, volume_bar, tick_bar, wsapp, message),
        on_error=lambda wsapp, error: print(error)
    )
    wsapp.run_forever()


if __name__ == "__main__":
    threshold_dollar = 10000
    threshold_volume = 1000
    threshold_tick = 100

    dollar_bar = RealTimeBars(threshold=threshold_dollar, bar_type='dollar')
    volume_bar = RealTimeBars(threshold=threshold_volume, bar_type='volume')
    tick_bar = RealTimeBars(threshold=threshold_tick, bar_type='tick')

    ws_trades(dollar_bar, volume_bar, tick_bar)