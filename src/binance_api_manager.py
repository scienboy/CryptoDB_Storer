import ccxt
import pandas as pd
from datetime import timedelta

class BinanceAPI():

    def __init__(self, market='spot', timeframes=['1m']):

        self.market = market
        # self.timeframes = settings.myList['timeframese']
        # self.timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
        self.timeframes = timeframes
        # self.timeframes = ['1m']
        
        # spot 마켓은 개인정보 없이 조회. future는 파일에서 데이터 가져와서 조회
        if market == 'spot':
            self.binance = ccxt.binance()
        elif market == 'future':

            # with open(".\\data\\keys.txt") as f:    # API key 조회
            with open("keys.txt") as f:    # 리눅스 용
                lines = f.readlines()
                api_key = lines[0].strip()
                secret = lines[1].strip()
            self.binance = ccxt.binance(config={
                'apiKey': api_key,
                'secret': secret,
                'options': {
                    'defaultType': market
                }
            })   # API key를 통해 binance 객체생성

    def get_tickers(self):
        markets = self.binance.fetch_tickers()
        return markets.keys()

    def get_ohlcvs(self, symbol, timeframe):
        ohlcvs = pd.DataFrame(self.binance.fetch_ohlcv(symbol, timeframe))
        ohlcvs.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        ohlcvs['datetime'] = pd.to_datetime(ohlcvs['datetime'], unit='ms') + timedelta(hours=9)
        return ohlcvs