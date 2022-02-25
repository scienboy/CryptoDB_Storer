from binance_api_manager import *

class store_market_data():

    def __init__(self, parent, hts_name='Binance', market='Spot', symbols=['BTC/USDT'], timeframes=['1m']):
        super().__init__()
        self.parent = parent


        self.market = market
        self.hts_name = hts_name
        self.symbols = symbols
        self.timeframes = timeframes
        self.flag_finish = False
        if self.hts_name == 'Binance':
            self.binance_api = BinanceAPI(market=self.market.lower())
