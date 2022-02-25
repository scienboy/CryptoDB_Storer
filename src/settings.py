# settings.py
from logging.config import dictConfig

dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(message)s',
        }
    },
    'handlers': {
        'file': {
            'level': 'INFO',                        # DEBUG, INFO, WARNING, ERROR, CRITICAL, NOTSET
            'class': 'logging.FileHandler',
            'filename': 'debug.log',
            'formatter': 'default',
        },
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['file']
    }
})


def initialize():
    global myList
    myList = {
        "api_parse_cnt_bithumb": 0,     # api_parse_cnt 산출을 위한 전역변수 셋업
        "api_parse_cnt_binance": 0,
        "hts_wallet": 'Binance',
        "hts_market": 'Binance',        # (bithumb/binance) bithumb일 경우 KRW, binance는 USDT
        "hts_market_currency": 'USDT',   # (KRW/USDT)        bithumb일 경우 KRW, binance는 USDT
        "mode": 'single',                # single / thread
        "market": 'Spot'
        # "timeframes": {'1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M'}
    }