from binance_api_manager import *
from db_manager import *
from datetime import datetime
import time
import sys
import settings
import logging
import schedule # 매분 실행되도록 하기 위한 쓰레드 라이브러리

def store_binance_data(market='spot', timeframes=['1m']):
    logging.info('main_algo started.')
    time_init = datetime.now()
    # print('init time: ' + str(time_init.strftime('%Y-%m-%d %H:%M:%S.%f')))
    binance_api = BinanceAPI(market, timeframes)
    idx_symbol = 0

    # market = settings.myList['market']
    symbols = binance_api.get_tickers()
    symbols_tg = []
    for symbol in symbols:
        if "/USDT" in symbol:
            if "BULL/" not in symbol and "BEAR/" not in symbol and "UP/" not in symbol and "DOWN/" not in symbol:
                symbols_tg.append(symbol)

    # symbols_tg = symbols_tg[336:]
    print(str(idx_symbol) + '\t' + str(len(symbols_tg)) + ' markets // ' + str(symbols_tg))
    logging.info(str(idx_symbol) + '\t ' + str(len(symbols_tg)) + ' markets // ' + str(symbols_tg))
    db_manager = DB_Manager(db_name='binance_usdt_ohlcv')

    for symbol_tg in symbols_tg:
        logging.debug(str(idx_symbol) + '\t' + str(symbol_tg) + ' market checking start!')

        for timeframe in binance_api.timeframes:
            table_name = str(market) + '_' + str(symbol_tg.replace('/', '_')) + '_' + str(timeframe)  # sql 입력을 위해 '/' 텍스트 삭제. 그리고 숫자가 맨 앞에
            dt_db_border = db_manager.parse_dt_border(table_name)  # db 내 가장 신구 데이터 기준시점 조회

            if dt_db_border != False:     # DB 데이터 조회 실패하지 않았다면,

                dt_db_first = datetime.strptime(dt_db_border['first'], '%Y-%m-%d %H:%M:%S')
                dt_db_last = datetime.strptime(dt_db_border['last'], '%Y-%m-%d %H:%M:%S')
                logging.debug(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' border reading success')

                if db_manager.asis_db_validity(dt_db_last, timeframe) != False:         # timeframe 기준 가장 최신의 데이터가 이미 있다면 db 업데이트를 하지 않기 위한 함수
                    logging.debug(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table validity check success')

                    # 바이낸스 데이터 가져오기
                    df_ohlcvs = binance_api.get_ohlcvs(symbol_tg, timeframe)
                    dt_hts_first = datetime.strptime(str(df_ohlcvs['datetime'].min()), '%Y-%m-%d %H:%M:%S')
                    dt_hts_last = datetime.strptime(str(df_ohlcvs['datetime'].max()), '%Y-%m-%d %H:%M:%S')

                    # print('dt_hts_first = ' + str(dt_hts_first))
                    # print('dt_hts_last = ' + str(dt_hts_last))

                    if dt_hts_first <= dt_db_last:  # 신규 데이터와 기존 데이터 사이 time gap 없을 경우
                        # hts에서 가져온 ohlcv 데이터 중 이미 db에 있는 내용은 삭제 후 insert
                        remove_data = df_ohlcvs[df_ohlcvs['datetime'] <= dt_db_last].index
                        df_ohlcvs_droped = df_ohlcvs.drop(remove_data)
                        db_manager.insert_data(table_name, df_ohlcvs_droped)
                        print(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table update success. No time gap. Data amount: ' + str(dt_hts_last - dt_db_last))
                        logging.info(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table update success. No time gap. Data amount: ' + str(dt_hts_last - dt_db_last))

                    else:  # 신규데이터와 기존 데이터 사이의 time gap 존재할 경우
                        # hts 조회 데이터 전체 db에 insert
                        db_manager.insert_data(table_name, df_ohlcvs)
                        timegap = dt_hts_first - dt_db_last
                        print('[WARNING] ' + str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table update success. Time gap existing!! Timegap= ' + str(timegap))
                        logging.warning(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table update success. Time gap existing!! Timegap= ' + str(timegap))

                else:
                    print(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table has the newest data already!')
                    logging.info(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table has the newest data already!')

            else:  # DB 데이터 조회 실패시에는 신규 테이블 생성을 시도해보고, 무조건 데이터 다 넣음
                logging.info(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' table parsing failed. Cannot get "dt_db_border" values.')
                # 신규 테이블 생성을 위한 변수
                dict_fields = {
                    'datetime': 'text',
                    'open': 'real',
                    'high': 'real',
                    'low': 'real',
                    'close': 'real',
                    'volume': 'real'
                }
                db_manager.create_table(table_name, dict_fields)
                logging.info(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' New table creation finished')
                df_ohlcvs = binance_api.get_ohlcvs(symbol_tg, timeframe)
                db_manager.insert_data(table_name, df_ohlcvs)
                logging.info(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' insert data to New table finished.')
                print(str(idx_symbol) + '\t' + str(table_name) + '  \t' + ' new table creation finished.')

            # 검산
            # 검산결과 로그찍기
        idx_symbol = idx_symbol + 1

    time_end = datetime.now()
    time_duration = time_end - time_init
    print('main_algo finished.')
    print('end time: ' + str(time_end.strftime('%Y-%m-%d %H:%M:%S.%f')))
    print('duration: ' + str(time_duration))
    print('\n')
    logging.info('main_algo finished.')
    logging.info('end time: ' + str(time_end.strftime('%Y-%m-%d %H:%M:%S.%f')))
    logging.info('duration: ' + str(time_duration))

def main_algo(timeframes=['1m']):
    store_binance_data('spot', timeframes)
    store_binance_data('future', timeframes)

if __name__ == '__main__':
    print('Program Started...')
    settings.initialize()  # 전역변수 초기화
    logging.info("Program Started...!")

    timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    # timeframes = ['1m']
    main_algo(timeframes)

    timeframes = ['1m']
    # timeframes = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    schedule.every(3).seconds.do(main_algo)                     # 1초마다 실행
    # schedule.every(60).minutes.do(main_algo)                     # 20분마다 실행
    # schedule.every().monday.at("09:00").do(printhelloworld)   # 월요일 09:00분에 실행
    # schedule.every().day.at("09:10").do(job)                  # 매일 09시10분에

    while True:

        try:
            schedule.run_pending()                          # first log line generation
            print(' ', end='\r')
            print('다음 실행까지 ' + time.strftime('%H:%M:%S', time.gmtime(schedule.idle_seconds())) + ' 초 남았습니다.')
        except:
            print('[ERROR] main function has a problem.')
            logging.error('main function has a problem.')
        time.sleep(1)

    print('Program Ended...')
    logging.info("Program Ended...!")