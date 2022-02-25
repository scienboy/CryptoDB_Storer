import os
import sqlite3
from datetime import timedelta, datetime


class DB_Manager():

    def __init__(self, db_name):

        path_dir = './db'
        try:
            os.makedirs(path_dir)
        except OSError:
            if not os.path.isdir(path_dir):
                raise

        self.db_name = db_name
        self.conn = sqlite3.connect("db/" + str(self.db_name) + '.db')  # connection 생성
        self.cur = self.conn.cursor()  # connection을 통해서 cursor를 넣어줘야 함


    def create_table(self, table_name, dict_fields):

        ins_sql = "create table if not exists " + str(table_name) + "("     # 'ins_sql' example: 'create table ?(title text, pubd text, pus text, page integer, re integer)'
        for key, val in dict_fields.items():
            ins_sql = ins_sql + str(key) + ' ' + str(val) + ', '
        ins_sql = ins_sql[:-2]
        ins_sql = ins_sql + ')'

        self.conn = sqlite3.connect("db/" + str(self.db_name) + '.db')  # connection 생성
        self.cur = self.conn.cursor()  # connection을 통해서 cursor를 넣어줘야 함
        self.cur.execute(ins_sql)
        self.conn.commit()
        self.conn.close()  # 커넥션 닫기

    def insert_data(self, table_name, data):
        self.conn = sqlite3.connect("db/" + str(self.db_name) + '.db')  # connection 생성
        self.cur = self.conn.cursor()  # connection을 통해서 cursor를 넣어줘야 함
        for row in data.itertuples():
            ins_sql = "insert into " + str(table_name) + "("
            for key in data.keys():
                ins_sql = ins_sql + str(key) + ", "
            ins_sql = ins_sql[:-2] + ") values (?, ?, ?, ?, ?, ?)"
            self.cur.execute(ins_sql, (str(row[1]), float(row[2]), float(row[3]), float(row[4]), float(row[5]), float(row[6])))
        self.conn.commit()
        self.conn.close()  # 커넥션 닫기

    def parse_dt_border(self, table_name):
        ins_sql_last = 'select max(datetime) from ' + table_name
        ins_sql_first = 'select min(datetime) from ' + table_name

        try:
            self.conn = sqlite3.connect("db/" + str(self.db_name) + '.db')  # connection 생성
            self.cur = self.conn.cursor()  # connection을 통해서 cursor를 넣어줘야 함
            self.cur.execute(ins_sql_first)
            for res in self.cur:
                datetime_first = res

            self.cur.execute(ins_sql_last)
            for res in self.cur:
                datetime_last = res

            self.conn.commit()
            self.conn.close()  # 커넥션 닫기

            if datetime_first[0] == None or datetime_last[0] == None:  # 테이블은 존재하나, 데이터가 없을 때 None이 출력됨
                return False

            datime_border = {'first': datetime_first[0], 'last': datetime_last[0]}
            return datime_border
        except:  # 테이블 자체가 없는 경우(대부분)
            return False

    def asis_db_validity(self, db_datatime_last, timeframe):
        # timeframe 기준 가장 최신의 데이터가 이미 있다면 db 업데이트를 하지 않기 위한 함수

        now = datetime.now() + timedelta(hours=9)       # binance는 현지 시간 기준으로 데이터 제공하는데 반해, 리눅스에서는 datetime.now() 함수가 utc 기준으로 시간을 리턴하기 때문에 보정작업 진행
        cnt = int(timeframe[:-1])
        factor = timeframe[-1]

        if factor == 'm':
            basis = now - timedelta(minutes=cnt)
        elif factor == 'h':
            basis = now - timedelta(hours=cnt)
        elif factor == 'd':
            basis = now - timedelta(days=cnt)
        elif factor == 'w':
            basis = now - timedelta(weeks=cnt)
        elif factor == 'M':
            basis = now - timedelta(weeks=4)
        else:
            basis = 0
            print('wtf!!!')


        # print('now = ' + str(now))
        # print('db_datatime_last = ' + str(db_datatime_last))
        # print('basis = ' + str(basis))

        if db_datatime_last > basis:
            return False
        else:
            return True