import datetime
from dateutil.relativedelta import relativedelta
import pymysql
from sqlalchemy import create_engine
import pandas as pd

class dbSearcher:
    def __init__(self):
        self.coin_db = pymysql.connect(
            user='root',
            passwd='Dares765!',
            host='127.0.0.1',
            db='coin-db',
            charset='utf8'
        )
        self.engine = create_engine("mysql+pymysql://root:" + "Dares765!" + "@127.0.0.1:3306/coin-db?charset=utf8", encoding='utf-8')

    ### appendonDB: DB에 데이터 추가하기 (INSERT)
    #requester.py에서 데이터를 받아온 데이터를 DB에 저장함
    def appendonDB_candles_months(self, monthData, condition='append'):
        #monthData<List> 입력받아서 MYSQL DB에 저장
        for item in monthData:
            item.fillna(0)
            item.to_sql(name='candles_months', con=self.engine, if_exists=condition, index=False)
        return
    def appendonDB_candles_weeks(self, weekData, condition='append'):
        #weekData<List> 입력받아서 MYSQL DB에 저장
        for item in weekData:
            item.fillna(0)
            item.to_sql(name='candles_weeks', con=self.engine, if_exists=condition, index=False)
        return
    def appendonDB_candles_days(self, dayData, condition='append'):
        #dayData<List> 입력받아서 MYSQL DB에 저장
        for item in dayData:
            item.fillna(0)
            item.to_sql(name='candles_days', con=self.engine, if_exists=condition, index=False)
        return
    def appendonDB_candles_minutes(self, minuteData, condition='append', unit=60):
        #minuteData<List> 입력받아서 MYSQL DB에 저장
        for item in minuteData:
            item.fillna(0)
            item.to_sql(name='candles_'+str(unit)+'minutes', con=self.engine, if_exists=condition, index=False)
        return
    def appendonDB_marketInfo(self, item):
        item.to_sql(name='ticker_info', con=self.engine, if_exists='append', index=False)

    ### get: DB로부터 캔들 정보 받아오기 (GET)
    # ___range: datetime_from ~ datetime_to (inclusive) 사이의 캔들 데이터 추출 = 반환형(Dataframe)  --  권장...
    # ___fromcounts: datetime_from부터 counts 수만큼 (inclusive) 캔들 데이터 추출 = 반환형(Dataframe)  --  빠진 캔들이 있을 수 있으므로 count보다 실제 더 적을수도 있음!
    # ___tocounts: ~ datetime_to까지의 counts 수만큼 (inclusive) 캔들 데이터 추출 = 반환형(Dataframe)  --  빠진 캔들이 있을 수 있으므로 count보다 실제 더 적을수도 있음!
    def get_1Day_Candle_range(self, tickers, datetime_from, datetime_to):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d 09:00:00")
        _to = datetime_to.strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_days WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        return pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
    def get_1Day_Candle_fromcounts(self, tickers, datetime_from, counts):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d 09:00:00")
        _to = (datetime_from + datetime.timedelta(days=counts)).strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_days WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<'" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        return pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
    def get_1Day_Candle_tocounts(self, tickers, datetime_to, counts):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = (datetime_to - datetime.timedelta(days=counts)).strftime("%Y-%m-%d 09:00:00")
        _to = datetime_to.strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_days WHERE candle_date_time_kst>'" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        return pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()

    def get_1Week_Candle_range(self, tickers, datetime_from, datetime_to):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d 09:00:00")
        _to = datetime_to.strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_weeks WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df

    def get_1Week_Candle_fromcounts(self, tickers, datetime_from, counts):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d 09:00:00")
        _to = (datetime_from + datetime.timedelta(weeks=counts)).strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_weeks WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<'" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df
    def get_1Week_Candle_tocounts(self, tickers, datetime_to, counts):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = (datetime_to - datetime.timedelta(weeks=counts)).strftime("%Y-%m-%d 09:00:00")
        _to = datetime_to.strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_weeks WHERE candle_date_time_kst>'" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df

    def get_1Month_Candle_range(self, tickers, datetime_from, datetime_to):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d 09:00:00")
        _to = datetime_to.strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_months WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df
    def get_1Month_Candle_fromcounts(self, tickers, datetime_from, counts):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d 09:00:00")
        _to = (datetime_from + relativedelta(months=counts)).strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_months WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<'" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df
    def get_1Month_Candle_tocounts(self, tickers, datetime_to, counts):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = (datetime_to - relativedelta(months=counts)).strftime("%Y-%m-%d 09:00:00")
        _to = datetime_to.strftime("%Y-%m-%d 09:00:00")
        sql = "SELECT * FROM candles_months WHERE candle_date_time_kst>'" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df

    def get_Minutes_Candle_range(self, tickers, datetime_from, datetime_to, unit=1):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d %H:%M:00")
        _to = datetime_to.strftime("%Y-%m-%d %H:%M:00")
        sql = "SELECT * FROM candles_" + str(unit) + "minutes WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df
    def get_Minutes_Candle_fromcounts(self, tickers, datetime_from, counts, unit=1):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = datetime_from.strftime("%Y-%m-%d %H:%M:00")
        _to = (datetime_from + datetime.timedelta(minutes=counts*unit)).strftime("%Y-%m-%d %H:%M:00")
        sql = "SELECT * FROM candles_" + str(unit) + "minutes WHERE candle_date_time_kst>='" + _from + "' AND candle_date_time_kst<'" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df
    def get_Minutes_Candle_tocounts(self, tickers, datetime_to, counts, unit=1):
        self.coin_db = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db',
                                     charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
        cursor = self.coin_db.cursor()
        _from = (datetime_to - datetime.timedelta(minutes=counts*unit)).strftime("%Y-%m-%d %H:%M:00")
        _to = datetime_to.strftime("%Y-%m-%d %H:%M:00")
        sql = "SELECT * FROM candles_" + str(unit) + "minutes WHERE candle_date_time_kst>'" + _from + "' AND candle_date_time_kst<='" + _to + "' AND market='" + tickers + "'"
        print(sql)
        cursor.execute(sql)
        result = cursor.fetchall()
        df = pd.DataFrame(result).drop_duplicates().sort_values(by=['candle_date_time_kst'], ascending=True).reset_index()
        df.set_index(df['candle_date_time_kst'], inplace=True)
        return df

    def form_backtesting(self, df):
        #backtesting.py 사용 시 필요한 format으로 바꿈
        temp = pd.DataFrame()
        temp['Open'] = df['opening_price']
        temp['High'] = df['high_price']
        temp['Low'] = df['low_price']
        temp['Close'] = df['trade_price']
        temp['Volumn'] = df['candle_acc_trade_volume']
        temp.Open = temp.Open.astype(float)
        temp.High = temp.High.astype(float)
        temp.Low = temp.Low.astype(float)
        temp.Close = temp.Close.astype(float)
        temp.Volumn = temp.Volumn.astype(float)
        return temp

    def closeConnectionDB(self):
        self.coin_db.close()