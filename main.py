import datetime
import talib
import index
import requester
import coinDB
from backtesting import Backtest



requester = requester.Requester()
dbsearcher = coinDB.dbSearcher()


trading_tickers = ['KRW-BTC', 'KRW-ETH', 'KRW-NEO', 'KRW-MTL', 'KRW-LTC', 'KRW-XRP', 'KRW-ETC', 'KRW-OMG', 'KRW-SNT', 'KRW-WAVES', 'KRW-XEM', 'KRW-QTUM', 'KRW-LSK', 'KRW-STEEM', 'KRW-XLM', 'KRW-ARDR', 'KRW-ARK', 'KRW-STORJ', 'KRW-GRS', 'KRW-REP', 'KRW-ADA', 'KRW-SBD', 'KRW-POWR', 'KRW-BTG', 'KRW-ICX', 'KRW-EOS', 'KRW-TRX', 'KRW-SC', 'KRW-ONT', 'KRW-ZIL', 'KRW-POLY', 'KRW-ZRX', 'KRW-LOOM', 'KRW-BCH', 'KRW-BAT', 'KRW-IOST', 'KRW-RFR', 'KRW-CVC', 'KRW-IQ', 'KRW-IOTA', 'KRW-MFT', 'KRW-ONG', 'KRW-GAS', 'KRW-UPP', 'KRW-ELF', 'KRW-KNC', 'KRW-BSV', 'KRW-THETA', 'KRW-QKC', 'KRW-BTT', 'KRW-MOC', 'KRW-ENJ', 'KRW-TFUEL', 'KRW-MANA', 'KRW-ANKR', 'KRW-AERGO', 'KRW-ATOM', 'KRW-TT', 'KRW-CRE', 'KRW-MBL', 'KRW-WAXP', 'KRW-HBAR', 'KRW-MED', 'KRW-MLK', 'KRW-STPT', 'KRW-ORBS', 'KRW-VET', 'KRW-CHZ', 'KRW-STMX', 'KRW-DKA', 'KRW-HIVE', 'KRW-KAVA', 'KRW-AHT', 'KRW-LINK', 'KRW-XTZ', 'KRW-BORA', 'KRW-JST', 'KRW-CRO', 'KRW-TON', 'KRW-SXP', 'KRW-HUNT', 'KRW-PLA', 'KRW-DOT', 'KRW-SRM', 'KRW-MVL', 'KRW-STRAX', 'KRW-AQT', 'KRW-GLM', 'KRW-SSX', 'KRW-META', 'KRW-FCT2', 'KRW-CBK', 'KRW-SAND', 'KRW-HUM', 'KRW-DOGE', 'KRW-STRK', 'KRW-PUNDIX', 'KRW-FLOW', 'KRW-DAWN', 'KRW-AXS', 'KRW-STX', 'KRW-XEC', 'KRW-SOL', 'KRW-MATIC', 'KRW-NU', 'KRW-AAVE', 'KRW-1INCH', 'KRW-ALGO']
print(trading_tickers)


## 코인 정보 저장하는 부분 (완료)
#coinDB.dbSearcher().appendonDB_marketInfo(requester.seeAllTickers())

## 코인 데이터 가져오기 ( 주석 해제하여 가져오기 )
for tickers in trading_tickers:
    ## 월봉 데이터 가져오기 (완료)
    #monthData = requester.seePrices_1Month(ticker=tickers)
    #coinDB.dbSearcher().appendonDB_candles_months(monthData)

    ## 주봉 데이터 가져오기 (완료)
    #weekData = requester.seePrices_1Week(ticker=tickers)
    #coinDB.dbSearcher().appendonDB_candles_weeks(weekData)

    ## 일봉 데이터 가져오기 (완료)
    #dayData = requester.seePrices_1Day(ticker=tickers)
    #coinDB.dbSearcher().appendonDB_candles_days(dayData)

    ##240분봉 데이터 가져오기 (완료)
    #min240Data = requester.seePrices_Minutes(ticker=tickers, unit=240)
    #coinDB.dbSearcher().appendonDB_candles_minutes(min240Data, unit=240)

    ##60분봉 데이터 가져오기 (완료)
    #min60Data = requester.seePrices_Minutes(ticker=tickers, unit=60)
    #coinDB.dbSearcher().appendonDB_candles_minutes(min60Data, unit=60)

    ##10분봉 데이터 가져오기 (완료)
    #min10Data = requester.seePrices_Minutes(ticker=tickers, unit=10)
    #coinDB.dbSearcher().appendonDB_candles_minutes(min10Data, unit=10)

    ##5분봉 데이터 가져오기 (완료)
    #min5Data = requester.seePrices_Minutes(ticker=tickers, unit=5)
    #coinDB.dbSearcher().appendonDB_candles_minutes(min5Data, unit=5)

    ##3분봉 데이터 가져오기
    #min3Data = requester.seePrices_Minutes(ticker=tickers, unit=3)
    #coinDB.dbSearcher().appendonDB_candles_minutes(min3Data, unit=3)

    ##1분봉 데이터 가져오기
    #min1Data = requester.seePrices_Minutes(ticker=tickers, unit=1)
    #coinDB.dbSearcher().appendonDB_candles_minutes(min1Data, unit=1)

    print(tickers + " is saved on DB!")


## DB 연결 테스트하는 코드
#connection = pymysql.connect(host='localhost', port=3306, user='root', password='Dares765!', db='coin-db', charset='utf8', autocommit=True, cursorclass=pymysql.cursors.DictCursor)
#cursor = connection.cursor()
#sql = "SELECT * FROM `candles_months`;"
#cursor.execute(sql)
#result = cursor.fetchall()
#connection.close()
#print(result[0]['opening_price'])


### dbsearcher 클래스 - 캔들 가져오기 사용법 ###
#dbsearcher.get_1Day_Candle_range(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=11, day=1), datetime_to=datetime.datetime(year=2021, month=12, day=1))
#dbsearcher.get_1Day_Candle_fromcounts(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=10, day=1), counts=9)
#dbsearcher.get_1Day_Candle_tocounts(tickers="KRW-BTC", datetime_to=datetime.datetime(year=2021, month=12, day=1), counts=26)

#dbsearcher.get_1Week_Candle_range(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=5, day=1), datetime_to=datetime.datetime(year=2021, month=12, day=1))
#dbsearcher.get_1Week_Candle_fromcounts(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=5, day=1), counts=9)
#dbsearcher.get_1Week_Candle_tocounts(tickers="KRW-BTC", datetime_to=datetime.datetime(year=2021, month=5, day=1), counts=26)

#dbsearcher.get_1Month_Candle_range(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=5, day=1), datetime_to=datetime.datetime(year=2021, month=12, day=1))
#dbsearcher.get_1Month_Candle_fromcounts(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=5, day=1), counts=5)
#dbsearcher.get_1Month_Candle_tocounts(tickers="KRW-BTC", datetime_to=datetime.datetime(year=2021, month=5, day=1), counts=11)

df = dbsearcher.get_Minutes_Candle_range(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=4, day=1, hour=10, minute=10), datetime_to=datetime.datetime(year=2021, month=5, day=1, hour=12, minute=20), unit=60)
#dbsearcher.get_Minutes_Candle_fromcounts(tickers="KRW-BTC", datetime_from=datetime.datetime(year=2021, month=5, day=1, hour=10, minute=30), counts=5, unit=240)
#dbsearcher.get_Minutes_Candle_tocounts(tickers="KRW-BTC", datetime_to=datetime.datetime(year=2021, month=11, day=14, hour=9, minute=0), counts=5000, unit=10)


### dataframe을 backtesting 사용 가능하게 form 바꿈
df = dbsearcher.form_backtesting(df)
print(df)


bt = Backtest(df, index.SmaCross, cash=100_000_000_000, commission=.002)
stats = bt.run()
print(stats)
bt.plot()