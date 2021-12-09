import json
from pandas import json_normalize
import requests
import datetime
import time

_TIME_INTERVALS_ = 0.08

def FormatDatetoURL(to):
    #Datetime을 URL FORM으로 만들기
    if to[0] != "&":
        temp = "&to=" + to
        return temp
    else:
        return to

class Requester:
    def __init__(self):
        self.headers = {"Accept": "application/json"}

    ### seeAllTickers : Upbit에서 거래되는 Ticker 정보 추출
    def seeAllTickers(self):
        url = "https://api.upbit.com/v1/market/all?isDetails=false"
        response = requests.request("GET", url, headers=self.headers)
        return json_normalize(json.loads(response.text))

    ### seePrices : 봉차트 Upbit 서버에서 가져오기
    def seePrices_1Day(self, ticker="KRW-BTC", to=datetime.datetime.now(), count="200"): #출력 type: <List>
        #아무 파라미터 없을 시 "KRW-BTC", 현재 시점 1일봉~200일
        #to는 "yyyy-MM-yy HH:mm:ss" 포멧으로 입력받을 것
        daysCandleData = []
        today_time = to.replace(hour=0, minute=0, second=0, microsecond=0)

        while True:
            today_time_trimmed = str(today_time)
            to_form = FormatDatetoURL(today_time_trimmed)
            url = "https://api.upbit.com/v1/candles/days?market=" + ticker + to_form + "&count="+count

            #response 받아와서 list로 변환하고 return 값인 daysCandleData에 append하는 과정
            response = requests.request("GET", url, headers=self.headers)
            data = json_normalize(json.loads(response.text))
            data.drop('timestamp', inplace=True, axis=1)
            daysCandleData.append(data)
            time.sleep(_TIME_INTERVALS_)

            #변환된 list의 갯수 확인
            if len(data) < 200:
                break
            else:
                today_time = today_time - datetime.timedelta(days=200)
        return daysCandleData # 복수의 Dataframe들로 구성된 List로 반환
    def seePrices_1Week(self, ticker="KRW-BTC", to=datetime.datetime.now(), count="200"): #출력 type: <List>
        #아무 파라미터 없을 시 "KRW-BTC", 현재 시점 1주일봉~200일
        # to는 "yyyy-MM-yy HH:mm:ss" 포멧으로 입력받을 것
        weeksCandleData = []
        today_time = to.replace(hour=0, minute=0, second=0, microsecond=0)

        while True:
            today_time_trimmed = str(today_time)
            to_form = FormatDatetoURL(today_time_trimmed)
            url = "https://api.upbit.com/v1/candles/weeks?market=" + ticker + to_form + "&count="+count

            # response 받아와서 list로 변환하고 return 값인 daysCandleData에 append하는 과정
            response = requests.request("GET", url, headers=self.headers)
            data = json_normalize(json.loads(response.text))
            data.drop('timestamp', inplace=True, axis=1)
            weeksCandleData.append(data)
            time.sleep(_TIME_INTERVALS_)

            #변환된 list의 갯수 확인
            if len(data) < 200:
                break
            else:
                today_time = today_time - datetime.timedelta(weeks=200)
        return weeksCandleData # 복수의 Dataframe들로 구성된 List로 반환
    def seePrices_1Month(self, ticker="KRW-BTC", to=datetime.datetime.now(), count="200"): #출력 type: <List>
        #아무 파라미터 없을 시 "KRW-BTC", 현재 시점 1달봉~200일
        # to는 "yyyy-MM-yy HH:mm:ss" 포멧으로 입력받을 것
        monthsCandleData = []
        today_time = to.replace(hour=0, minute=0, second=0, microsecond=0)

        while True:
            today_time_trimmed = str(today_time)
            to_form = FormatDatetoURL(today_time_trimmed)
            url = "https://api.upbit.com/v1/candles/months?market=" + ticker + to_form + "&count=" + count

            # response 받아와서 list로 변환하고 return 값인 daysCandleData에 append하는 과정
            response = requests.request("GET", url, headers=self.headers)
            data = json_normalize(json.loads(response.text))
            data.drop('timestamp', inplace=True, axis=1)
            monthsCandleData.append(data)
            time.sleep(_TIME_INTERVALS_)

            # 변환된 list의 갯수 확인
            if len(data) < 200:
                break
            else:
                today_time = today_time - datetime.timedelta(months=200)
        return monthsCandleData  # 복수의 Dataframe들로 구성된 List로 반환
    def seePrices_Minutes(self, ticker="KRW-BTC", unit=60, to=datetime.datetime.now(), count="200"): #출력 type: <List>
        #아무 파라미터 없을 시 "KRW-BTC", 현재 시점 1달봉~200일
        # to는 "yyyy-MM-yy HH:mm:ss" 포멧으로 입력받을 것
        minutesCandleData = []
        today_time = to.replace(year=2021, month=12, day=5, hour= 19, minute=0, second=0, microsecond=0)

        while True:
            today_time_trimmed = str(today_time)
            to_form = FormatDatetoURL(today_time_trimmed)
            url = "https://api.upbit.com/v1/candles/minutes/" + str(unit) + "?market=" + ticker + to_form + "&count=" + count

            # response 받아와서 list로 변환하고 return 값인 daysCandleData에 append하는 과정
            response = requests.request("GET", url, headers=self.headers)
            data = json_normalize(json.loads(response.text))
            data.drop('timestamp', inplace=True, axis=1)
            data.drop('unit', inplace=True, axis=1)
            minutesCandleData.append(data)
            time.sleep(_TIME_INTERVALS_)

            # 변환된 list의 갯수 확인
            if len(data) < 200:
                break
            else:
                today_time = today_time - datetime.timedelta(minutes=200*unit)
        return minutesCandleData  # 복수의 Dataframe들로 구성된 List로 반환
