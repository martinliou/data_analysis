#!/usr/bin/python
# -*- coding: utf-8 -*-

import boto3
import os
import json
from datetime import datetime, timedelta
import pysftp
from boto3.dynamodb.conditions import Key
from StockData import HEALTH_DIAGNOSIS_TABLE, IBDA

tw50 = ['1101', '1102', '1216', '1301', '1303', '1326', '1402', '2002', '2105', '2207', '2227', '2301', '2303', '2308', '2317', '2327', '2330', '2352', '2357', '2382', '2395', '2408', '2412', '2454', '2474', '2609', '2610',
        '2633', '2801', '2823', '2880', '2881', '2882', '2883', '2884', '2885', '2886', '2887', '2888', '2890', '2891', '2892', '2912', '3008', '3045', '3711', '4904', '4938', '5871', '5876', '5880', '6505', '9904', '9910']
tw100 = ['2330', '2317', '3008', '2454', '3045', '2308', '3711', '2408', '2382', '4904', '2327', '2395', '2303', '2474', '4938', '2357', '3034', '6669', '2379', '2492', '4958', '2301', '2409', '2345', '6415', '6239', '3481', '2354', '2356', '2324', '2377', '3231', '2337', '2360', '2344', '3702', '2385', '6409', '2347', '3037', '3044', '3406', '5269', '2353', '2313', '6176', '6213', '3552', '2449',
         '6269', '2383', '2352', '2376', '2404', '2448', '2451', '2059', '6456', '3533', '3443', '2456', '3706', '3023', '2439', '8046', '3005', '6278', '3653', '6285', '2498', '3665', '3376', '2441', '2458', '6271', '3019', '3189', '6412', '4915', '8150', '3036', '6116', '6214', '2362', '4943', '2373', '6230', '2312', '2455', '5434', '2392', '8016', '3576', '5469', '3673', '6414', '4935', '5388', '2367', '0050']
idx50 = ['0050']
IGNORE_DATE = ['20200121', '20200123', '20200129']
BEGIN_DATE = '20181001'
DEBUG = False
REFRESH_DAYS = 20


class Analyzer(object):
    def __init__(self):
        self.ibda = IBDA()
        self.validSymbols = set()
        self.allTwseSymbols = list()
        self.twseScore = dict()

    def signalTest(self):
        dynamodb = boto3.resource('dynamodb')
        todaySymbolFile = '{}.json'.format(
            datetime.today().strftime('%Y%m%d'))

        allSymbols = list()
        if os.path.exists(todaySymbolFile):
            with open(todaySymbolFile, 'r') as fr:
                data = fr.read()
                allSymbols = json.loads(data)

        else:
            allSymbols = self.ibda.twFinProdLists()
            with open(todaySymbolFile, 'w') as fw:
                fw.write(json.dumps(allSymbols))

        for item in allSymbols:
            self.allTwseSymbols.append(item)

            if item.get('cfi').startswith('CEO') or \
                    item.get('cfi').startswith('CMX') or \
                    item.get('cfi').startswith('EDS'):
                continue

            self.validSymbols.add(item.get('coid'))

        scoreFile = 'score.json'

        if os.path.exists(scoreFile):
            with open(scoreFile, 'r') as fr:
                self.twseScore = json.loads(fr.read())

            for k, item in enumerate(self.allTwseSymbols):
                symbolData = list()
                coid = item.get('coid')
                wprcdData = dict()
                beginDate = BEGIN_DATE

                if coid in self.twseScore:
                    symbolData = self.twseScore[coid]

                    if len(symbolData) is 0:
                        print('{} has no data'.format(coid))
                        continue

                    lastDate = symbolData[len(symbolData) - 1].get('date')

                    beginDate = lastDate

                table = dynamodb.Table('WPRCD')
                results = table.query(
                    KeyConditionExpression=Key('COID').eq(
                        coid) & Key('MDATE').gt(beginDate)
                )['Items']

                for result in results:
                    try:
                        wprcdData[result.get('MDATE')] = {
                            'close': float(result.get('CLOSE')),
                            'open': float(result.get('OPEN'))
                        }
                    except:
                        continue

                table = dynamodb.Table(HEALTH_DIAGNOSIS_TABLE)
                results = table.query(
                    KeyConditionExpression=Key('COID').eq(
                        coid) & Key('MDATE').gt(beginDate)
                )['Items']

                fundamentalFields = [
                    'eps', 'roe', 'pm', 'opm', 'ni',
                    'mi', 'mmi', 'dy', 'per', 'pbr'
                ]
                technicalFields = [
                    'rsi', 'osi', 'kd', 'ma',
                    'ltp', 'hcp', 'ls', 'maa', 'bias'
                ]
                chipFields = [
                    'icb', 'fcb', 'cc', 'fr',
                    'ir', 'tr'
                ]

                for item in results:
                    fundamentals = []
                    for field in fundamentalFields:
                        fundamentals.append(int(item.get('f_' + field)))

                    technicals = []
                    for field in technicalFields:
                        technicals.append(int(item.get('t_' + field)))

                    chips = []
                    for field in chipFields:
                        chips.append(int(item.get('c_' + field)))

                    techScore = sum(technicals)
                    chipScore = sum(chips)
                    fundScore = sum(fundamentals)
                    mdate = item.get('MDATE')
                    closePrice = 0
                    openPrice = 0

                    if mdate in wprcdData:
                        closePrice = wprcdData[mdate]['close']
                        openPrice = wprcdData[mdate]['open']

                    symbolData.append({
                        'date': item.get('MDATE'),
                        'techScore': techScore,
                        'fundScore': fundScore,
                        'chipScore': chipScore,
                        'ma': int(item.get('t_ma')),
                        'open': openPrice,
                        'close': closePrice
                    })

                self.twseScore[coid] = symbolData

            with open(scoreFile, 'w') as fw:
                fw.write(json.dumps(self.twseScore))

        else:
            table = dynamodb.Table(HEALTH_DIAGNOSIS_TABLE)
            for k, item in enumerate(self.allTwseSymbols):

                symbolData = list()
                coid = item.get('coid', str())
                wprcdDicts = dict()
                wprcdData = self.ibda.fetchSpecificTopicIDData('WPRCD', coid)
                wprcdData = sorted(wprcdData, key=lambda x:
                                   x['MDATE'], reverse=False)

                for wprcd in wprcdData:
                    wprcdDicts[wprcd.get('MDATE')] = {
                        'close': wprcd.get('CLOSE'),
                        'open': wprcd.get('OPEN')
                    }

                results = table.query(
                    KeyConditionExpression=Key('COID').eq(
                        coid) & Key('MDATE').gte(BEGIN_DATE)
                )['Items']

                fundamentalFields = [
                    'eps', 'roe', 'pm', 'opm', 'ni',
                    'mi', 'mmi', 'dy', 'per', 'pbr'
                ]
                technicalFields = [
                    'rsi', 'osi', 'kd', 'ma',
                    'ltp', 'hcp', 'ls', 'maa', 'bias'
                ]
                chipFields = [
                    'icb', 'fcb', 'cc', 'fr',
                    'ir', 'tr'
                ]

                for item in results:

                    if item.get('MDATE') < BEGIN_DATE:
                        continue

                    if item.get('MDATE') in IGNORE_DATE:
                        continue

                    fundamentals = []
                    for field in fundamentalFields:
                        fundamentals.append(int(item.get('f_' + field)))

                    technicals = []
                    for field in technicalFields:
                        technicals.append(int(item.get('t_' + field)))

                    chips = []
                    for field in chipFields:
                        chips.append(int(item.get('c_' + field)))

                    techScore = sum(technicals)
                    chipScore = sum(chips)
                    fundScore = sum(fundamentals)
                    mdate = item.get('MDATE')
                    closePrice = 0
                    openPrice = 0

                    if mdate in wprcdDicts:
                        closePrice = wprcdDicts[mdate]['close']
                        openPrice = wprcdDicts[mdate]['open']

                    symbolData.append({
                        'date': item.get('MDATE'),
                        'techScore': techScore,
                        'fundScore': fundScore,
                        'chipScore': chipScore,
                        'ma': int(item.get('t_ma')),
                        'open': openPrice,
                        'close': closePrice
                    })

                self.twseScore[coid] = symbolData

            with open(scoreFile, 'w') as fw:
                fw.write(json.dumps(self.twseScore))

        self.calcMovingAverage()

    def getFocusStockNo(self):
        retData = list()
        with open('major 200.txt', 'r') as fr:
            lines = fr.readlines()

            for line in lines:
                symbol = line.strip()

                if symbol not in self.validSymbols:
                    continue

                retData.append(symbol)

        return retData

    def calcMovingAverage(self, startDate=BEGIN_DATE, isSignal=False):

        symbols = self.getFocusStockNo()
        totalIn = 0
        totalOut = 0
        totalDiff = 0
        totalSuc = 0
        totalCount = 0
        profits = list()
        records = list()

        for coid, allItems in self.twseScore.items():
            entryFlag = True
            loseFlag = False
            entryRecord = dict()
            leaveRecord = dict()
            coidDiff = 0
            coidIn = 0
            coidOut = 0

            if coid not in symbols:
                continue

            items = list()
            for item in allItems:
                items.append(item)

            for k, item in enumerate(items):
                try:

                    curScore = item.get('chipScore') + item.get('techScore')

                    msg = '股票代碼: {} 分數 {} 日期 {} MA {} 開盤價 {} 收盤價 {}\r'.format(
                        coid, curScore, item.get('date'), item.get('ma'), item.get('open'), item.get('close'))


                    # 如果分數介於5-6分間且移動平均線是上揚狀況
                    if (curScore >= 2 and curScore <= 4) and item.get('ma') == 1 and entryFlag and not loseFlag:

                        records.append({
                            'symbol': coid,
                            'signal': 0,
                            'score': curScore,
                            'price': item.get('close'),
                            'day': item.get('date')
                        })

                        entryRecord = {
                            'close': item.get('close'),
                            'score': curScore,
                            'date': items[k + 1].get('date'),
                            'open': items[k + 1].get('open') if k < len(items) - 1 else -1,
                            'signal': 0,
                            'ma': item.get('ma')
                        }

                        entryFlag = False

                    if curScore >= 13 and not entryFlag and not loseFlag:

                        records.append({
                            'symbol': coid,
                            'signal': 1,
                            'score': curScore,
                            'price': item.get('close'),
                            'day': item.get('date')
                        })

                        leaveRecord = {
                            'close': item.get('close'),
                            'score': curScore,
                            'date': items[k + 1].get('date'),
                            'open': items[k + 1].get('open') if k < len(items) - 1 else -1,
                            'signal': 1,
                            'ma': item.get('ma')
                        }

                        entryFlag = True

                    if loseFlag and curScore >= 8:
                        loseFlag = False

                    if curScore <= 0 and item.get('ma') == 0 and not entryFlag and not loseFlag:
                        records.append({
                            'symbol': coid,
                            'signal': 1,
                            'score': curScore,
                            'price': item.get('close'),
                            'day': item.get('date')
                        })

                        leaveRecord = {
                            'close': item.get('close'),
                            'score': curScore,
                            'date': items[k + 1].get('date'),
                            'open': items[k + 1].get('open') if k < len(items) - 1 else -1,
                            'signal': 2,
                            'ma': item.get('ma')
                        }

                        loseFlag = True
                        entryFlag = True

                    if entryRecord and leaveRecord:
                        if leaveRecord.get('open') != -1 and entryRecord.get('open') != -1:
                            totalCount += 1

                            if leaveRecord.get('open') > entryRecord.get('open'):
                                totalSuc += 1

                            totalIn += entryRecord.get('open')
                            coidIn += entryRecord.get('open')
                            totalOut += leaveRecord.get('open')
                            coidOut += leaveRecord.get('open')
                            curDiff = round(leaveRecord.get('open') -
                                            entryRecord.get('open'), 2)
                            coidDiff += curDiff
                            msg = '股票代碼{} 買入價{} 買入日{} 前日進場分數{} 賣出價{} 賣出日{} 前日出場分數{} 差價{} 訊號：{}'.format(
                                coid,
                                entryRecord.get('open'),
                                entryRecord.get('date'),
                                entryRecord.get('score'),
                                leaveRecord.get('open'),
                                leaveRecord.get('date'),
                                leaveRecord.get('score'),
                                curDiff,
                                '停利' if leaveRecord.get(
                                    'signal') == 1 else '停損'
                            )

                            if DEBUG:
                                print(msg)

                        entryRecord = leaveRecord = dict()

                    if entryRecord and not leaveRecord and k == len(items) - 1:
                        if entryRecord.get('open') != -1:
                            totalIn += entryRecord.get('open')
                            coidIn += entryRecord.get('open')
                            totalOut += item.get('open')
                            coidOut += item.get('open')
                            curDiff = round(item.get('open') -
                                            entryRecord.get('open'), 2)
                            coidDiff += curDiff

                            records.append({
                                'symbol': coid,
                                'signal': 2,
                                'score': curScore,
                                'price': item.get('close'),
                                'day': item.get('date')
                            })

                except Exception as e:
                    continue
            try:
                profit = (coidOut - coidIn) / coidIn
                profits.append(profit)
            except:
                pass

            totalDiff += coidDiff

        msg = '買入價：{}, 賣出價：{}, 股價差異：{}, 全部獲利率(價差)：{}%， 平均獲利率(平均價差)：{}%，勝率：{}%，進出場資料量：{}'.format(
            round(totalIn, 2),
            round(totalOut, 2),
            round(totalDiff, 2),
            round(100 * totalOut / totalIn - 100, 2),
            round(100 * sum(profits) / len(profits), 2),
            round(100 * totalSuc / totalCount, 2),
            totalCount
        )

        txtFileName = 'statistic_ma.txt'
        with open(txtFileName, 'w') as fw:
            fw.write('{}\r'.format(msg))
            if isSignal:
                dynamodb = boto3.resource('dynamodb')
                table = dynamodb.Table('ai_mv_signals')
                fw.close()
                mdate = str()

                for i in range(REFRESH_DAYS):
                    try:
                        mdate = (datetime.now() - timedelta(days=i)
                                ).strftime('%Y%m%d')

                        items = table.query(
                            ProjectionExpression='COID',
                            KeyConditionExpression=Key('MDATE').eq(mdate)
                        )['Items']

                        for item in items:
                            table.delete_item(
                                Key={
                                    'MDATE': mdate,
                                    'COID': item.get('COID')
                                })
                    except:
                        pass

if __name__ == "__main__":
    analyzer = Analyzer()
    analyzer.signalTest()
