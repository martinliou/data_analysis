#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import re
import time
import traceback
import json
import requests
import threading
import re
from bs4 import BeautifulSoup
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from elasticsearch import Elasticsearch, RequestsHttpConnection, helpers
from RtMarketData import DEMO_AWS_ES_HOST
ib = None
nyseReq = dict()
cnseReq = dict()
MARKET_SYMBOL_INDEX = 'market_symbol_lists'
MARKET_BROKER_INDEX = 'market_broker_lists'
SEHK_BASE_URL = 'https://www.interactivebrokers.com/en/index.php?f=2222&exch={}&showcategories=STK&p=&cc=&limit=100&page={}'
SEHK_ZHTW_SYMBOL_URL = 'https://www.ejfq.com/home/tc/tradingview3_360/php/symbols.php?symbol={}'
NYSE_SYMBOL_QUERY_URL = 'https://money.moneydj.com/us/rest/getStockIDFullName/{}'
CYSE_PARSER_URL = 'https://histock.tw/stock/module/stockdata.aspx?m=stocks&mid=13'

forexLists = [
    'USDSGD', 'USDCAD', 'USDZAR', 'USDJPY', 'USDCZK',
    'USDILS', 'USDCHF', 'USDRUB', 'USDSEK', 'USDMXN',
    'USDTRY', 'EURUSD', 'USDHKD', 'GBPUSD', 'AUDUSD',
    'NZDUSD', 'USDCNH', 'USDNOK', 'USDTWD'
]

forexDicts = {
    'USD': {'en': 'United States Dollar', 'tw': '美元'},
    'SGD': {'en': 'Singapore Dollar', 'tw': '新加坡元'},
    'CAD': {'en': 'Canadian dollar', 'tw': '加拿大元'},
    'ZAR': {'en': 'South African Rand', 'tw': '南非幣'},
    'JPY': {'en': 'Japanese Yen', 'tw': '日元'},
    'CZK': {'en': 'Czech Republic Koruna', 'tw': '捷克克朗'},
    'ILS': {'en': 'Israeli new shekel', 'tw': '以色列謝克'},
    'CHF': {'en': 'Swiss franc', 'tw': '瑞士法郎'},
    'RUB': {'en': 'Rubli', 'tw': '俄羅斯盧布'},
    'SEK': {'en': 'Swedish krone', 'tw': '瑞典克朗'},
    'MXN': {'en': 'Mexican Peso', 'tw': '墨西哥披索'},
    'TRY': {'en': 'Turkey Lira', 'tw': '土耳其里拉'},
    'EUR': {'en': 'Euro', 'tw': '歐元'},
    'HKD': {'en': 'HongKong Dollor', 'tw': '港幣'},
    'GBP': {'en': 'Great British Pound', 'tw': '英鎊'},
    'NZD': {'en': 'Australian Dollar', 'tw': '紐西蘭元'},
    'CNH': {'en': 'Chinese Yuan', 'tw': '人民幣'},
    'AUD': {'en': 'Australian Dollar', 'tw': '澳元'},
    'NOK': {'en': 'Norwegian Krone', 'tw': '挪威克朗'},
    'TWD': {'en': 'Taiwan New Dollor', 'tw': '新台幣'}
}

hsiIndex = {
    'symbol': 'HSI',
    'exchange': 'HKFE',
    'currency': 'HKD',
    'sec_type': 'IND',
    'en_name': 'Hang Seng Index',
    'tc_name': '恆生指數',
    'region': 'hk',
    'is_index': 1,
    'cid': 1,
    'long_run': True
}

hscciIndex = {
    'symbol': 'HSCCI',
    'exchange': 'HKFE',
    'currency': 'HKD',
    'sec_type': 'IND',
    'en_name': 'Hang Seng China Affiliated Corp Index',
    'tc_name': '紅籌指數',
    'region': 'hk',
    'is_index': 1,
    'cid': 1,
    'long_run': True
}

hsceiIndex = {
    'symbol': 'HHI.HK',
    'exchange': 'HKFE',
    'currency': 'HKD',
    'sec_type': 'IND',
    'en_name': 'Hang Seng China Enterprises Index',
    'tc_name': '恒生中國企業指數',
    'region': 'hk',
    'is_index': 1,
    'cid': 1,
    'long_run': True
}

sp500Index = {
    'symbol': 'SPX',
    'exchange': 'CBOE',
    'currency': 'USD',
    'sec_type': 'IND',
    'en_name': 'S&P 500 Stock Index',
    'tc_name': '標準普爾500指數',
    'region': 'us',
    'is_index': 1,
    'cid': 1,
    'long_run': True
}
ndxIndex = {
    'symbol': 'COMP',
    'exchange': 'NASDAQ',
    'currency': 'USD',
    'sec_type': 'IND',
    'en_name': 'NASDAQ 100 Stock Index',
    'tc_name': '那斯達克綜合指數',
    'region': 'us',
    'is_index': 1,
    'cid': 1,
    'long_run': True
}
djiIndex = {
    'symbol': 'INDU',
    'exchange': 'CME',
    'currency': 'USD',
    'sec_type': 'IND',
    'en_name': 'Dow Jones Industrial Average',
    'tc_name': '道瓊工業平均指數',
    'region': 'us',
    'is_index': 1,
    'cid': 1,
    'long_run': True
}
soxIndex = {
    'symbol': 'SOX',
    'exchange': 'PHLX',
    'currency': 'USD',
    'sec_type': 'IND',
    'en_name': 'Semiconductor Index',
    'tc_name': '費城半導體',
    'region': 'us',
    'is_index': 1,
    'cid': 1,
    'long_run': True
}
n225Index = {
    'symbol': 'N225',
    'exchange': 'OSE.JPN',
    'currency': 'JPY',
    'sec_type': 'IND',
    'en_name': 'Nikkei 225',
    'tc_name': '日經225指數',
    'region': 'jp',
    'is_index': 1,
    'cid': 0,
    'long_run': True
}
topxIndex = {
    'symbol': 'TOPX',
    'exchange': 'OSE.JPN',
    'currency': 'JPY',
    'sec_type': 'IND',
    'en_name': 'TOPIX Index',
    'tc_name': '東證期貨',
    'region': 'jp',
    'is_index': 1,
    'cid': 0,
    'long_run': True
}
cacIndex = {
    'symbol': 'CAC40',
    'exchange': 'MONEP',
    'currency': 'EUR',
    'sec_type': 'IND',
    'en_name': 'CAC 40',
    'tc_name': '法國 CAC',
    'region': 'fr',
    'is_index': 1,
    'cid': 0,
    'long_run': True
}
aexIndex = {
    'symbol': 'AEXSET',
    'exchange': 'FTA',
    'currency': 'EUR',
    'sec_type': 'IND',
    'en_name': 'AEX EDSP',
    'tc_name': '荷蘭 AEXSET',
    'region': 'nl',
    'is_index': 1,
    'cid': 0,
    'long_run': True
}
bfxIndex = {
    'symbol': 'BFX',
    'exchange': 'BELFOX',
    'currency': 'EUR',
    'sec_type': 'IND',
    'en_name': 'BEL 20 Index',
    'tc_name': '比利時 BFX',
    'region': 'be',
    'is_index': 1,
    'cid': 0,
    'long_run': True
}
psiIndex = {
    'symbol': 'PSI20',
    'exchange': 'BVL',
    'currency': 'EUR',
    'sec_type': 'IND',
    'en_name': 'PORTUGAL PSI 20 INDEX',
    'tc_name': '葡萄牙 PSI',
    'region': 'pt',
    'is_index': 1,
    'cid': 0,
    'long_run': True
}
daxIndex = {
    'symbol': 'DAX',
    'exchange': 'DTB',
    'currency': 'EUR',
    'sec_type': 'IND',
    'en_name': 'DAX 30 Index',
    'tc_name': '德國 DAX',
    'region': 'de',
    'is_index': 1,
    'cid': 0,
    'long_run': True
}


class IBWrapper(EWrapper, EClient):
    '''
    This class is used as an IB client socket
    We receive realtime information by overriding methods provided by IB
    '''

    def __init__(self):
        EClient.__init__(self, self)

    def symbolSamples(self, reqId, contractDescriptions):
        try:
            if len(nyseReq) is not 0:
                reqSymbol = nyseReq[reqId]['symbol']
                usExchanges = ['NYSE', 'NASDAQ', 'AMEX', 'ARCA']
                primEx = str()
                for contractDesc in contractDescriptions:
                    contract = contractDesc.contract

                    if reqSymbol == contract.symbol:
                        if contract.primaryExchange in usExchanges:
                            primEx = contract.primaryExchange
                            break

                        inExList = False
                        for item in usExchanges:
                            if contract.primaryExchange.startswith(item):
                                primEx = item
                                inExList = True

                        if inExList:
                            break

                        primEx = contract.primaryExchange

                if len(primEx) is 0:
                    primEx = 'NYSE'

                es.update(index=MARKET_SYMBOL_INDEX, id='NYSE_{}'.format(
                    reqSymbol), body={'doc': {'exchange': primEx}})

            if len(cnseReq) is not 0:
                reqSymbol = cnseReq[reqId]['symbol']

                for contractDesc in contractDescriptions:
                    contract = contractDesc.contract

                    if reqSymbol == contract.symbol:
                        docID = 'CNSE_{}'.format(reqSymbol)
                        source = {
                            'symbol': reqSymbol,
                            'en_name': cnseReq[reqId]['name'],
                            'tc_name': cnseReq[reqId]['name'],
                            'exchange': contract.primaryExchange,
                            'currency': 'CNH',
                            'sec_type': 'STK',
                            'region': 'cn',
                            'is_index': 0,
                            'long_run': False
                        }

                        es.index(index=MARKET_SYMBOL_INDEX,
                                 id=docID, body=source)
                        break
        except Exception as e:
            print(traceback.format_exc())

    def error(self, reqId, errorCode, errorString):
        global cnseReq
        try:
            print(reqId, ' ', errorCode, ' ', errorString)
        except Exception as e:
            print('error ', e)


class IBCrawler(object):
    '''
    IBCrawler methods are used to store internation stocks or indices data
    into elasticsearch database after we use web crawler technique to fetch
    all required information
    '''
    def __init__(self):
        pass

    def initIB(self):
        global ib
        ib = IBWrapper()
        ib.connect('127.0.0.1', 9000, 50)
        threading.Thread(target=ib.run).start()

    def sehkParser(self):
        curCount = 0
        sehkProds = list()
        while True:
            curCount += 1
            r = requests.get(SEHK_BASE_URL.format('sehk', str(curCount)))

            if r.status_code == requests.codes.ok:
                pageContent = BeautifulSoup(r.text, 'html5lib')
                prodDivContent = pageContent.find(
                    'section', id='exchange-products')
                tbodies = prodDivContent.find('tbody')
                rows = tbodies.findChildren('tr')

                if not rows:
                    break

                for row in rows:
                    symbol = row.findChildren('td')[0].text
                    name = row.findChildren('td')[1].text
                    sehkProds.append({'symbol': symbol, 'name': name})

        addedItems = list()
        for item in sehkProds:
            symbol = item.get('symbol')
            enName = item.get('name')
            r = requests.get(SEHK_ZHTW_SYMBOL_URL.format(symbol))

            tcName = str()
            try:
                tcName = json.loads(r.text).get('description')
            except:
                pass

            docID = 'SEHK_{}'.format(symbol)
            source = {
                'symbol': symbol,
                'en_name': enName,
                'tc_name': tcName,
                'exchange': 'SEHK',
                'currency': 'HKD',
                'sec_type': 'STK',
                'region': 'hk',
                'is_index': 0,
                'long_run': False
            }

            body = {
                '_index': MARKET_SYMBOL_INDEX,
                '_id': docID,
                '_source': source
            }

            addedItems.append(body)

        helpers.bulk(es, addedItems)

    def nyseParser(self, scanFlag=True):
        curCount = 1
        nyseProds = list()

        if scanFlag:
            self.initIB()
            totalCount = 0
            global nyseReq

            while True:
                r = requests.get(SEHK_BASE_URL.format('nyse', str(curCount)))

                if r.status_code == requests.codes.ok:
                    pageContent = BeautifulSoup(r.text, 'html5lib')
                    prodDivContent = pageContent.find(
                        'section', id='exchange-products')
                    tbodies = prodDivContent.find('tbody')
                    rows = tbodies.findChildren('tr')

                    if not rows:
                        break

                    for row in rows:
                        totalCount += 1
                        symbol = row.findChildren('td')[0].text
                        nyseReq[totalCount] = {'symbol': symbol}

                    curCount += 1
                    time.sleep(1)
                else:
                    print('Error break, ', r.status_code)

            for k, item in nyseReq.items():
                ib.reqMatchingSymbols(k, item.get('symbol'))
                time.sleep(1)

        else:
            while True:
                curCount += 1
                r = requests.get(SEHK_BASE_URL.format('nyse', str(curCount)))

                if r.status_code == requests.codes.ok:
                    pageContent = BeautifulSoup(r.text, 'html5lib')
                    prodDivContent = pageContent.find(
                        'section', id='exchange-products')
                    tbodies = prodDivContent.find('tbody')
                    rows = tbodies.findChildren('tr')

                    if not rows:
                        break

                    def isascii(s): return len(s) == len(s.encode())
                    for row in rows:
                        symbol = row.findChildren('td')[0].text
                        name = row.findChildren('td')[1].text
                        tcName = str()

                        try:
                            r = requests.get(
                                NYSE_SYMBOL_QUERY_URL.format(symbol))
                            symbolData = json.loads(r.text)
                            tcName = symbolData.get('V1')

                            if isascii(tcName):
                                raise Exception
                        except:
                            tcName = name

                        nyseProds.append(
                            {'symbol': symbol, 'name': name, 'tcname': tcName})

        addedItems = list()
        for item in nyseProds:
            symbol = item.get('symbol')
            enName = item.get('name')
            tcName = item.get('tcname')
            r = requests.get(SEHK_ZHTW_SYMBOL_URL.format(symbol))

            docID = 'NYSE_{}'.format(symbol)
            source = {
                'symbol': symbol,
                'en_name': enName,
                'tc_name': tcName,
                'exchange': 'NYSE',
                'currency': 'USD',
                'sec_type': 'STK',
                'region': 'us',
                'is_index': 0,
                'long_run': False
            }

            body = {
                '_index': MARKET_SYMBOL_INDEX,
                '_id': docID,
                '_source': source
            }

            addedItems.append(body)

        helpers.bulk(es, addedItems)

    def cnseParser(self):
        global cnseReq
        r = requests.get(CYSE_PARSER_URL, headers={
            'Host': 'histock.tw',
            'Referer': 'https://histock.tw/szstock'
        })

        results = json.loads(r.text)

        self.initIB()
        cnseProds = list()
        for item in results:
            cnseProds.append({
                'symbol': item.get('No'),
                'tcName': item.get('Name')
            })

        totalCount = 0
        for item in cnseProds:
            totalCount += 1
            symbol = re.findall('\d+', item['symbol'])[0]
            cnseReq[totalCount] = {'symbol': symbol, 'name': item['tcName']}
            ib.reqMatchingSymbols(totalCount, symbol)
            time.sleep(1)

    def forexParser(self):
        for k, item in enumerate(forexLists):
            symbol = item[0:3]
            currency = item[3:6]

            docID = 'FOREX_{}'.format(item)
            source = {
                'symbol': '{}{}'.format(symbol, currency),
                'en_name': '{} to {}'.format(forexDicts[symbol]['en'], forexDicts[currency]['en']),
                'tc_name': '{}兌{}'.format(forexDicts[symbol]['tw'], forexDicts[currency]['tw']),
                'exchange': 'IDEALPRO',
                'currency': currency,
                'sec_type': 'CASH',
                'region': 'global',
                'is_index': 0,
                'long_run': False
            }

            es.index(index=MARKET_SYMBOL_INDEX,
                     id=docID, body=source)

    def initES(self):
        global es
        try:
            es = Elasticsearch(
                hosts=[DEMO_AWS_ES_HOST],
                use_ssl=True,
                port=443,
                max_retries=10,
                timeout=30,
                retry_on_timeout=True,
                request_timeout=30,
                connection_class=RequestsHttpConnection
            )

            return True
        except Exception as e:
            return False

    def checkEsIndex(self):
        for item in [MARKET_SYMBOL_INDEX, MARKET_BROKER_INDEX]:
            exist = es.indices.exists(item)

            if not exist:
                es.indices.create(index=item)

    def indicesCreator(self):
        requireIndices = [hsiIndex, sp500Index, ndxIndex,
                          djiIndex, soxIndex, hscciIndex, hsceiIndex,
                          n225Index, topxIndex, cacIndex, aexIndex,
                          bfxIndex, psiIndex, daxIndex]
        addedItems = list()

        for item in requireIndices:
            docID = '{}_{}'.format(item.get('exchange'), item.get('symbol'))
            body = {
                '_index': MARKET_SYMBOL_INDEX,
                '_id': docID,
                '_source': item
            }
            addedItems.append(body)

        r = helpers.bulk(es, addedItems)

    def checkSymbols(self):
        global es
        results = helpers.scan(
            es,
            index=MARKET_SYMBOL_INDEX,
            query={}
        )

        count = 0
        for item in results:
            source = item.get('_source')
            if source.get('currency') == 'CNH':
                count += 1

    def deleteSymbols(self):
        global es
        r = es.delete_by_query(index=MARKET_SYMBOL_INDEX, body={
            'query': {'match': {'symbol': 'STZ19'}}})

    def hkBrokers(self):
        lines = list()
        with open('HK.txt', 'r', encoding='utf-8') as fr:
            lines = fr.readlines()

        addedItems = list()
        for line in lines:
            tokens = line.split(' ', 1)
            code = tokens[0]
            if len(code) != 6:
                continue

            broker = tokens[1]
            body = {
                '_index': MARKET_BROKER_INDEX,
                '_id': code,
                '_source': {
                    'code': code,
                    'region': 'hk',
                    'broker': broker.strip()
                }
            }
            addedItems.append(body)

        r = helpers.bulk(es, addedItems)

    def otcSymbols(self):
        addedItems = list()
        otcItems = [{
            'symbol': 'LCHD',
            'name': 'LEADER CAPITAL HOLDINGS CORP'
        }, {
            'symbol': 'AUOTY',
            'name': 'AU OPTRONICS CORP-SPON ADR'
        }]

        for item in otcItems:
            symbol = item.get('symbol')
            enName = item.get('name')
            docID = 'OTC_{}'.format(symbol)
            source = {
                'symbol': symbol,
                'en_name': enName,
                'tc_name': enName,
                'exchange': 'PINK',
                'currency': 'USD',
                'sec_type': 'STK',
                'region': 'us',
                'is_index': 0,
                'long_run': False,
                'cid': 1
            }

            body = {
                '_index': MARKET_SYMBOL_INDEX,
                '_id': docID,
                '_source': source
            }

            addedItems.append(body)
        print(addedItems)
        helpers.bulk(es, addedItems)


if __name__ == '__main__':
    try:
        crawler = IBCrawler()
        crawler.initES()
        crawler.checkEsIndex()

        if len(sys.argv) > 1:
            firstArg = sys.argv[1]

            if firstArg == 'sehk':
                crawler.sehkParser()
            elif firstArg == 'nyse':
                crawler.nyseParser()
            elif firstArg == 'index':
                crawler.indicesCreator()
            elif firstArg == 'check':
                crawler.checkSymbols()
            elif firstArg == 'delete':
                crawler.deleteSymbols()
            elif firstArg == 'cnse':
                crawler.cnseParser()
            elif firstArg == 'forex':
                crawler.forexParser()
            elif firstArg == 'hk_broker':
                crawler.hkBrokers()
            elif firstArg == 'otc':
                crawler.otcSymbols()
    except Exception as e:
        print(e)
