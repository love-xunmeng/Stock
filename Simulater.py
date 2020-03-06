# -*- coding:utf-8 -*-
import json
import urllib

import pandas as pd
import datetime
import time

import requests
import tushare as ts

from StockTool import check_is_trade_date, check_is_trade_time


def monitor():
    if not check_is_trade_date(datetime.datetime.now()):
        return

    dt_yesterday = datetime.datetime.now() + datetime.timedelta(days=-1)
    str_yesterday = dt_yesterday.strftime('%Y-%m-%d')
    src_dir = '../data/result/lowest_position_stocks'
    lowest_position_stocks = pd.read_excel(src_dir + '/' + str_yesterday+'.xls')
    lowest_position_stocks['code'] = lowest_position_stocks['code'].apply(lambda x: "{:0>6}".format(x))

    while True:
        # 不是交易时间
        #if not check_is_trade_time():
        #    print('不是交易时间')
        #    break
        print('\n')
        start_time = datetime.datetime.now()
        str_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
        print(str_start_time + " start")
        try:
            i = 0
            for index, row in lowest_position_stocks.iterrows():
                i = i + 1
                code = "{:0>6}".format(row['code'])
                name = row['name']
                if name.startswith('ST') or name.startswith('*ST') or code.startswith('3') or code.startswith('688'):
                    continue

                df = ts.get_realtime_quotes(code)
                if 0 == df.shape[0]:
                    continue

                price = float(df.iloc[0]['price'])
                if 0.0 < price < row['min_price']:
                    lowest_position_stocks.loc[index, 'min_price'] = price
                    lowest_position_stocks.loc[index, 'min_price_date'] = datetime.datetime.now().strftime(
                        '%Y-%m-%d %H:%M:%S')

                if price > 0.0:
                    lowest_position_stocks.loc[index, 'price'] = price
                    lowest_position_stocks.loc[index, 'price-min'] = lowest_position_stocks.loc[index, 'price'] - \
                                                                     lowest_position_stocks.loc[index, 'price-min']
                    lowest_position_stocks.loc[index, 'price-min_ratio'] = lowest_position_stocks.loc[
                                                                               index, 'price-min'] / \
                                                                           lowest_position_stocks.loc[
                                                                               index, 'min_price']
                    lowest_position_stocks.loc[index, 'max-price'] = row['max_price'] - price
                    lowest_position_stocks.loc[index, 'max-price_ratio'] = (row['max_price'] - price) / price * 100


            lowest_position_stocks = lowest_position_stocks.sort_values \
                (['price-min_ratio', 'min_price_date', 'max-price_ratio', 'outstanding_value', 'min_price', 'code'],
                 ascending=[True, False, False, False, True, False])
            print(lowest_position_stocks.head(10))
        except urllib.error.HTTPError:
            print('urllib.error.HTTPError')
            time.sleep(1)
        except urllib.error.URLError:
            print('urllib.error.URLError')
            time.sleep(1)

        finish_time = datetime.datetime.now()
        str_finish_time = finish_time.strftime('%Y-%m-%d %H:%M:%S')
        print(str_finish_time + " finish")
        print("process time:", (finish_time - start_time).seconds)
