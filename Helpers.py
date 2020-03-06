# -*- coding:utf-8 -*-
import os

import pandas as pd
import datetime
import tushare as ts

from Downloader import download_k_data_one_stock


def find_lowest_position_stocks(dt_date):
    src_dir = '../data/k_data/k_data_from_20180501'
    dst_dir = '../data/result/lowest_position_stocks'
    basics = pd.read_excel(src_dir + '/basics.xls')
    earliest_to_market_date = dt_date - datetime.timedelta(days=365 * 3)
    lowest_position_stocks = pd.DataFrame(columns=['code', 'name',
                                                   'price',
                                                   'min_price', 'price-min', 'price-min_ratio', 'min_price_date',
                                                   'max_price', 'max-price', 'max-price_ratio', 'max_price_date',
                                                   'mean', 'median',
                                                   'industry', 'outstanding', 'totals', 'outstanding_value'])

    for index, row in basics.iterrows():
        print(index)

        code = str(row['code'])
        code = "{:0>6}".format(code)
        name = row['name']
        industry = row['industry']
        outstanding = row['outstanding']
        totals = row['totals']

        if 0 == row['timeToMarket']:
            continue

        str_time_to_market = str(row['timeToMarket'])
        time_to_market = datetime.datetime.strptime(str_time_to_market, '%Y%m%d')
        area = row['area']

        if name.startswith('ST') or name.startswith('*ST'):
            continue

        if code.startswith('3'):
            continue

        if time_to_market > earliest_to_market_date:
            continue

        k_data_path = src_dir + '/' + code + '.csv'
        if not os.path.exists(k_data_path):
            continue

        try:
            k_data = pd.read_csv(k_data_path, parse_dates=['date'])
        except pd.errors.EmptyDataError:
            continue
        except KeyError:
            print(code)
            continue

        min_price = 1111110.0
        min_price_date = ''
        max_price = 0.0
        max_price_date = ''
        for k_index, k_row in k_data.iterrows():
            if k_row['close'] < min_price:
                min_price = k_row['close']
                min_price_date = k_row['date']
            if k_row['close'] > max_price:
                max_price = k_row['close']
                max_price_date = k_row['date']

        item = {}
        item['code'] = code
        item['name'] = name
        item['price'] = k_data.iloc[-1]['close']
        item['min_price'] = min_price
        item['price-min'] = item['price'] - item['min_price']
        item['price-min_ratio'] = item['price-min'] / item['min_price'] * 100
        item['min_price_date'] = min_price_date
        # item['profit_ratio_percent'] = '{:.2f}%'.format(item['profit'] / item['min_price'] * 100)
        item['max_price'] = max_price
        item['max-price'] = item['max_price'] - item['price']
        item['max-price_ratio'] = item['max-price'] / item['price'] * 100
        item['max_price_date'] = max_price_date
        item['mean'] = k_data['close'].mean()
        item['median'] = k_data['close'].median()
        item['industry'] = industry
        item['outstanding'] = outstanding
        item['totals'] = totals
        item['outstanding_value'] = min_price * outstanding
        lowest_position_stocks = lowest_position_stocks.append(item, ignore_index=True)

    # lowest_position_stocks = lowest_position_stocks[lowest_position_stocks['outstanding_value'] > 300]
    # lowest_position_stocks = lowest_position_stocks[lowest_position_stocks['outstanding_value'] < 2000]
    # lowest_position_stocks = lowest_position_stocks[lowest_position_stocks['min_price'] < 20.0]
    lowest_position_stocks = lowest_position_stocks.sort_values(
        ['price-min_ratio', 'min_price_date', 'max-price_ratio', 'outstanding_value', 'min_price', 'code'],
        ascending=[True, False, False, False, True, False])
    lowest_position_stocks.to_excel(dst_dir + '/' + dt_date.strftime("%Y-%m-%d") + '.xls', encoding='utf-8', index=False)


def find_lowest_position_stocks_with_targets(target_stock_set):
    src_dir = '../data/k_data/k_data_from_20180501'
    dst_dir = '../data/result/lowest_position_stocks'
    basics = pd.read_excel(src_dir + '/basics.xls')
    basics['code'] = basics['code'].apply(lambda x: "{:0>6}".format(x))
    today = datetime.datetime.now()
    earliest_to_market_date = today - datetime.timedelta(days=365 * 3)
    lowest_position_stocks = pd.DataFrame(columns=['code', 'name',
                                                   'price',
                                                   'min_price', 'price-min', 'price-min_ratio', 'min_price_date',
                                                   'max_price', 'max-price', 'max-price_ratio', 'max_price_date',
                                                   'mean', 'median',
                                                   'industry', 'outstanding', 'totals', 'outstanding_value'])

    for code in target_stock_set:
        print(code)

        whether_found_target = False
        for index, row in basics.iterrows():
            if row['code'] == code:
                name = row['name']
                industry = row['industry']
                outstanding = row['outstanding']
                totals = row['totals']
                time_to_market = row['timeToMarket']
                whether_found_target = True
                break

        if not whether_found_target:
            continue

        if 0 == time_to_market:
            continue

        str_time_to_market = str(row['timeToMarket'])
        time_to_market = datetime.datetime.strptime(str_time_to_market, '%Y%m%d')
        area = row['area']

        if name.startswith('ST') or name.startswith('*ST'):
            continue

        if code.startswith('3'):
            continue

        if time_to_market > earliest_to_market_date:
            continue

        try:
            k_data = pd.read_csv(src_dir + '/' + code + '.csv', parse_dates=['date'])
        except pd.errors.EmptyDataError:
            continue
        except KeyError:
            print(code)
            continue

        min_price = 1111110.0
        min_price_date = ''
        max_price = 0.0
        max_price_date = ''
        for k_index, k_row in k_data.iterrows():
            if k_row['close'] < min_price:
                min_price = k_row['close']
                min_price_date = k_row['date']
            if k_row['close'] > max_price:
                max_price = k_row['close']
                max_price_date = k_row['date']

        item = {}
        item['code'] = code
        item['name'] = name
        item['price'] = k_data.iloc[-1]['close']
        item['min_price'] = min_price
        item['price-min'] = item['price'] - item['min_price']
        item['price-min_ratio'] = item['price-min'] / item['min_price'] * 100
        item['min_price_date'] = min_price_date
        # item['profit_ratio_percent'] = '{:.2f}%'.format(item['profit'] / item['min_price'] * 100)
        item['max_price'] = max_price
        item['max-price'] = item['max_price'] - item['price']
        item['max-price_ratio'] = item['max-price'] / item['price'] * 100
        item['max_price_date'] = max_price_date
        item['mean'] = k_data['close'].mean()
        item['median'] = k_data['close'].median()
        item['industry'] = industry
        item['outstanding'] = outstanding
        item['totals'] = totals
        item['outstanding_value'] = min_price * outstanding
        lowest_position_stocks = lowest_position_stocks.append(item, ignore_index=True)

    lowest_position_stocks = lowest_position_stocks.sort_values(
        ['price-min_ratio', 'min_price_date', 'max-price_ratio', 'outstanding_value', 'min_price', 'code'],
        ascending=[True, False, False, False, True, False])
    lowest_position_stocks.to_excel(dst_dir + '/' + today.strftime("%Y-%m-%d") + '_industry.xls', encoding='utf-8',
                                    index=False)


def incremental_update_k_data():
    save_dir = '../data/k_data/k_data_from_20180501'
    basics = ts.get_stock_basics()
    basics = basics.reset_index()
    basics['code'] = basics['code'].apply(lambda x: "{:0>6}".format(x))
    basics.to_excel(save_dir + '/basics.xls', encoding='utf-8')

    today_all = ts.get_today_all()
    for index, row in today_all.iterrows():
        k_data_path = save_dir + '/' + row['code'] + '.csv'
        if not os.path.exists(k_data_path):
            continue
        k_data = pd.read_csv(k_data_path)
        if 0 == k_data.shape[0]:
            continue

        # 今天的收盘价和前一天下载数据的收盘价不一致，说明有除权除息
        if abs(k_data.iloc[-1]['close'] - row['settlement']) > 0.001:
            download_k_data_one_stock(row['code'])
        else:
            k_data = k_data.append({'date': datetime.datetime.now().strftime("%Y/%m/%d"),
                                    'open': row['open'],
                                    'close': row['trade'],
                                    'high': row['high'],
                                    'low': row['low'],
                                    'volume': row['volume'],
                                    'code': row['code']}, ignore_index=True)
            k_data.to_csv(save_dir + '/' + row['code'] + '.csv', index=False)

    find_lowest_position_stocks()
