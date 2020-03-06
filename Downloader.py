# -*- coding:utf-8 -*-

import datetime
import tushare as ts


def download_k_data_one_stock(code):
    start_date = '2018-05-01'
    autype = 'qfq'
    retry_count = 9999999
    save_dir = '../data/k_data/k_data_from_20180501/'
    h_data = ts.get_k_data(code, start=start_date, autype=autype, retry_count=retry_count)
    h_data.to_csv(save_dir + code + '.csv', index=False)


def download_k_data():
    start_date = '2018-05-01'
    autype = 'qfq'
    retry_count = 9999999
    save_dir = '../data/k_data/k_data_from_20180501/'

    today = datetime.datetime.now()
    earliest_to_market_date = today - datetime.timedelta(days=365 * 3)

    basic = ts.get_stock_basics()
    basic.to_excel(save_dir + 'basics.xls', encoding='utf-8')
    basic = basic.reset_index()
    for index, row in basic.iterrows():
        code = "{:0>6}".format(str(row['code']))
        name = row['name']

        if 0 == row['timeToMarket']:
            continue

        str_time_to_market = str(row['timeToMarket'])
        time_to_market = datetime.datetime.strptime(str_time_to_market, '%Y%m%d')

        if name.startswith('ST') or name.startswith('*ST'):
            continue

        if code.startswith('3'):
            continue

        if time_to_market > earliest_to_market_date:
            continue

        # code = row.code
        print("Downloading k_data:", index, code)
        h_data = ts.get_k_data(code, start=start_date, autype=autype, retry_count=retry_count)
        h_data.to_csv(save_dir + code + '.csv', index=False)

    # 上证指数
    h_data = ts.get_k_data('000001', start=start_date, index=True, retry_count=9999999)
    h_data.to_csv(save_dir + 'sh.csv', index=False)
    # 深证指数
    h_data = ts.get_k_data('399001', start=start_date, index=True, retry_count=9999999)
    h_data.to_csv(save_dir + 'sz.csv', index=False)
    # 创业指数
    h_data = ts.get_k_data('399006', start=start_date, index=True, retry_count=9999999)
    h_data.to_csv(save_dir + 'cyb.csv', index=False)


def download_k_data_back_test(code_set, start_date, save_dir):
    autype = 'qfq'
    retry_count = 9999999
    for code in code_set:
        print("Downloading k_data:", code)
        h_data = ts.get_k_data(code, start=start_date, autype=autype, retry_count=retry_count)
        h_data.to_csv(save_dir + '/' + code + '.csv', index=False)
