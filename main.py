# -*- coding:utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
import schedule

from BackTester import back_test, back_test_with_sh_index
from Downloader import download_k_data
from Helpers import find_lowest_position_stocks, find_lowest_position_stocks_with_targets, incremental_update_k_data
from Simulater import monitor
import tushare as ts

from StockTool import check_is_trade_date

pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 1000)


def update_min_price_incremental():
    pass


def update_min_price_every_date():
    dt_yesterday = datetime.now() + timedelta(days=-1)
    if not check_is_trade_date(dt_yesterday):
        return
    download_k_data()
    find_lowest_position_stocks(dt_yesterday)


def print_interested_concept_stocks():
    interested_concepts = ['5G概念', '雄安新区', '物联网']
    concept_classified_stocks = ts.get_concept_classified()
    for concept_name, group in concept_classified_stocks.groupby(['c_name']):
        for interested_concept in interested_concepts:
            if concept_name == interested_concept:
                print(interested_concept)


def download_interested_industry_stocks():
    # interested_industries = ['水泥行业', '建筑建材', '钢铁行业', '房地产', '公路桥梁']
    interested_industries = ['建筑建材', '水泥行业', '钢铁行业', '公路桥梁']
    industry_classified_stocks = ts.get_industry_classified()
    interested_industry_stock_list = []
    for industry_name, group in industry_classified_stocks.groupby(['c_name']):
        for interested_industry in interested_industries:
            if industry_name == interested_industry:
                for index, row in group.iterrows():
                    interested_industry_stock_list.append("{:0>6}".format(row['code']))
                    # interested_industry_stock_list.append(
                    #    {'code':"{:0>6}".format(row['code']), 'name': row['name'], 'industry': row['c_name']})
    return interested_industry_stock_list


def monitor_impl():
    #update_min_price_every_date()
    monitor()

    #schedule.every().day.at('02:00').do(update_min_price_every_date)
    #schedule.every().day.at('09:30').do(monitor)
    #schedule.every().day.at('13:00').do(monitor)
    #while True:
    #    schedule.run_pending()


def main():
    monitor_impl()

    # interested_industry_stock_list = download_interested_industry_stocks()
    # find_lowest_position_stocks_with_targets(interested_industry_stock_list)

    # back_test()
    # back_test_with_sh_index()
    # monitor()
    # incremental_update_k_data()

    # find_lowest_position_stocks()

    # find_lowest_position_stocks_instant()

    # find_lowest_position_stocks_instant()
    # update_min_price()
    # merge_all_stock_data()

    # print(get_trade_dates())
    # today_k_data = download_k_data(['600011', '600029'], '2020-02-24')
    # print(today_k_data)

    # today_all_stocks_k_data = download_k_data('2020-02-21')
    # print('\n')
    # print(today_all_stocks_k_data)
    # print('\n')
    # today_all_stocks_k_data = download_k_data('2020-02-24')


if __name__ == '__main__':
    main()
