# -*- coding:utf-8 -*-
import datetime

import pandas as pd


class StockStaticItem:
    def __init__(self, code, name, min_price, min_price_date, max_price, max_price_date):
        self.code_ = code
        self.name_ = name
        self.min_price_ = min_price
        self.min_price_date_ = min_price_date
        self.max_price_ = max_price
        self.max_price_date_ = max_price_date


def calculate_stock_static_info(k_data):
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
    item = {'min_price_date': min_price_date, 'min_price': min_price, 'max_price': max_price,
            'max_price_date': max_price_date}

    return item


class BuyItem:
    def __init__(self, code, buy_day, buy_price, holding):
        self.code_ = code
        self.buy_day_ = buy_day
        self.buy_price_ = buy_price
        self.holding_ = holding


def back_test_with_candidate_stocks():
    code_set = ['601989', '600011', '600029', '601800', '601766']
    start_date = '2012-01-01'
    strategy_name = 'lowest_position_stocks'
    save_dir = './data/tmp/back_test/' + strategy_name
    # download_k_data_back_test(code_set, start_date, save_dir)

    print('\n')
    for code in code_set:
        # 取一年数据找出最低点作为开始
        k_data = pd.read_csv(save_dir + "/" + code + ".csv")
        k_data['close-1'] = k_data['close'].shift(1)
        k_data['close-(close-1)'] = k_data['close'] - k_data['close-1']
        k_data['sign'] = np.sign(k_data['close-(close-1)'])

        print(k_data.tail(10))

        min_price = 100000000.0
        min_price_date = ""
        min_price_index = 0
        for i in range(0, 300):
            row = k_data.iloc[i]
            if row['close'] < min_price:
                min_price = row['close']
                min_price_date = row['date']
                min_price_index = i
        print(code, min_price_index, min_price_date, min_price)

        # print('\n')
        need_slump_days = 2
        need_rise_days = 2
        buy_list = []
        buy_max_price = 0.0
        max_price = 0.0
        for i in range(min_price_index + need_slump_days + need_rise_days + 1,
                       k_data.shape[0] - need_slump_days - need_rise_days):
            slump_days = 0
            rise_days = 0
            current_price = k_data.iloc[i]['close']
            if current_price > max_price:
                max_price = current_price
            for j in range(need_slump_days + need_rise_days, need_rise_days, -1):
                if k_data.iloc[i - j - 1]['sign'] < 0:
                    slump_days += 1
            for j in range(0, need_rise_days):
                if k_data.iloc[i - j - 1]['sign'] > 0:
                    rise_days += 1
            if slump_days == need_slump_days and rise_days == need_rise_days:
                buy_list.append(BuyItem(code, k_data.iloc[i]['date'], current_price, 200))
                if current_price > buy_max_price:
                    buy_max_price = current_price

            # 清仓
            clearance = False
            # if((current_price < min_price * 0.95)):
            # if((buy_max_price > 0 and current_price < buy_max_price * 0.95) or (current_price < min_price * 0.95)):
            if (max_price > 0 and current_price < max_price * 0.95) or (current_price < min_price * 0.95):
                if max_price > 0 and current_price < max_price * 0.95:
                    print("max_price > 0 and current_price < max_price * 0.95")
                # if(buy_max_price > 0 and current_price < buy_max_price * 0.95):
                # print("buy_max_price > 0 and current_price < buy_max_price * 0.95")
                if current_price < min_price * 0.95:
                    print("current_price < min_price * 0.95")
                clearance = True

            if clearance:
                # if(i % 100 == 0):
                # if(i % 100 == 0 or clearance):
                buy_price_sum = 0.0
                profit = 0
                holding = 0
                for buy_item in buy_list:
                    # profit = profit + (current_price - buy_item.buy_price_)
                    holding += buy_item.holding_
                    buy_price_sum += buy_item.buy_price_
                # print(buy_item.code_, buy_item.buy_day_, buy_item.buy_price_, buy_item.holding_)
                if len(buy_list) > 0:
                    profit = current_price - buy_price_sum / len(buy_list)
                print(k_data.iloc[i]['date'], current_price, profit, holding)

            if clearance:
                break

        print('\n')


def merge_all_stock_data():
    src_dir = '../data/tmp/back_test/lowest_position_stocks'
    basics = pd.read_excel(src_dir + '/basics.xls')

    all_stock_data_list = []

    for index, row in basics.iterrows():
        print(index, row['code'])

        code = str(row['code'])
        code = "{:0>6}".format(code)
        name = row['name']
        industry = row['industry']
        outstanding = row['outstanding']
        totals = row['totals']

        if code.startswith('3') or code.startswith('688'):
            continue

        if 0 == row['timeToMarket']:
            continue

        str_time_to_market = str(row['timeToMarket'])
        time_to_market = datetime.datetime.strptime(str_time_to_market, '%Y%m%d')

        if time_to_market > datetime.datetime.strptime('2009-1-1', "%Y-%m-%d"):
            continue

        try:
            k_data = pd.read_csv(src_dir + '/' + code + '.csv', parse_dates=['date'])
        except pd.errors.EmptyDataError:
            continue
        except KeyError:
            print(code)
            continue

        all_stock_data_list.extend(k_data.values.tolist())

    print('\n')
    print('len(all_stock_data_list):', len(all_stock_data_list))

    df_all_stock = pd.DataFrame(all_stock_data_list)
    df_all_stock.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'code']
    print('\n')
    print(df_all_stock.shape)

    print('\n')

    df_all_stock.to_csv('../data/result/df_all_stock.csv', index=False)


def back_test():
    df_all_stock = pd.read_csv('../data/result/df_all_stock.csv', parse_dates=['date'])
    print(df_all_stock.shape)

    # 第一步，用第一年 2012 的数据计算最小值列表
    df_init_data = df_all_stock[df_all_stock['date'] < '2013-01-01']
    stock_static_map = {}

    for code, group in df_init_data.groupby(['code']):
        str_code = "{:0>6}".format(code)
        # print(str_code)
        stock_static_item = calculate_stock_static_info(group)
        stock_static_item['code'] = str_code
        stock_static_item['price'] = 0.0
        stock_static_item['price-min'] = 9999.9999
        stock_static_item['price-min_ratio'] = 9999.9999
        stock_static_item['max-price'] = -9999.9999
        stock_static_item['max-price_ratio'] = -9999.9999
        stock_static_map[str_code] = stock_static_item
    # stock_static_list.append(stock_static_item)

    # df_stock_static = pd.DataFrame(stock_static_list)

    # 第二步：用2013-2020-2月的数据做回测
    # 初始资金10000，最多持有3只股票，每只股票最低价格不能超过15元，每次购入200股
    max_hold_stocks = 5
    cash = 10000
    holding_stock_list = []
    trade_record_list = []

    df_back_test_data = df_all_stock[df_all_stock['date'] > '2013-01-01']
    date_count = 0
    for date, group in df_back_test_data.groupby(['date']):
        # print('-----------', date, '-------------')
        # 2.1 每天更新

        for index, row in group.iterrows():
            str_code = "{:0>6}".format(row['code'])
            if str_code in stock_static_map.keys():
                if 0.0 < row['close'] < stock_static_map[str_code]['min_price']:
                    stock_static_map[str_code]['min_price'] = row['close']
                    stock_static_map[str_code]['min_price_date'] = row['date']
                if row['close'] > stock_static_map[str_code]['min_price']:
                    stock_static_map[str_code]['max_price'] = row['close']
                    stock_static_map[str_code]['max_price_date'] = row['date']

                stock_static_map[str_code]['price'] = row['close']
                stock_static_map[str_code]['price-min'] = row['close'] - stock_static_map[str_code]['min_price']
                stock_static_map[str_code]['price-min_ratio'] = stock_static_map[str_code]['price-min'] / \
                                                                stock_static_map[str_code]['price'] * 100
                stock_static_map[str_code]['max-price'] = stock_static_map[str_code]['max_price'] - row['close']
                stock_static_map[str_code]['max-price_ratio'] = stock_static_map[str_code]['max-price'] / \
                                                                stock_static_map[str_code]['price'] * 100
            else:
                item = {'code': str_code, 'min_price': row['close'], 'min_price_date': row['date'],
                        'max_price': row['close'], 'max_price_date': row['date'], 'price': row['close'],
                        'price-min': 9999.9999, 'price-min_ratio': 9999.9999, 'max-price': -9999.9999,
                        'max-price_ratio': -9999.9999}
            # 不能直接加，一个可能是新股
            # stock_static_map[str_code] = item

        # 2.2 看看是否需要买入，卖出
        # 卖出条件：
        #	1）涨幅超过30%卖出
        # 买入条件：
        #	1）当前价格比最低价格不超过3%
        #	2) 股价不超过15元
        #	3）市值超过300亿：这个可以再补
        #
        # sorted(L, cmp=lambda x,y:cmp(x[1],y[1]))   # 利用cmp函数
        # sorted(L, key=lambda x:x[1])               # 利用key

        # print("Step2.2.1")
        # 2.2.1 先卖出符合条件的
        for holding_stcok in holding_stock_list:
            for index, row in group.iterrows():
                code = "{:0>6}".format(row['code'])
                # print("@@@@@@@", index, code)
                if code == holding_stcok['code']:
                    holding_stcok['price'] = row['close']
                    if (row['close'] - holding_stcok['buy_price']) / holding_stcok['buy_price'] > 0.50:
                        cash += row['close'] * 200
                        print('\n')
                        print('sell ', date, code, row['close'])
                        holding_stcok['sell_price'] = row['close']
                        holding_stcok['sell_date'] = date
                        trade_record_list.append(holding_stcok)
                        holding_stock_list.remove(holding_stcok)

                    # 2.2.2 买入
        stock_static_list = sorted(stock_static_map.items(), key=lambda x: x[1]['price-min_ratio'])
        for item in stock_static_list:
            item = item[1]

            # if item['price-min_ratio'] > 0.30 or item['price'] > 15.00:
            #	break

            if len(holding_stock_list) < max_hold_stocks and item['price'] > 0.0 and item['price'] * 200 < cash:
                buy_item = {'code': item['code'], 'price': item['price'], 'buy_price': item['price'], 'buy_date': date,
                            'min_price': item['min_price'], 'price-min_ratio': item['price-min_ratio'],
                            'min_price_date': item['min_price_date'], 'max_price': item['max_price'],
                            'max-price_ratio': item['max-price_ratio'], 'max_price_date': item['max_price_date']}

                cash -= item['price'] * 200
                print('\n')
                print('buy', buy_item['buy_date'], buy_item['code'], buy_item['buy_price'], buy_item['min_price'], cash)
                holding_stock_list.append(buy_item)

        date_count += 1
        if date_count % 20 == 0:
            capital = cash
            for holding_stcok in holding_stock_list:
                capital += holding_stcok['price'] * 200
            print(date_count, date, capital)


    df_trade_record = pd.DataFrame(trade_record_list)
    df_trade_record.to_csv('../data/result/trade_record.csv', index=False)

    print('finish')


def back_test_with_sh_index():
    src_dir = '../data/tmp/back_test/lowest_position_stocks'
    df_all_stock = pd.read_csv('../data/result/df_all_stock.csv', parse_dates=['date'])
    print(df_all_stock.shape)

    # 第一步，用第一年 2012 的数据计算最小值列表
    df_init_data = df_all_stock[df_all_stock['date'] < '2013-01-01']
    stock_static_map = {}

    for code, group in df_init_data.groupby(['code']):
        str_code = "{:0>6}".format(code)
        # print(str_code)
        stock_static_item = calculate_stock_static_info(group)
        stock_static_item['code'] = str_code
        stock_static_item['price'] = 0.0
        stock_static_item['price-min'] = 9999.9999
        stock_static_item['price-min_ratio'] = 9999.9999
        stock_static_item['max-price'] = -9999.9999
        stock_static_item['max-price_ratio'] = -9999.9999
        stock_static_map[str_code] = stock_static_item

    df_sh_index = pd.read_csv(src_dir + '/sh.csv', parse_dates=['date'])
    df_sh_index['close-1'] = df_sh_index['close'].shift(1)
    df_sh_index['ratio'] = (df_sh_index['close'] - df_sh_index['close-1']) / df_sh_index['close-1']
    df_sh_index = df_sh_index[df_sh_index['date'] > '2013-01-01']
    df_sh_index.set_index(['date'], inplace=True)

    # 第二步：用2013-2020-2月的数据做回测
    # 初始资金10000，最多持有3只股票，每只股票最低价格不能超过15元，每次购入200股
    max_hold_stocks = 5
    cash = 10000
    holding_stock_list = []
    trade_record_list = []

    df_back_test_data = df_all_stock[df_all_stock['date'] > '2013-01-01']
    date_count = 0
    for date, group in df_back_test_data.groupby(['date']):
        # print('-----------', date, '-------------')
        # 2.1 每天更新

        for index, row in group.iterrows():
            str_code = "{:0>6}".format(row['code'])
            if str_code in stock_static_map.keys():
                if 0.0 < row['close'] < stock_static_map[str_code]['min_price']:
                    stock_static_map[str_code]['min_price'] = row['close']
                    stock_static_map[str_code]['min_price_date'] = row['date']
                if row['close'] > stock_static_map[str_code]['min_price']:
                    stock_static_map[str_code]['max_price'] = row['close']
                    stock_static_map[str_code]['max_price_date'] = row['date']

                stock_static_map[str_code]['price'] = row['close']
                stock_static_map[str_code]['price-min'] = row['close'] - stock_static_map[str_code]['min_price']
                stock_static_map[str_code]['price-min_ratio'] = stock_static_map[str_code]['price-min'] / \
                                                                stock_static_map[str_code]['price'] * 100
                stock_static_map[str_code]['max-price'] = stock_static_map[str_code]['max_price'] - row['close']
                stock_static_map[str_code]['max-price_ratio'] = stock_static_map[str_code]['max-price'] / \
                                                                stock_static_map[str_code]['price'] * 100
            else:
                item = {'code': str_code, 'min_price': row['close'], 'min_price_date': row['date'],
                        'max_price': row['close'], 'max_price_date': row['date'], 'price': row['close'],
                        'price-min': 9999.9999, 'price-min_ratio': 9999.9999, 'max-price': -9999.9999,
                        'max-price_ratio': -9999.9999}
            # 不能直接加，一个可能是新股
            # stock_static_map[str_code] = item

        # 2.2 看看是否需要买入，卖出
        # 卖出条件：
        #	1）涨幅超过30%卖出
        # 买入条件：
        #	1）当前价格比最低价格不超过3%
        #	2) 股价不超过15元
        #	3）市值超过300亿：这个可以再补
        #
        # sorted(L, cmp=lambda x,y:cmp(x[1],y[1]))   # 利用cmp函数
        # sorted(L, key=lambda x:x[1])               # 利用key

        # print("Step2.2.1")
        # 2.2.1 先卖出符合条件的
        for holding_stcok in holding_stock_list:
            for index, row in group.iterrows():
                code = "{:0>6}".format(row['code'])
                # print("@@@@@@@", index, code)
                if code == holding_stcok['code']:
                    holding_stcok['price'] = row['close']
                    if (row['close'] - holding_stcok['buy_price']) / holding_stcok['buy_price'] > 0.50:
                        cash += row['close'] * 200
                        print('\n')
                        print('sell ', date, code, row['close'])
                        holding_stcok['sell_price'] = row['close']
                        holding_stcok['sell_date'] = date
                        trade_record_list.append(holding_stcok)
                        holding_stock_list.remove(holding_stcok)

                    # 2.2.2 买入
        stock_static_list = sorted(stock_static_map.items(), key=lambda x: x[1]['price-min_ratio'])
        for item in stock_static_list:
            item = item[1]

            # if item['price-min_ratio'] > 0.30 or item['price'] > 15.00:
            #	break

            if len(holding_stock_list) < max_hold_stocks and item['price'] > 0.0 and item['price'] * 200 < cash:
                if df_sh_index.loc[date]['ratio'] < -0.01:
                    buy_item = {'code': item['code'], 'price': item['price'], 'buy_price': item['price'], 'buy_date': date,
                                'min_price': item['min_price'], 'price-min_ratio': item['price-min_ratio'],
                                'min_price_date': item['min_price_date'], 'max_price': item['max_price'],
                                'max-price_ratio': item['max-price_ratio'], 'max_price_date': item['max_price_date']}

                    cash -= item['price'] * 200
                    print('\n')
                    print('buy', buy_item['buy_date'], buy_item['code'], buy_item['buy_price'], buy_item['min_price'], cash)
                    holding_stock_list.append(buy_item)

        date_count += 1
        if date_count % 20 == 0:
            capital = cash
            for holding_stcok in holding_stock_list:
                capital += holding_stcok['price'] * 200
            print(date_count, date, capital)

    df_trade_record = pd.DataFrame(trade_record_list)
    df_trade_record.to_csv('../data/result/trade_record.csv', index=False)

    print('finish')
