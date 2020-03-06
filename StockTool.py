# -*- coding:utf-8 -*-
import json
import time
from datetime import datetime

import requests


def check_is_trade_time():
    dt_now = datetime.now()
    dt_morning_start_time = datetime(dt_now.year, dt_now.month, dt_now.day, 9, 30, 00)
    dt_morning_end_time = datetime(dt_now.year, dt_now.month, dt_now.day, 11, 30, 00)
    dt_afternoon_start_time = datetime(dt_now.year, dt_now.month, dt_now.day, 13, 00, 00)
    dt_afternoon_end_time = datetime(dt_now.year, dt_now.month, dt_now.day, 15, 00, 00)
    if dt_morning_start_time <= dt_now <= dt_morning_end_time:
        return True
    if dt_afternoon_start_time <= dt_now <= dt_afternoon_end_time:
        return True
    return False


def check_is_trade_date(dt_date):
    # today = time.strftime('%Y%m%d', time.localtime())
    str_date = dt_date.strftime('%Y%m%d')
    server_url = "http://www.easybots.cn/api/holiday.php?d="
    req = requests.get(server_url + str_date)

    # 获取data值
    vop_data = json.loads(req.text)
    if int(vop_data[str_date]) == 1:
        return False
    elif int(vop_data[str_date]) == 0:
        return True
    elif int(vop_data[str_date]) == 2:
        return False
    else:
        print('Error')
        return False
