#-*- coding: utf-8 -*-

from datetime import date, datetime, timedelta
from operator import sub

from dateutil.relativedelta import relativedelta


def offset(prev_months):
    temp = date.today() - relativedelta(months=prev_months + 1)
    return date(temp.year, temp.month, 1).isoformat()


def last_day_prev_month(date_=None, format="%Y-%m-%d"):
    date_ = datetime.strptime(date_ or date.today().isoformat(), format)
    return sub(date(date_.year, date_.month, 1),
               timedelta(days=1)).isoformat()


def first_day_current_month(date_=None, format="%Y-%m-%d"):
    date_ = datetime.strptime(date_ or date.today().isoformat(), format)
    return date(date_.year, date_.month, 1).isoformat()


def today():
    return date.today().isoformat()


def yesterday():
    return (date.today() - timedelta(days=1)).isoformat()
