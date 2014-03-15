#-*- coding: utf-8 -*-

from datetime import date, timedelta

from dateutil.relativedelta import relativedelta


def offset(prev_months):
    temp = date.today() - relativedelta(months=prev_months + 1)
    return date(temp.year, temp.month, 1).isoformat()


def yesterday():
    return (date.today() - timedelta(days=1)).isoformat()
