#-*- coding: utf-8 -*-

import os
import glob
from datetime import date, datetime, timedelta
from operator import sub

from dateutil.relativedelta import relativedelta

__all__ = [os.path.splitext(os.path.basename(f))[0] for f in
           glob.glob("{0}/recipe*.py".format(os.path.dirname(__file__)))]


def offset(prev_months):
    temp = date.today() - relativedelta(months=prev_months + 1)
    return date(temp.year, temp.month, 1).isoformat()


def last_day_prev_month(date_=None, format="%Y-%m-%d"):
    date_ = datetime.strptime(date_ or date.today().isoformat(), format)
    return sub(date(date_.year, date_.month, 1),
               timedelta(days=1)).isoformat()


def today():
    return date.today().isoformat()


def yesterday():
    return (date.today() - timedelta(days=1)).isoformat()
