#-*- coding: utf-8 -*-

import os
import glob
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

__all__ = [os.path.splitext(os.path.basename(f))[0] for f in
           glob.glob("{0}/recipe*.py".format(os.path.dirname(__file__)))]


def offset(prev_months):
    temp = date.today() - relativedelta(months=prev_months + 1)
    return date(temp.year, temp.month, 1).isoformat()


def yesterday():
    return (date.today() - timedelta(days=1)).isoformat()
