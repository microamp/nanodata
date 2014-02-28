#-*- coding: utf-8 -*-

from datetime import datetime

from nanodata import TYPE_INVOICE, TYPE_PAYMENT, FORMAT_DT


def _dt(dtstr):
    return datetime.strptime(dtstr, FORMAT_DT)


def docs(start, end, types=(TYPE_INVOICE, TYPE_PAYMENT,)):
    return {"type": {"$in": types},
            "start_date": {"$gte": _dt(start)},
            "end_date": {"$lte": _dt(end)}}
