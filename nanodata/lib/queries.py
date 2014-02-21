#-*- coding: utf-8 -*-

from datetime import datetime

from nanodata import TYPE_INVOICE, FORMAT_DT


def _dt(dtstr):
    return datetime.strptime(dtstr, FORMAT_DT)


def invoices(start, end=None):
    return ({"type": TYPE_INVOICE,
             "start_date": {"$gte": _dt(start)}}
            if end is None else
            {"type": TYPE_INVOICE,
             "start_date": {"$gte": _dt(start)},
             "end_date": {"$lte": _dt(end)}})
