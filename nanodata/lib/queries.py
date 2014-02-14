#-*- coding: utf-8 -*-

from datetime import datetime

from nanodata import TYPE_INVOICE, FORMAT_DT


def _dt(dtstr):
    return datetime.strptime(dtstr, FORMAT_DT)


def invoices(start):
    return {"type": TYPE_INVOICE,
            "start_date": {"$gte": _dt(start)}}
