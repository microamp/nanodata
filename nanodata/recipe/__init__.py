#-*- coding: utf-8 -*-

from datetime import date, timedelta


def yesterday():
    return (date.today() - timedelta(days=1)).isoformat()
