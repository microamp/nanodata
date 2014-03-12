#-*- coding: utf-8 -*-


def merge(*dicts):
    return {k: v for d in dicts for k, v in d.iteritems()}
