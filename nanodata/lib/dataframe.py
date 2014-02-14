#-*- coding: utf-8 -*-

from datetime import date

import pandas as pd

from nanodata import COLUMN_MAPPING


def build_df(data, mapping=None):
    """Convert a PyMongo cursor object to a DataFrame object."""
    mapping = mapping or COLUMN_MAPPING
    return pd.DataFrame({mapping[k]: d[k] for k in mapping.keys()}
                        for d in data)


def dt_to_d(df, keys=("start",)):
    """Convert datetime objects in given keys to date objects."""
    def _dt_to_d(dt):
        return date(dt.year, dt.month, dt.day)

    for k in keys:
        df[k] = df[k].map(_dt_to_d)

    return df


def group_by(df, keys=("start",)):
    """Group by keys given."""
    return df.groupby(keys)


def count(dfgb, unstack=False):
    """Apply count to a DataFrameGroupBy object."""
    return dfgb.size().unstack() if unstack else dfgb.size()
