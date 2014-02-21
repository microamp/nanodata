#-*- coding: utf-8 -*-

from datetime import date

import pandas as pd

from nanodata import COLUMN_MAPPING


def from_json(json):
    """Build a DataFrame object from JSON."""
    def _from_json():
        try:
            return pd.read_json(json)
        except ValueError:
            return pd.read_json(json, typ="series")
        except:
            raise

    return _from_json() if json is not None else None


def to_json(df):
    """Export a DataFrame object as JSON."""
    return df.to_json() if df is not None else None


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


def sum(dfgb, key="amount", unstack=False):
    """Apply sum to a DataFrameGroupBy object."""
    return dfgb[key].sum().unstack() if unstack else dfgb[key].sum()
