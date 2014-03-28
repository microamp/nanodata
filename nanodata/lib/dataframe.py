#-*- coding: utf-8 -*-

from datetime import date, datetime

import pandas as pd

from nanodata import COLUMN_MAPPING_BILLING


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
    mapping = mapping or COLUMN_MAPPING_BILLING
    return pd.DataFrame({v: d[k] for k, v in mapping.iteritems()}
                        for d in data)


def dt_to_d(df, keys=("start",)):
    """Convert datetime objects in given keys to date objects."""
    for k in keys:
        df[k] = df[k].map(lambda dt: date(dt.year, dt.month, dt.day))

    return df


def to_monthly(df, key="start"):
    """Make each datetime object monthly by offseting to 1st of the month."""
    df[key] = df[key].map(lambda dt: date(dt.year, dt.month, 1))
    return df


def to_id(df, key="customer"):
    """Convert each object to its string representation of the ID."""
    df[key] = df[key].map(lambda obj: unicode(obj.id))
    return df


def round_nums(df, keys=("amount",)):
    """Round values in given keys to their nearest whole numbers."""
    for k in keys:
        df[k] = df[k].map(lambda n: int(round(n)))

    return df


def rename_columns(df, columns=()):
    """Rename columns."""
    return df.rename(columns=dict(columns), inplace=False)


def rename_index(df, fn=None):
    """Rename time indices to %b %Y."""
    monthly = lambda dt: datetime.strftime(
        datetime.strptime(dt if isinstance(dt, basestring) else
                          date(dt.year, dt.month, dt.day).isoformat(),
                          "%Y-%m-%d"),
        "%b %Y"
    )
    return df.rename(index=fn or monthly)


def group_by(df, keys=("start",)):
    """Group by keys given."""
    return df.groupby(keys)


def to_series(df, key="amount"):
    """Convert to pandas.Series."""
    return pd.Series(df[key])


def drop_duplicates(df, columns=()):
    """Remove duplicate rows."""
    return df.drop_duplicates(cols=columns)


def fill_na(df, value=0):
    """Fill nil with ``value``."""
    return df.fillna(value)


def count(dfgb, unstack=False):
    """Apply count to a DataFrameGroupBy object."""
    return dfgb.size().unstack() if unstack else dfgb.size()


def sum(dfgb, key="amount", unstack=False):
    """Apply sum to a DataFrameGroupBy object."""
    return dfgb[key].sum().unstack() if unstack else dfgb[key].sum()
