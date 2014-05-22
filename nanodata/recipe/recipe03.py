#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe03
    ------------------------

    [Recipe #03]

    Monthly Payments (#)
"""

from functools import partial
from logging import getLogger

from nanodata import (config, COLLECTION_BILLING, COLUMN_MAPPING_BILLING,
                      TYPE_PAYMENT,)
from nanodata.lib import db, queries as q, dataframe as df, fn, plot
from nanodata.lib.dt import offset, first_day_current_month

logger = getLogger(__name__)

PLOT_FUNC = plot.build_plot
PLOT_INFO = {"title": "Monthly Payments (#)",
             "kind": "bar",
             "xlabel": "Month",
             "ylabel": "Number of Documents"}


def date_range():
    return offset(config.PREV_MONTHS), first_day_current_month()


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        start, end = date_range()
        docs = db_source.read(COLLECTION_BILLING,
                              query=q.docs(start,
                                           end=end,
                                           types=(TYPE_PAYMENT,)))
        logger.debug("Documents from {start} to {end} (exclusive): "
                     "{count}".format(start=start,
                                      end=end,
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING_BILLING),
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("start",)),
                       df.count)
        return f(docs)


if __name__ == "__main__":
    cook()
