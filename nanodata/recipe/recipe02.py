#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe02
    ------------------------

    [Recipe #02]

    Daily Invoices ($)
"""

from functools import partial
from logging import getLogger

from nanodata import (config, COLLECTION_BILLING, COLUMN_MAPPING_BILLING,
                      TYPE_INVOICE,)
from nanodata.lib import db, queries as q, dataframe as df, fn, plot
from nanodata.lib.dt import offset, first_day_current_month

logger = getLogger(__name__)

PLOT_FUNC = plot.build_plot
PLOT_INFO = {"title": "Daily Invoices ($)",
             "kind": "line",
             "xlabel": "Date",
             "ylabel": "Amount ($)"}


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        start, end = offset(config.PREV_MONTHS), first_day_current_month()
        docs = db_source.read(COLLECTION_BILLING,
                              query=q.docs(start,
                                           end=end,
                                           types=(TYPE_INVOICE,)))
        logger.debug("Invoices from {start} to {end}: "
                     "{count}".format(start=start,
                                      end=end,
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING_BILLING),
                       partial(df.dt_to_d, keys=("start",)),
                       partial(df.group_by, keys=("start",)),
                       partial(df.sum, key="amount"))
        return f(docs)


if __name__ == "__main__":
    cook()
