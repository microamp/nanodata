#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe03
    ------------------------

    [Recipe #03]

    Monthly Invoices Created
"""

from functools import partial
from logging import getLogger

from nanodata import (config, COLLECTION_BILLING, COLUMN_MAPPING_BILLING,
                      TYPE_INVOICE,)
from nanodata.lib import db, queries as q, dataframe as df, fn, plot
from nanodata.recipe import offset, yesterday

logger = getLogger(__name__)

PLOT_FUNC = plot.build_plot
PLOT_INFO = {"title": "Monthly Invoices Created",
             "kind": "bar",
             "xlabel": "Month",
             "ylabel": "Number of Invoices"}


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        start, end = offset(config.PREV_MONTHS), yesterday()
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
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("start",)),
                       df.count)
        return f(docs)


if __name__ == "__main__":
    cook()
