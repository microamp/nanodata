#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe08
    ------------------------

    [Recipe #08]

    Monthly Billing Documents ($)
"""

from functools import partial
from logging import getLogger

from nanodata import (config, COLLECTION_BILLING, COLUMN_MAPPING_BILLING,
                      TYPE_INVOICE, TYPE_PAYMENT, TYPE_DEBIT, TYPE_CREDIT,)
from nanodata.lib import db, queries as q, dataframe as df, fn, plot
from nanodata.recipe import offset, last_day_prev_month

logger = getLogger(__name__)

PLOT_FUNC = plot.build_subplots
PLOT_INFO = {"title": "Monthly Billing Documents ($)",
             "nrows": 2,
             "ncols": 2,
             "sharex": True,
             "kind": "line",
             "xlabel": "Month",
             "rot": 90,
             "coordinates": {"Invoice": (0, 0),
                             "Payment": (0, 1),
                             "Debit": (1, 0),
                             "Credit": (1, 1)}}


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        start, end = offset(config.PREV_MONTHS), last_day_prev_month()
        docs = db_source.read(COLLECTION_BILLING,
                              query=q.docs(start,
                                           end=end,
                                           types=(TYPE_INVOICE,
                                                  TYPE_PAYMENT,
                                                  TYPE_DEBIT,
                                                  TYPE_CREDIT,)))
        logger.debug("Billing documents from {start} to {end}: "
                     "{count}".format(start=start,
                                      end=end,
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING_BILLING),
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("start", "type",)),
                       partial(df.sum, unstack=True),
                       partial(df.rename_columns,
                               columns=((TYPE_INVOICE, "Invoice"),
                                        (TYPE_PAYMENT, "Payment"),
                                        (TYPE_DEBIT, "Debit"),
                                        (TYPE_CREDIT, "Credit"))),
                       df.fill_na)
        return f(docs)


if __name__ == "__main__":
    cook()
