#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe07
    ------------------------

    [Recipe #07]

    Monthly Billing Documents (Total Amount)
"""

from functools import partial
from logging import getLogger

from nanodata import (config, COLUMN_MAPPING, TYPE_INVOICE, TYPE_PAYMENT,
                      TYPE_DEBIT, TYPE_CREDIT, TYPE_REFUND,)
from nanodata.lib import db, queries as q, dataframe as df, fn, plot
from nanodata.recipe import offset, yesterday

logger = getLogger(__name__)

PLOT_FUNC = plot.build_plot
PLOT_INFO = {"title": "Monthly Billing Documents (Total Amount)",
             "kind": "barh",
             "stacked": True,
             "xlabel": "Total Amount ($)",
             "ylabel": "Month"}


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        start, end = offset(config.PREV_MONTHS), yesterday()
        docs = db_source.read(config.DB_SOURCE["collection"],
                              query=q.docs(start,
                                           end=end,
                                           types=(TYPE_INVOICE,
                                                  TYPE_PAYMENT,
                                                  TYPE_DEBIT,
                                                  TYPE_CREDIT,
                                                  TYPE_REFUND,)))
        logger.debug("Billing documents from {start} to {end}: "
                     "{count}".format(start=start,
                                      end=end,
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING),
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("start", "type",)),
                       partial(df.sum, unstack=True),
                       partial(df.rename_columns,
                               columns=((TYPE_INVOICE, "Invoice"),
                                        (TYPE_PAYMENT, "Payment"),
                                        (TYPE_DEBIT, "Debit"),
                                        (TYPE_CREDIT, "Credit"),
                                        (TYPE_REFUND, "Refund"),)),
                       df.fill_na)
        return f(docs)


if __name__ == "__main__":
    cook()
