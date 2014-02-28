#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe06
    ------------------------

    [Recipe #06]

    Daily Invoices/Payments (Total Amount)
"""

from functools import partial
from logging import getLogger

from nanodata import config, COLUMN_MAPPING, TYPE_INVOICE, TYPE_PAYMENT
from nanodata.lib import db, queries as q, dataframe as df, fn
from nanodata.recipe import yesterday

PLOT_INFO = {"title": "Daily Invoices/Payments (Total Amount)",
             "kind": "line"}
X_LABEL = "Date"
Y_LABEL = "Total amount ($)"

logger = getLogger(__name__)


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        docs = db_source.read(config.DB_SOURCE["collection"],
                              query=q.docs(config.OFFSET,
                                           end=yesterday(),
                                           types=(TYPE_INVOICE,
                                                  TYPE_PAYMENT,)))
        logger.debug("invoices/payments from {start} to {end}: "
                     "{count}".format(start=config.OFFSET,
                                      end=yesterday(),
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING),
                       partial(df.dt_to_d, keys=("start",)),
                       partial(df.group_by, keys=("start", "type",)),
                       partial(df.sum, key="amount", unstack=True),
                       partial(df.rename_columns, columns=((0, "Invoice"),
                                                           (1, "Payment"),)))
        return f(docs)


if __name__ == "__main__":
    cook()
