#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe08
    ------------------------

    [Recipe #08]

    Monthly Invoices/Payments (Total Amount)
"""

from functools import partial
from logging import getLogger

from nanodata import config, COLUMN_MAPPING, TYPE_INVOICE, TYPE_PAYMENT
from nanodata.lib import db, queries as q, dataframe as df, fn
from nanodata.recipe import yesterday

PLOT_INFO = {"title": "Monthly Invoices/Payments (Total Amount)",
             "kind": "bar"}
X_LABEL = "Month"
Y_LABEL = "Amount ($)"

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
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("year", "month", "type",)),
                       partial(df.sum, unstack=True),
                       partial(df.rename_columns, columns=((0, "Invoice"),
                                                           (1, "Payment"),)))
        return f(docs)


if __name__ == "__main__":
    cook()
