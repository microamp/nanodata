#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe03
    ------------------------

    [Recipe #03]

    Monthly Invoices Created
"""

from functools import partial
from logging import getLogger

from nanodata import config, COLUMN_MAPPING, TYPE_INVOICE
from nanodata.lib import db, queries as q, dataframe as df, fn
from nanodata.recipe import yesterday

PLOT_INFO = {"title": "Monthly Invoices Created",
             "kind": "bar"}
X_LABEL = "Date"
Y_LABEL = "Number of Invoices"

logger = getLogger(__name__)


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        docs = db_source.read(config.DB_SOURCE["collection"],
                              query=q.docs(config.OFFSET,
                                           end=yesterday(),
                                           types=(TYPE_INVOICE,)))
        logger.debug("invoices from {start} to {end}: "
                     "{count}".format(start=config.OFFSET,
                                      end=yesterday(),
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING),
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("start",)),
                       df.count)
        return f(docs)


if __name__ == "__main__":
    cook()
