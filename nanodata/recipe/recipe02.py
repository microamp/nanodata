#-*- coding: utf-8 -*-

from functools import partial
from logging import getLogger

from nanodata import config, COLUMN_MAPPING
from nanodata.lib import (db, queries as q, dataframe as df, fn,)
from nanodata.recipe import yesterday

PLOT_INFO = {"title": "Daily Invoices (Total Amount)",
             "kind": "line"}
X_LABEL = "Dates"
Y_LABEL = "Total Amount ($)"

logger = getLogger(__name__)


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        invoices = db_source.read(config.DB_SOURCE["collection"],
                                  query=q.invoices(config.OFFSET,
                                                   end=yesterday()))
        logger.debug("invoices from {start} to {end}: "
                     "{count}".format(start=config.OFFSET,
                                      end=yesterday(),
                                      count=invoices.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING),
                       partial(df.dt_to_d, keys=("start",)),
                       partial(df.group_by, keys=("start",)),
                       partial(df.sum, key="amount"))
        return f(invoices)


if __name__ == "__main__":
    cook()
