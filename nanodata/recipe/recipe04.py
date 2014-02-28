#-*- coding: utf-8 -*-

from functools import partial
from logging import getLogger

from nanodata import config, COLUMN_MAPPING, TYPE_PAYMENT
from nanodata.lib import db, queries as q, dataframe as df, fn
from nanodata.recipe import yesterday

PLOT_INFO = {"title": "Daily Payments (Total Amount)",
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
                                           types=(TYPE_PAYMENT,)))
        logger.debug("payments from {start} to {end}: "
                     "{count}".format(start=config.OFFSET,
                                      end=yesterday(),
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING),
                       partial(df.dt_to_d, keys=("start",)),
                       partial(df.group_by, keys=("start",)),
                       partial(df.sum, key="amount"))
        return f(docs)


if __name__ == "__main__":
    cook()
