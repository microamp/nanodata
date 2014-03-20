#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe10
    ------------------------

    [Recipe #10]

    New Customers
"""

from functools import partial
from logging import getLogger

from nanodata import config, COLLECTION_CUSTOMER, COLUMN_MAPPING_CUSTOMER
from nanodata.lib import db, queries as q, dataframe as df, fn, plot
from nanodata.recipe import offset, yesterday

logger = getLogger(__name__)

PLOT_FUNC = plot.build_plot
PLOT_INFO = {"title": "New Customers",
             "kind": "bar",
             "xlabel": "Month",
             "ylabel": "Number of Customers"}


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        start, end = offset(config.PREV_MONTHS), yesterday()
        customers = db_source.read(COLLECTION_CUSTOMER,
                                   query=q.customers(start, end))
        logger.debug("Customers from {start} to {end}: "
                     "{count}".format(start=start,
                                      end=end,
                                      count=customers.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING_CUSTOMER),
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("start",)),
                       df.count,
                       df.rename_index_monthly)
        return f(customers)


if __name__ == "__main__":
    cook()
