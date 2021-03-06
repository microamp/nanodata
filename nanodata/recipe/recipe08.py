#-*- coding: utf-8 -*-

"""
    nanodata.recipe.recipe08
    ------------------------

    [Recipe #08]

    New Customers (#)
"""

from functools import partial
from logging import getLogger

from nanodata import config, COLLECTION_CUSTOMER, COLUMN_MAPPING_CUSTOMER
from nanodata.lib import db, queries as q, dataframe as df, fn, plot
from nanodata.lib.dt import offset, first_day_current_month

logger = getLogger(__name__)

PLOT_FUNC = plot.build_plot
PLOT_INFO = {"title": "New Customers (#)",
             "kind": "bar",
             "xlabel": "Month",
             "ylabel": "Number of Customers"}


def date_range():
    return offset(config.PREV_MONTHS), first_day_current_month()


def cook():
    # read from source
    with db.DatabaseHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        brand = db_source.read_one(COLLECTION_CUSTOMER,
                                   query={"name": config.BRAND_NAME})
        start, end = date_range()
        docs = db_source.read(COLLECTION_CUSTOMER,
                              query=q.customers(brand["_id"],
                                                start,
                                                end))
        logger.debug("Documents from {start} to {end} (exclusive): "
                     "{count}".format(start=start,
                                      end=end,
                                      count=docs.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING_CUSTOMER),
                       partial(df.to_monthly, key="start"),
                       partial(df.group_by, keys=("start",)),
                       df.count)
        return f(docs)


if __name__ == "__main__":
    cook()
