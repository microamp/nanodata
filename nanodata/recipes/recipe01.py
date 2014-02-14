#-*- coding: utf-8 -*-

from functools import partial

from nanodata import config, COLUMN_MAPPING
from nanodata.lib import (mongo, queries as q, dataframe as df, fn,)

TITLE = "# of invoices"


def cook():
    # read from source
    with mongo.MongoHelper(config.DB_SOURCE["hosts"],
                           config.DB_SOURCE["name"]) as db_source:
        invoices = db_source.read(config.DB_SOURCE["collection"],
                                  query=q.invoices(config.OFFSET))
        print("invoice since {start}: {count}".format(start=config.OFFSET,
                                                      count=invoices.count()))

        # prepare recipe and start cooking!
        f = fn.compose(partial(df.build_df, mapping=COLUMN_MAPPING),
                       partial(df.dt_to_d, keys=("start",)),
                       partial(df.group_by, keys=("start",)),
                       df.count)
        cooked = f(invoices)
        print(cooked)

        # TODO: write to target
#        with mongo.MongoHelper(config.DB_TARGET["hosts"],
#                               config.DB_TARGET["name"]) as db_target:
#            pass


if __name__ == "__main__":
    cook()
