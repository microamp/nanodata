#-*- coding: utf-8 -*-

from flask import Blueprint, make_response
from logging import getLogger

from nanodata import config
from nanodata.lib import import_recipe, dataframe as df
from nanodata.lib.plot import build_plot
from nanodata.lib.cache import CacheHelper

bp_recipes = Blueprint("recipes", __name__)
logger = getLogger(__name__)


@bp_recipes.route("/recipes/<recipe_no>/json")
def json(recipe_no):
    pass


@bp_recipes.route("/recipes/<recipe_no>/plot")
def plot(recipe_no):
    m = import_recipe(recipe_no)

    # read from cache otherwise build from scratch
    with CacheHelper(**config.DB_CACHE) as cache:
        cached = cache.get(recipe_no)
        if cached:
            logger.debug("reading recipe #{0} from cache".format(recipe_no))
            df_ = df.from_json(cached)
        else:
            logger.debug("building recipe #{0} from scratch".format(recipe_no))
            df_ = m.cook()
            cache.set(recipe_no, df.to_json(df_))

    # build plot
    p = build_plot(df_, title=m.TITLE, labels=m.LABELS, plot_type=m.PLOT_TYPE)

    response = make_response(p.getvalue())
    response.headers["Content-Type"] = "image/png"

    return response
