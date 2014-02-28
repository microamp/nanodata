#-*- coding: utf-8 -*-

from flask import Blueprint, make_response, jsonify
from logging import getLogger
import simplejson

from nanodata import config
from nanodata.lib import import_recipe, dataframe as df
from nanodata.lib.plot import build_plot
from nanodata.lib.cache import CacheHelper

bp_recipes = Blueprint("recipes", __name__)
logger = getLogger(__name__)

USE_CACHE = False


def _build_df(recipe_module, recipe_no, use_cache=USE_CACHE):
    """Read data frame from cache if found, otherwise build it from scratch."""
    with CacheHelper(**config.DB_CACHE) as cache:
        try:
            cached = cache.get(recipe_no)
        except Exception as e:
            logger.error(e.message)
            cached = None

        if cached and use_cache:
            logger.debug("reading recipe #{0} from cache".format(recipe_no))
            df_ = df.from_json(cached)
        else:
            logger.debug("building recipe #{0} from scratch".format(recipe_no))
            df_ = recipe_module.cook()
            cache.set(recipe_no, df.to_json(df_))
        return df_


@bp_recipes.route("/recipes/<recipe_no>/json")
def json(recipe_no):
    def _jsonify(v):
        return v if isinstance(v, dict) else _jsonify(simplejson.loads(v))

    m = import_recipe(recipe_no)
    df_ = _build_df(m, recipe_no)
    return jsonify(no=recipe_no, plotinfo=m.PLOT_INFO,
                   dataframe=_jsonify(df.to_json(df_)))


@bp_recipes.route("/recipes/<recipe_no>/plot")
def plot(recipe_no):
    m = import_recipe(recipe_no)
    df_ = _build_df(m, recipe_no)
    p = build_plot(df_, m.X_LABEL, m.Y_LABEL, **m.PLOT_INFO)

    response = make_response(p.getvalue())
    response.headers["Content-Type"] = "image/png"

    return response
