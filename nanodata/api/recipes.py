#-*- coding: utf-8 -*-

from flask import Blueprint, make_response, jsonify
from logging import getLogger
import simplejson

from nanodata import config
from nanodata.lib import import_recipe, dataframe as df
from nanodata.lib.plot import build_plot
from nanodata.lib.cache import CacheHelper
from nanodata.recipe import yesterday

bp_recipes = Blueprint("recipes", __name__)
logger = getLogger(__name__)

CACHE_ENABLED = getattr(config, "CACHE_ENABLED", True)  # cache on by default


def _build_df(recipe_module, recipe_no, cache_enabled=True):
    """Read data frame from cache if found, otherwise build it from scratch."""
    def _cache_key(start, end, num):
        return "{start}-{end}-{num}".format(start=start.replace("-", ""),
                                            end=end.replace("-", ""),
                                            num=num)

    with CacheHelper(**config.DB_CACHE) as cache:
        cache_key = _cache_key(config.OFFSET, yesterday(), recipe_no)
        try:
            cached = cache.get(cache_key)
        except Exception as e:
            logger.error(e.message)
            cached = None

        if cached and cache_enabled:
            logger.debug("Reading recipe #{0} from cache "
                         "(key: '{1}')".format(recipe_no, cache_key))
            df_ = df.from_json(cached)
        else:
            logger.debug("Building recipe #{0} "
                         "(key: '{1}')".format(recipe_no, cache_key))
            df_ = recipe_module.cook()
            cache.set(cache_key, df.to_json(df_))
        return df_


@bp_recipes.route("/recipes/<recipe_no>/json")
def json(recipe_no):
    def _jsonify(v):
        return v if isinstance(v, dict) else _jsonify(simplejson.loads(v))

    m = import_recipe(recipe_no)
    df_ = _build_df(m, recipe_no, cache_enabled=CACHE_ENABLED)
    return jsonify(no=recipe_no, plotinfo=m.PLOT_INFO,
                   dataframe=_jsonify(df.to_json(df_)))


@bp_recipes.route("/recipes/<recipe_no>/plot")
def plot(recipe_no):
    m = import_recipe(recipe_no)
    df_ = _build_df(m, recipe_no, cache_enabled=CACHE_ENABLED)
    p = build_plot(df_, m.X_LABEL, m.Y_LABEL, **m.PLOT_INFO)

    response = make_response(p.getvalue())
    response.headers["Content-Type"] = "image/png"

    return response
