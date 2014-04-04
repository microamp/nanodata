#-*- coding: utf-8 -*-

from flask import Blueprint, make_response, jsonify
from logging import getLogger
import simplejson

from nanodata import config
from nanodata.lib import import_recipe, dataframe as df
from nanodata.lib.cache import CacheHelper
from nanodata.lib.dt import offset, first_day_current_month
from nanodata.recipe import __all__ as all_recipes

bp_recipes = Blueprint("recipes", __name__)
logger = getLogger(__name__)


def _build_df(recipe_module, recipe_no, cache_enabled=True):
    """Read data frame from cache if found, otherwise build it from scratch."""
    def _cache_key(start, end, num, day=None):
        return "{num}-{start}-{end}".format(num=num,
                                            start=start.replace("-", ""),
                                            end=end.replace("-", ""))

    with CacheHelper(**config.DB_CACHE) as cache:
        cache_key = _cache_key(offset(config.PREV_MONTHS),
                               first_day_current_month(),
                               recipe_no)
        logger.debug("Cache keys: {0}".format(", ".join(sorted(cache.keys()))))

        # read from cache
        try:
            cached = cache.get(cache_key)
        except Exception as e:
            logger.error("Error while reading from cache: "
                         "{0}".format(e.message))
            cached = None

        if cached and cache_enabled:  # found in cache
            logger.debug("Reading recipe #{0} from cache "
                         "(key: '{1}')".format(recipe_no, cache_key))
            df_ = df.from_json(cached)
        else:  # missing in cache
            logger.debug("Building recipe #{0} "
                         "(key: '{1}')".format(recipe_no, cache_key))
            df_ = recipe_module.cook()
            try:
                # update cache
                cache.set(cache_key, df.to_json(df_))
                logger.debug("Cache added: {0}".format(cache_key))
            except Exception as e:
                logger.error("Error while updating cache: "
                             "{0}".format(e.message))

        logger.debug("Displaying the first 10 rows:")
        logger.debug(df_.head(10))

        return df.rename_index(df_)


@bp_recipes.route("/recipes/<recipe_no>/json")
def json(recipe_no):
    def _jsonify(v):
        return v if isinstance(v, dict) else _jsonify(simplejson.loads(v))

    m = import_recipe(recipe_no)
    df_ = _build_df(m, recipe_no,
                    cache_enabled=getattr(config, "CACHE_ENABLED", True))
    return jsonify(no=recipe_no, plotinfo=m.PLOT_INFO,
                   dataframe=_jsonify(df.to_json(df_)))


@bp_recipes.route("/recipes/<recipe_no>/plot")
def plot(recipe_no):
    m = import_recipe(recipe_no)
    df_ = _build_df(m, recipe_no,
                    cache_enabled=getattr(config, "CACHE_ENABLED", True))
    p = m.PLOT_FUNC(df_, **m.PLOT_INFO)

    response = make_response(p.getvalue())
    response.headers["Content-Type"] = "image/png"

    return response


@bp_recipes.route("/recipes")
def list_all():
    def _build():
        return [{"no": recipe_no,
                 "title": import_recipe(recipe_no).PLOT_INFO["title"]}
                for recipe_no in sorted(map(lambda r: r.lstrip("recipe"),
                                            all_recipes),
                                        key=lambda r: int(r))]

    return jsonify(recipes=_build())
