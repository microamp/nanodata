#-*- coding: utf-8 -*-

from importlib import import_module

from flask import Blueprint, make_response

from nanodata.lib.plot import build_plot

bp_recipes = Blueprint("recipes", __name__)


def _import_module(recipe_no):
    return import_module("nanodata.recipe.recipe{no}".format(no=recipe_no))


@bp_recipes.route("/recipes/<recipe_no>/json")
def json(recipe_no):
    pass


@bp_recipes.route("/recipes/<recipe_no>/plot")
def plot(recipe_no):
    m = _import_module(recipe_no)
    p = build_plot(m.cook(), title=m.TITLE, labels=m.LABELS,
                   plot_type=m.PLOT_TYPE)
    response = make_response(p.getvalue())
    response.headers["Content-Type"] = "image/png"

    return response
