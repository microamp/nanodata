#-*- coding: utf-8 -*-

from flask import Blueprint, jsonify

bp_recipes = Blueprint("recipes", __name__)


@bp_recipes.route("/recipes/<recipe_no>")
def recipes(recipe_no):
    return jsonify(name="/recipes/{0}".format(recipe_no))
