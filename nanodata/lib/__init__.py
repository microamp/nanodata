#-*- coding: utf-8 -*-

from importlib import import_module


def import_recipe(recipe_no):
    return import_module("nanodata.recipe.recipe{no}".format(no=recipe_no))
