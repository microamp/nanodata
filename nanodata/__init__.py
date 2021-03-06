#-*- coding: utf-8 -*-

import os
import logging.config
from itertools import ifilter

from flask import Flask

__version__ = "0.1.3"

COLLECTION_BILLING = "billing"
COLLECTION_CUSTOMER = "customer"

TYPE_INVOICE = 0
TYPE_PAYMENT = 1
TYPE_DEBIT = 2
TYPE_CREDIT = 3
TYPE_REFUND = 4

FORMAT_DT = "%Y-%m-%d"

COLUMN_MAPPING_BILLING = {"type": "type",
                          "amount": "amount",
                          "start_date": "start",
                          "customer": "customer"}
COLUMN_MAPPING_CUSTOMER = {"start_date": "start"}


class DefaultConfig(object):
    DEBUG = True
    SCRIPT_NAME = ""


def from_pyfile(config_file):
    config = DefaultConfig()
    config.__file__ = os.path.join(os.getcwd(), config_file)
    try:
        execfile(config_file, config.__dict__)
    except IOError:
        raise
    return config


config_paths = ("/srv/www/nanodata/current/conf/settings.py",
                "nanodata/settings.py",
                "settings.py",)
config = from_pyfile(list(ifilter(lambda path: os.path.exists(path),
                                  config_paths))[0])


def _before_request():
    pass


def _teardown_request(e):
    pass


def create_app():
    global config

    # flask app
    app = Flask(__name__)

    # app config
    app.config.from_object(config)

    # before/after request
    app.before_request(_before_request)
    app.teardown_request(_teardown_request)

    # logging config
    logging_config = os.environ.get("LOGGING_CONFIG",
                                    app.config.get("LOGGING_CONFIG"))
    if logging_config:
        logging.config.fileConfig(logging_config)

    # blueprints
    import nanodata.api.recipe
    app.register_blueprint(nanodata.api.recipe.bp_recipe)

    return app
