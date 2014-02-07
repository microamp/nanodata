#-*- coding: utf-8 -*-

from nanodata import create_app
from nanodata import config

app = create_app()


if __name__ == "__main__":
    app.run("0.0.0.0", **{attr.lower(): getattr(config, attr)
                          for attr in ("PORT", "DEBUG",)
                          if hasattr(config, attr)})
