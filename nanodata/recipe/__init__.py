#-*- coding: utf-8 -*-

import os
import glob

__all__ = [os.path.splitext(os.path.basename(f))[0] for f in
           glob.glob("{0}/recipe*.py".format(os.path.dirname(__file__)))]
