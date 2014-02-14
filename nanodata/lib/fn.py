#-*- coding: utf-8 -*-


def compose(func, *funcs):
    """Compose multiple functions together."""
    def _compose(*args, **kwargs):
        return reduce(lambda arg, f: f(arg),
                      funcs,
                      func(*args, **kwargs))

    return _compose
