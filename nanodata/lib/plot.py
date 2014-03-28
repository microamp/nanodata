#-*- coding: utf-8 -*-

from StringIO import StringIO

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib import pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

sns.set_color_palette("deep", desat=.9)

FMT_MONTHLY = mdates.DateFormatter("%Y")


def _output_png(figure):
    try:
        figure.tight_layout()

        canvas = FigureCanvas(figure)
        png_output = StringIO()
        canvas.print_png(png_output)

        return png_output
    finally:
        figure.clear()  # avoid chart from overlapping


def build_plot(df, xlabel="X-Axis", ylabel="Y-Axis", **kwargs):
    plot = df.plot(**kwargs)
    plot.set_xlabel(xlabel)
    plot.set_ylabel(ylabel)

    figure = plot.get_figure()

    return _output_png(figure)


def build_subplots(df, **kwargs):
    figure, axes = plt.subplots(nrows=kwargs["rows"], ncols=kwargs["cols"])
    plt.suptitle(kwargs["title"])

    for column, (x, y) in kwargs["coordinates"].iteritems():
        df[column].plot(ax=axes[x, y], kind=kwargs["kind"])
        axes[x, y].set_title(column)
        axes[x, y].set_xlabel(kwargs["xlabel"])

    return _output_png(figure)
