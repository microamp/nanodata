#-*- coding: utf-8 -*-

from StringIO import StringIO

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib import pyplot as plt
import seaborn as sns

sns.set_color_palette("deep", desat=.9)


def build_plot(df, xlabel="X-Axis", ylabel="Y-Axis", **kwargs):
    plot = df.plot(**kwargs)
    plot.set_xlabel(xlabel)
    plot.set_ylabel(ylabel)

    figure = plot.get_figure()
    figure.autofmt_xdate()
    figure.tight_layout()

    canvas = FigureCanvas(figure)
    png_output = StringIO()
    canvas.print_png(png_output)

    figure.clear()  # avoid chart from overlapping

    return png_output


def build_subplots(df, **kwargs):
    figure, axes = plt.subplots(nrows=kwargs["rows"], ncols=kwargs["cols"])
    plt.suptitle(kwargs["title"])

    for column, (x, y) in kwargs["coordinates"].iteritems():
        df[column].plot(ax=axes[x, y], kind=kwargs["kind"])
        axes[x, y].set_title(column)
        axes[x, y].set_xlabel(kwargs["xlabel"])

    figure.autofmt_xdate()
    figure.tight_layout()

    canvas = FigureCanvas(figure)
    png_output = StringIO()
    canvas.print_png(png_output)

    figure.clear()  # avoid chart from overlapping

    return png_output
