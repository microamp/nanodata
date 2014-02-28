#-*- coding: utf-8 -*-

from StringIO import StringIO

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
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
