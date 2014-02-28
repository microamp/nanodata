#-*- coding: utf-8 -*-

from StringIO import StringIO

from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import seaborn as sns


def build_plot(df, title="Title", labels=("X-axis", "Y-axis",),
               plot_type="line"):
    sns.set_color_palette("deep", desat=.3)

    plot = df.plot(kind=plot_type, title=title)
    plot.set_xlabel(labels[0])
    plot.set_ylabel(labels[1])

    figure = plot.get_figure()
    figure.autofmt_xdate()
    figure.tight_layout()

    canvas = FigureCanvas(figure)
    png_output = StringIO()
    canvas.print_png(png_output)

    figure.clear()  # avoid chart from overlapping

    return png_output
