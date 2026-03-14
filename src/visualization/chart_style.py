"""
Les Nuits du Sahel — Chart Style
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

AMBER = '#D4893A'
INDIGO = '#1B2A4A'
RED = '#C0392B'
GRAY = '#4A4A4A'
LIGHT_GRAY = '#E8E8E8'
AMBER_FILL = '#D4893A33'
INDIGO_FILL = '#1B2A4A22'
WHITE = '#FFFFFF'


def apply_style():
    plt.rcParams.update({
        'figure.facecolor': WHITE,
        'axes.facecolor': WHITE,
        'axes.edgecolor': LIGHT_GRAY,
        'axes.grid': True,
        'grid.color': LIGHT_GRAY,
        'grid.linewidth': 0.5,
        'text.color': INDIGO,
        'figure.dpi': 150,
        'savefig.dpi': 150,
        'savefig.bbox': 'tight',
    })


def style_axis(ax, title=None, xlabel=None, ylabel=None):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_color(LIGHT_GRAY)
    ax.spines['bottom'].set_color(LIGHT_GRAY)
    if title:
        ax.set_title(title, fontsize=16, fontweight='bold', color=INDIGO, pad=15)
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=11, color=GRAY)
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=11, color=GRAY)
    ax.tick_params(axis='both', length=0)
    return ax