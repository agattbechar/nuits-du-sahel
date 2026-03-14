"""
Les Nuits du Sahel — Chart Generation
=======================================
Run from project root:  python3 src/visualization/charts.py
"""

import csv
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from src.visualization.chart_style import (
    apply_style, style_axis,
    AMBER, INDIGO, RED, GRAY, LIGHT_GRAY, WHITE
)

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'nuits_data')
OUT_DIR = os.path.join(os.path.dirname(__file__), '..', '..', 'site', 'assets', 'charts')
os.makedirs(OUT_DIR, exist_ok=True)

THRESHOLD = 26


def read_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    with open(path, 'r') as f:
        return list(csv.DictReader(f))


def recompute_tropical_at_threshold(nightly_data, threshold=THRESHOLD):
    months = defaultdict(lambda: {'total': 0, 'tropical': 0, 'min_temps': []})
    years = defaultdict(lambda: {'total': 0, 'tropical': 0})
    year_months = defaultdict(lambda: {'total': 0, 'tropical': 0})

    for row in nightly_data:
        min_temp = float(row['min_temp'])
        month = int(row['month'])
        year = int(row['year'])
        is_hot = 1 if min_temp > threshold else 0
        months[month]['total'] += 1
        months[month]['tropical'] += is_hot
        months[month]['min_temps'].append(min_temp)
        years[year]['total'] += 1
        years[year]['tropical'] += is_hot
        ym = f"{year}-{month:02d}"
        year_months[ym]['total'] += 1
        year_months[ym]['tropical'] += is_hot

    month_names = ["", "Jan", "Fev", "Mar", "Avr", "Mai", "Jun",
                   "Jul", "Aou", "Sep", "Oct", "Nov", "Dec"]
    monthly = []
    for m in range(1, 13):
        d = months[m]
        if d['total'] > 0:
            pct = 100 * d['tropical'] / d['total']
            avg_min = sum(d['min_temps']) / len(d['min_temps'])
            monthly.append({'month': m, 'month_name': month_names[m],
                'total': d['total'], 'tropical': d['tropical'],
                'pct': round(pct, 1), 'avg_min': round(avg_min, 1)})
    yearly = []
    for y in sorted(years.keys()):
        d = years[y]
        pct = 100 * d['tropical'] / d['total'] if d['total'] > 0 else 0
        yearly.append({'year': y, 'total': d['total'],
                       'tropical': d['tropical'], 'pct': round(pct, 1)})
    ym_list = []
    for ym in sorted(year_months.keys()):
        d = year_months[ym]
        y, m = ym.split('-')
        pct = 100 * d['tropical'] / d['total'] if d['total'] > 0 else 0
        ym_list.append({'year_month': ym, 'year': int(y), 'month': int(m),
                        'total': d['total'], 'tropical': d['tropical'],
                        'pct': round(pct, 1)})
    return monthly, yearly, ym_list


def recompute_consecutive_runs(nightly_data, threshold=THRESHOLD):
    runs = []
    current_start = None
    current_length = 0
    for row in nightly_data:
        min_temp = float(row['min_temp'])
        if min_temp > threshold:
            if current_start is None:
                current_start = row['night_date']
                current_length = 1
            else:
                current_length += 1
        else:
            if current_length > 0:
                runs.append({'start_date': current_start, 'length': current_length})
            current_start = None
            current_length = 0
    if current_length > 0:
        runs.append({'start_date': current_start, 'length': current_length})
    runs.sort(key=lambda x: x['length'], reverse=True)
    return runs


# ── CHART 1: Monthly ─────────────────────────────────────────

def chart_monthly_tropical(monthly):
    apply_style()
    fig, ax = plt.subplots(figsize=(11, 7))

    names = [m['month_name'] for m in monthly]
    pcts = [m['pct'] for m in monthly]
    worst_idx = pcts.index(max(pcts))
    jul_idx = 6

    colors = []
    for i in range(len(pcts)):
        if i == worst_idx:
            colors.append(RED)
        elif i == jul_idx:
            colors.append('#5B7BA5')
        else:
            colors.append(AMBER)

    bars = ax.bar(names, pcts, color=colors, width=0.65, edgecolor='none')

    # Value labels on bars — skip July to avoid overlap with annotation
    for i, (bar, pct) in enumerate(zip(bars, pcts)):
        if pct > 0 and i != jul_idx:
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1.2,
                    f'{pct:.0f}%', ha='center', va='bottom', fontsize=9,
                    color=INDIGO, fontweight='bold')

    # Title + subtitle using ax.text — reliable positioning
    ax.text(0.0, 1.12,
            'Nouakchott ne dort pas en septembre',
            transform=ax.transAxes, fontsize=18, fontweight='bold', color=INDIGO)
    ax.text(0.0, 1.05,
            f'% des nuits ou le minimum ne descend pas sous {THRESHOLD} C — Nouakchott, 2019-2025',
            transform=ax.transAxes, fontsize=10, color=GRAY)

    style_axis(ax, ylabel='% des nuits')
    ax.set_ylim(0, max(pcts) + 18)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter())

    # Annotation on September (worst)
    worst_bar = bars[worst_idx]
    ax.annotate(
        f'{monthly[worst_idx]["month_name"]} : {pcts[worst_idx]:.0f}%\nle pire mois',
        xy=(worst_bar.get_x() + worst_bar.get_width() / 2, pcts[worst_idx]),
        xytext=(worst_bar.get_x() + worst_bar.get_width() / 2 + 1.8, pcts[worst_idx] + 5),
        fontsize=9, color=RED, fontweight='bold',
        arrowprops=dict(arrowstyle='->', color=RED, lw=1.2),
        ha='center'
    )

    # Annotation on July — positioned high in the chart area, clear of bar labels
    jul_bar = bars[jul_idx]
    ax.annotate(
        f'Juillet : {pcts[jul_idx]:.0f}% — le harmattan modere',
        xy=(jul_bar.get_x() + jul_bar.get_width() / 2, pcts[jul_idx]),
        xytext=(2.0, max(pcts) * 0.55),
        fontsize=9, color='#5B7BA5', fontweight='bold',
        arrowprops=dict(arrowstyle='->', color='#5B7BA5', lw=1.2),
        ha='center'
    )

    plt.subplots_adjust(top=0.82)
    path = os.path.join(OUT_DIR, 'monthly_tropical.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved: {path}")
    return worst_idx, monthly[worst_idx]


# ── CHART 2: Yearly trend ────────────────────────────────────

def chart_yearly_trend(yearly):
    apply_style()
    fig, ax = plt.subplots(figsize=(11, 7))

    years_list = [y['year'] for y in yearly]
    counts = [y['tropical'] for y in yearly]

    ax.plot(years_list, counts, color=AMBER, linewidth=2.5, marker='o',
            markersize=8, markerfacecolor=INDIGO, markeredgecolor=INDIGO,
            markeredgewidth=1.5, zorder=5)
    ax.fill_between(years_list, counts, alpha=0.12, color=AMBER)

    for x, y in zip(years_list, counts):
        ax.text(x, y + 2.5, str(y), ha='center', va='bottom',
                fontsize=11, fontweight='bold', color=INDIGO)

    ax.text(0.0, 1.12,
            '2025 : la pire annee pour les nuits de Nouakchott',
            transform=ax.transAxes, fontsize=18, fontweight='bold', color=INDIGO)
    ax.text(0.0, 1.05,
            f'Nombre de nuits ou le minimum reste au-dessus de {THRESHOLD} C — Nouakchott',
            transform=ax.transAxes, fontsize=10, color=GRAY)

    style_axis(ax, ylabel='Nuits chaudes')
    ax.set_xticks(years_list)
    ax.set_ylim(0, max(counts) + 22)

    worst_idx = counts.index(max(counts))
    ax.annotate(
        f'{years_list[worst_idx]} : {counts[worst_idx]} nuits\nrecord',
        xy=(years_list[worst_idx], counts[worst_idx]),
        xytext=(years_list[worst_idx] - 1.2, counts[worst_idx] + 10),
        fontsize=10, color=RED, fontweight='bold',
        arrowprops=dict(arrowstyle='->', color=RED, lw=1.2),
    )

    avg = sum(counts) / len(counts)
    ax.axhline(y=avg, color=GRAY, linewidth=1, linestyle=':', alpha=0.6)
    ax.text(years_list[0] - 0.3, avg + 1, f'moyenne : {avg:.0f}',
            fontsize=9, color=GRAY, style='italic')

    plt.subplots_adjust(top=0.82)
    path = os.path.join(OUT_DIR, 'yearly_trend.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved: {path}")


# ── CHART 3: Night profile ───────────────────────────────────

def chart_night_profile(hourly_data, worst_month, best_month):
    apply_style()
    fig, ax = plt.subplots(figsize=(11, 7))

    month_names_fr = ["", "Janvier", "Fevrier", "Mars", "Avril", "Mai", "Juin",
                      "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"]

    night_hours = [18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5]
    hour_labels = [f'{h:02d}h' for h in night_hours]

    worst_temps = []
    best_temps = []

    for h in night_hours:
        for row in hourly_data:
            if int(row['month']) == worst_month and int(row['hour']) == h:
                worst_temps.append(float(row['avg_temp']))
            if int(row['month']) == best_month and int(row['hour']) == h:
                best_temps.append(float(row['avg_temp']))

    x = list(range(len(night_hours)))

    ax.plot(x, worst_temps, color=RED, linewidth=2.5, marker='o',
            markersize=6, label=month_names_fr[worst_month], zorder=5)
    ax.plot(x, best_temps, color=INDIGO, linewidth=2.5, marker='s',
            markersize=6, label=month_names_fr[best_month], zorder=5)

    ax.axhline(y=THRESHOLD, color=AMBER, linewidth=1.5, linestyle='--', alpha=0.8)
    ax.text(len(x) - 0.3, THRESHOLD - 0.8, f'seuil {THRESHOLD} C',
            fontsize=9, color=AMBER, fontweight='bold', ha='right')

    ax.fill_between(x, worst_temps, best_temps, alpha=0.06, color=RED)

    ax.set_xticks(x)
    ax.set_xticklabels(hour_labels)

    ax.text(0.0, 1.12,
            'La forme de la nuit a Nouakchott',
            transform=ax.transAxes, fontsize=18, fontweight='bold', color=INDIGO)
    ax.text(0.0, 1.05,
            f'Temperature moyenne heure par heure, 18h-06h — {month_names_fr[worst_month]} vs {month_names_fr[best_month]}',
            transform=ax.transAxes, fontsize=10, color=GRAY)

    style_axis(ax, ylabel='Temperature ( C)')

    worst_min = min(worst_temps)
    worst_min_idx = worst_temps.index(worst_min)
    ax.annotate(
        f'{worst_min:.1f} C a {hour_labels[worst_min_idx]}\nmeme au plus bas, pas de repos',
        xy=(worst_min_idx, worst_min),
        xytext=(worst_min_idx - 2.5, worst_min - 3),
        fontsize=9, color=RED, fontweight='bold',
        arrowprops=dict(arrowstyle='->', color=RED, lw=1.2),
    )

    ax.legend(loc='upper right', frameon=True, facecolor=WHITE,
              edgecolor=LIGHT_GRAY, fontsize=10)

    plt.subplots_adjust(top=0.82)
    path = os.path.join(OUT_DIR, 'night_profile.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved: {path}")


# ── CHART 4: Calendar heatmap ────────────────────────────────

def chart_calendar_heatmap(ym_data, all_years):
    apply_style()

    years = sorted(set(int(y['year']) for y in all_years))
    month_names = ["Jan", "Fev", "Mar", "Avr", "Mai", "Jun",
                   "Jul", "Aou", "Sep", "Oct", "Nov", "Dec"]

    matrix = np.zeros((len(years), 12))
    for row in ym_data:
        y_idx = years.index(row['year'])
        m_idx = row['month'] - 1
        matrix[y_idx][m_idx] = row['pct']

    fig, ax = plt.subplots(figsize=(13, 7))

    colors_list = [WHITE, '#FFF3E0', '#FFD699', AMBER, '#E07B39', RED]
    cmap = LinearSegmentedColormap.from_list('heat_nights', colors_list, N=256)

    im = ax.imshow(matrix, cmap=cmap, aspect='auto', vmin=0, vmax=100)

    ax.set_xticks(range(12))
    ax.set_xticklabels(month_names, fontsize=10)
    ax.set_yticks(range(len(years)))
    ax.set_yticklabels([str(y) for y in years], fontsize=10)

    for i in range(len(years)):
        for j in range(12):
            val = matrix[i][j]
            if val > 0:
                text_color = WHITE if val > 60 else INDIGO
                ax.text(j, i, f'{val:.0f}', ha='center', va='center',
                        fontsize=9, fontweight='bold', color=text_color)

    # Title on the axes directly, subtitle below it
    # Title and subtitle using ax.text — reliable positioning
    ax.text(0.0, 1.12,
            'Six ans de nuits a Nouakchott — la saison rouge revient chaque annee',
            transform=ax.transAxes, fontsize=15, fontweight='bold', color=INDIGO)
    ax.text(0.0, 1.05,
            f'% de nuits ou le minimum ne descend pas sous {THRESHOLD} C — Nouakchott, 2019-2025',
            transform=ax.transAxes, fontsize=10, color=GRAY)

    cbar = fig.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label('% des nuits', fontsize=10, color=GRAY)
    cbar.ax.tick_params(labelsize=9)

    for spine in ax.spines.values():
        spine.set_visible(False)
    ax.tick_params(length=0)

    plt.subplots_adjust(top=0.82)
    path = os.path.join(OUT_DIR, 'calendar_heatmap.png')
    fig.savefig(path)
    plt.close(fig)
    print(f"Saved: {path}")


# ── SITE DATA ────────────────────────────────────────────────

def write_site_data(monthly, yearly, ym_data, runs, nightly_data):
    total_nights = len(nightly_data)
    tropical_nights = sum(1 for row in nightly_data if float(row['min_temp']) > THRESHOLD)
    pct = round(100 * tropical_nights / total_nights, 1) if total_nights > 0 else 0
    worst = max(monthly, key=lambda m: m['pct'])
    best = min(monthly, key=lambda m: m['pct'])
    longest_run = runs[0] if runs else {'length': 0, 'start_date': 'N/A'}
    avg_per_year = tropical_nights // len(yearly) if yearly else 0

    lines = [
        f"# Les Nuits du Sahel — Donnees cles (seuil {THRESHOLD} C)",
        f"",
        f"## Chiffres principaux",
        f"- Nuits analysees : {total_nights}",
        f"- Nuits chaudes (min > {THRESHOLD} C) : {tropical_nights} ({pct}%)",
        f"- Moyenne par an : ~{avg_per_year}",
        f"- Plus longue serie consecutive : {longest_run['length']} nuits (debut {longest_run['start_date']})",
        f"",
        f"## Pire mois : {worst['month_name']} — {worst['pct']}%",
        f"## Meilleur mois : {best['month_name']} — {best['pct']}%",
        f"",
        f"## Par mois",
    ]
    for m in monthly:
        lines.append(f"  {m['month_name']:>3} : {m['pct']:5.1f}% | moy min {m['avg_min']} C")
    lines.append(f"\n## Par annee")
    for y in yearly:
        lines.append(f"  {y['year']} : {y['tropical']} nuits ({y['pct']}%)")
    lines.append(f"\n## Top 5 series consecutives")
    for i, r in enumerate(runs[:5]):
        lines.append(f"  {i+1}. {r['length']} nuits (debut {r['start_date']})")

    path = os.path.join(DATA_DIR, '09_site_summary_28C.md')
    with open(path, 'w') as f:
        f.write('\n'.join(lines))
    print(f"Saved: {path}")

    path2 = os.path.join(DATA_DIR, '10_monthly_28C.csv')
    with open(path2, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['month', 'month_name', 'total', 'tropical', 'pct', 'avg_min'])
        w.writeheader()
        w.writerows(monthly)
    print(f"Saved: {path2}")

    path3 = os.path.join(DATA_DIR, '11_yearly_28C.csv')
    with open(path3, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['year', 'total', 'tropical', 'pct'])
        w.writeheader()
        w.writerows(yearly)
    print(f"Saved: {path3}")

    path4 = os.path.join(DATA_DIR, '12_runs_28C.csv')
    with open(path4, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=['start_date', 'length'])
        w.writeheader()
        w.writerows(runs[:20])
    print(f"Saved: {path4}")


# ── MAIN ─────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"Les Nuits du Sahel — Chart Generation (seuil {THRESHOLD} C)")
    print("=" * 60)
    print()

    print("Reading pipeline data...")
    nightly = read_csv('02_nightly_minimums.csv')
    hourly = read_csv('07_hourly_night_profile.csv')
    print(f"  {len(nightly)} nights loaded")
    print(f"  {len(hourly)} hourly profile rows loaded")
    print()

    print(f"Recomputing at {THRESHOLD} C threshold...")
    monthly, yearly, ym_data = recompute_tropical_at_threshold(nightly, THRESHOLD)
    runs = recompute_consecutive_runs(nightly, THRESHOLD)

    total_trop = sum(m['tropical'] for m in monthly)
    total_nights = sum(m['total'] for m in monthly)
    print(f"  Tropical nights at {THRESHOLD} C: {total_trop} / {total_nights} ({100*total_trop/total_nights:.1f}%)")

    worst = max(monthly, key=lambda m: m['pct'])
    best = min(monthly, key=lambda m: m['pct'])
    print(f"  Worst month: {worst['month_name']} ({worst['pct']}%)")
    print(f"  Best month: {best['month_name']} ({best['pct']}%)")

    if runs:
        print(f"  Longest consecutive run: {runs[0]['length']} nights (start {runs[0]['start_date']})")
    print()

    print("Generating charts...")
    worst_idx, worst_month_data = chart_monthly_tropical(monthly)
    print(f"  Worst month highlighted: {worst_month_data['month_name']} (index {worst_idx})")

    chart_yearly_trend(yearly)
    chart_night_profile(hourly, worst['month'], best['month'])
    chart_calendar_heatmap(ym_data, yearly)
    print()

    print("Writing site data files...")
    write_site_data(monthly, yearly, ym_data, runs, nightly)

    print()
    print("=" * 60)
    print("DONE. Charts in site/assets/charts/")
    print("Site data in nuits_data/")
    print("=" * 60)


if __name__ == '__main__':
    main()