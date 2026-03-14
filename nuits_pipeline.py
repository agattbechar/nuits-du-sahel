"""
Les Nuits du Sahel — Data Pipeline
===================================
Run this locally: python nuits_pipeline.py

Fetches hourly temperature data for Nouakchott (2019-2025) from Open-Meteo,
then produces all analysis CSVs needed to build the site.

Same source as La Taxe Canicule: Open-Meteo archive API
Coordinates: lat 18.0858, lon -15.9785 (Nouakchott)
Timezone: Africa/Nouakchott (UTC+0, no DST)

Output files (all saved to ./nuits_data/):
  01_hourly_raw.csv          — raw hourly temps
  02_nightly_minimums.csv    — min temp per night (18:00 day N → 06:00 day N+1)
  03_tropical_nights.csv     — nights where min > 25°C (WHO definition)
  04_monthly_summary.csv     — tropical nights per month, avg min temp
  05_yearly_summary.csv      — tropical nights per year, trend
  06_consecutive_runs.csv    — longest consecutive tropical night streaks
  07_hourly_night_profile.csv — avg temp by hour (18-06) by month
  08_final_summary.md        — key findings in plain text
"""

import json
import urllib.request
import csv
import os
from datetime import datetime, timedelta
from collections import defaultdict

OUT_DIR = "./nuits_data"
os.makedirs(OUT_DIR, exist_ok=True)

LAT = 18.0858
LON = -15.9785
TZ = "Africa/Nouakchott"

# Tropical night threshold (WHO/WMO standard)
TROPICAL_NIGHT_THRESHOLD = 25.0

# Night window: 18:00 to 06:00 next day
NIGHT_START_HOUR = 18
NIGHT_END_HOUR = 6


def fetch_data():
    """Fetch hourly temperature data from Open-Meteo archive API."""
    all_hours = []
    
    for year in range(2019, 2026):
        start = f"{year}-01-01"
        # For current year, go up to today or Dec 31
        end = f"{year}-12-31"
        
        url = (
            f"https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={LAT}&longitude={LON}"
            f"&start_date={start}&end_date={end}"
            f"&hourly=temperature_2m,relative_humidity_2m,apparent_temperature"
            f"&timezone={TZ}"
        )
        
        print(f"Fetching {year}...")
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=60) as resp:
                data = json.loads(resp.read().decode())
            
            times = data["hourly"]["time"]
            temps = data["hourly"]["temperature_2m"]
            humidity = data["hourly"]["relative_humidity_2m"]
            apparent = data["hourly"]["apparent_temperature"]
            
            for i, t in enumerate(times):
                if temps[i] is not None:
                    all_hours.append({
                        "datetime": t,
                        "temperature": temps[i],
                        "humidity": humidity[i] if humidity[i] is not None else "",
                        "apparent_temperature": apparent[i] if apparent[i] is not None else ""
                    })
            
            print(f"  {year}: {len(times)} hours")
        except Exception as e:
            print(f"  {year}: ERROR - {e}")
            print("  Make sure you have internet access and try again.")
            return None
    
    # Save raw
    path = os.path.join(OUT_DIR, "01_hourly_raw.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["datetime", "temperature", "humidity", "apparent_temperature"])
        w.writeheader()
        w.writerows(all_hours)
    
    print(f"\nTotal hours fetched: {len(all_hours)}")
    print(f"Saved: {path}")
    return all_hours


def compute_nightly_minimums(hours):
    """
    Group hours into nights (18:00 day N → 06:00 day N+1).
    A 'night' is labeled by the date it starts on.
    """
    nights = defaultdict(list)
    
    for h in hours:
        dt = datetime.strptime(h["datetime"], "%Y-%m-%dT%H:%M")
        hour = dt.hour
        
        if hour >= NIGHT_START_HOUR:
            # Evening: this belongs to tonight (labeled by today's date)
            night_date = dt.date()
            nights[night_date].append(h["temperature"])
        elif hour < NIGHT_END_HOUR:
            # Early morning: this belongs to last night (labeled by yesterday's date)
            night_date = (dt - timedelta(days=1)).date()
            nights[night_date].append(h["temperature"])
    
    results = []
    for night_date in sorted(nights.keys()):
        temps = nights[night_date]
        if len(temps) >= 8:  # need at least 8 hours for a valid night
            min_temp = min(temps)
            max_temp = max(temps)
            avg_temp = sum(temps) / len(temps)
            results.append({
                "night_date": night_date.isoformat(),
                "year": night_date.year,
                "month": night_date.month,
                "min_temp": round(min_temp, 1),
                "max_temp": round(max_temp, 1),
                "avg_temp": round(avg_temp, 1),
                "hours_counted": len(temps),
                "is_tropical": 1 if min_temp > TROPICAL_NIGHT_THRESHOLD else 0
            })
    
    path = os.path.join(OUT_DIR, "02_nightly_minimums.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    
    print(f"Nightly minimums: {len(results)} nights")
    print(f"Saved: {path}")
    return results


def compute_tropical_nights(nightly):
    """Extract only tropical nights (min > 25°C)."""
    tropical = [n for n in nightly if n["is_tropical"] == 1]
    
    path = os.path.join(OUT_DIR, "03_tropical_nights.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=tropical[0].keys())
        w.writeheader()
        w.writerows(tropical)
    
    total = len(nightly)
    trop = len(tropical)
    print(f"Tropical nights: {trop} / {total} ({100*trop/total:.1f}%)")
    print(f"Saved: {path}")
    return tropical


def monthly_summary(nightly):
    """Tropical nights and avg min temp by month across all years."""
    months = defaultdict(lambda: {"total_nights": 0, "tropical_nights": 0, "min_temps": []})
    
    # Also by year-month for detailed view
    year_months = defaultdict(lambda: {"total_nights": 0, "tropical_nights": 0, "min_temps": []})
    
    for n in nightly:
        m = n["month"]
        ym = f"{n['year']}-{n['month']:02d}"
        
        months[m]["total_nights"] += 1
        months[m]["min_temps"].append(n["min_temp"])
        if n["is_tropical"]:
            months[m]["tropical_nights"] += 1
        
        year_months[ym]["total_nights"] += 1
        year_months[ym]["min_temps"].append(n["min_temp"])
        if n["is_tropical"]:
            year_months[ym]["tropical_nights"] += 1
    
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    results = []
    for m in range(1, 13):
        d = months[m]
        if d["total_nights"] > 0:
            avg_min = sum(d["min_temps"]) / len(d["min_temps"])
            pct = 100 * d["tropical_nights"] / d["total_nights"]
            results.append({
                "month": m,
                "month_name": month_names[m],
                "total_nights": d["total_nights"],
                "tropical_nights": d["tropical_nights"],
                "tropical_pct": round(pct, 1),
                "avg_min_temp": round(avg_min, 1),
                "min_of_mins": round(min(d["min_temps"]), 1),
                "max_of_mins": round(max(d["min_temps"]), 1)
            })
    
    path = os.path.join(OUT_DIR, "04_monthly_summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    
    # Also save year-month detail
    ym_results = []
    for ym in sorted(year_months.keys()):
        d = year_months[ym]
        avg_min = sum(d["min_temps"]) / len(d["min_temps"])
        pct = 100 * d["tropical_nights"] / d["total_nights"]
        year, month = ym.split("-")
        ym_results.append({
            "year_month": ym,
            "year": int(year),
            "month": int(month),
            "month_name": month_names[int(month)],
            "total_nights": d["total_nights"],
            "tropical_nights": d["tropical_nights"],
            "tropical_pct": round(pct, 1),
            "avg_min_temp": round(avg_min, 1)
        })
    
    path2 = os.path.join(OUT_DIR, "04b_monthly_detail.csv")
    with open(path2, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ym_results[0].keys())
        w.writeheader()
        w.writerows(ym_results)
    
    print(f"Monthly summary: {len(results)} months")
    print(f"Saved: {path} and {path2}")
    return results


def yearly_summary(nightly):
    """Tropical nights per year — the trend line."""
    years = defaultdict(lambda: {"total": 0, "tropical": 0, "min_temps": []})
    
    for n in nightly:
        y = n["year"]
        years[y]["total"] += 1
        years[y]["min_temps"].append(n["min_temp"])
        if n["is_tropical"]:
            years[y]["tropical"] += 1
    
    results = []
    for y in sorted(years.keys()):
        d = years[y]
        avg_min = sum(d["min_temps"]) / len(d["min_temps"])
        pct = 100 * d["tropical"] / d["total"] if d["total"] > 0 else 0
        results.append({
            "year": y,
            "total_nights": d["total"],
            "tropical_nights": d["tropical"],
            "tropical_pct": round(pct, 1),
            "avg_min_temp": round(avg_min, 1)
        })
    
    path = os.path.join(OUT_DIR, "05_yearly_summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    
    print(f"Yearly summary:")
    for r in results:
        print(f"  {r['year']}: {r['tropical_nights']} tropical nights ({r['tropical_pct']}%)")
    print(f"Saved: {path}")
    return results


def consecutive_runs(nightly):
    """Find longest consecutive streaks of tropical nights."""
    runs = []
    current_start = None
    current_length = 0
    
    for n in nightly:
        if n["is_tropical"]:
            if current_start is None:
                current_start = n["night_date"]
                current_length = 1
            else:
                current_length += 1
        else:
            if current_length > 0:
                runs.append({
                    "start_date": current_start,
                    "end_date": n["night_date"],  # first non-tropical night
                    "length": current_length,
                    "year": int(current_start[:4]),
                    "start_month": int(current_start[5:7])
                })
            current_start = None
            current_length = 0
    
    # Don't forget last run
    if current_length > 0:
        runs.append({
            "start_date": current_start,
            "end_date": nightly[-1]["night_date"],
            "length": current_length,
            "year": int(current_start[:4]),
            "start_month": int(current_start[5:7])
        })
    
    # Sort by length descending
    runs.sort(key=lambda x: x["length"], reverse=True)
    
    path = os.path.join(OUT_DIR, "06_consecutive_runs.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["start_date", "end_date", "length", "year", "start_month"])
        w.writeheader()
        w.writerows(runs)
    
    print(f"Consecutive tropical night runs: {len(runs)} streaks")
    if runs:
        top = runs[0]
        print(f"  Longest: {top['length']} nights starting {top['start_date']}")
        if len(runs) > 1:
            print(f"  2nd longest: {runs[1]['length']} nights starting {runs[1]['start_date']}")
    print(f"Saved: {path}")
    return runs


def hourly_night_profile(hours):
    """Average temperature by hour (18-06) by month — the shape of the night."""
    profiles = defaultdict(lambda: defaultdict(list))
    
    for h in hours:
        dt = datetime.strptime(h["datetime"], "%Y-%m-%dT%H:%M")
        hour = dt.hour
        
        if hour >= NIGHT_START_HOUR or hour < NIGHT_END_HOUR:
            month = dt.month
            # For early morning hours, the "night month" is the previous day's month
            if hour < NIGHT_END_HOUR:
                prev = dt - timedelta(days=1)
                month = prev.month
            profiles[month][hour].append(h["temperature"])
    
    month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    
    results = []
    for month in range(1, 13):
        for hour in [18, 19, 20, 21, 22, 23, 0, 1, 2, 3, 4, 5]:
            temps = profiles[month].get(hour, [])
            if temps:
                results.append({
                    "month": month,
                    "month_name": month_names[month],
                    "hour": hour,
                    "hour_label": f"{hour:02d}:00",
                    "avg_temp": round(sum(temps) / len(temps), 1),
                    "min_temp": round(min(temps), 1),
                    "max_temp": round(max(temps), 1),
                    "above_25_pct": round(100 * sum(1 for t in temps if t > 25) / len(temps), 1)
                })
    
    path = os.path.join(OUT_DIR, "07_hourly_night_profile.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=results[0].keys())
        w.writeheader()
        w.writerows(results)
    
    print(f"Hourly night profiles: {len(results)} rows")
    print(f"Saved: {path}")
    return results


def write_summary(nightly, monthly, yearly, runs):
    """Write final summary with key findings."""
    total = len(nightly)
    tropical = sum(1 for n in nightly if n["is_tropical"])
    pct = 100 * tropical / total if total > 0 else 0
    
    # Worst month
    worst = max(monthly, key=lambda m: m["tropical_pct"])
    best = min(monthly, key=lambda m: m["tropical_pct"])
    
    # Longest run
    longest = runs[0] if runs else None
    
    # Year trend
    first_year = yearly[0] if yearly else None
    last_full_year = yearly[-2] if len(yearly) > 1 else yearly[-1] if yearly else None
    
    lines = [
        "# Les Nuits du Sahel — Key Findings",
        f"## Data: Nouakchott, {yearly[0]['year']}–{yearly[-1]['year']}" if yearly else "",
        f"## Source: Open-Meteo (ERA5), lat {LAT} lon {LON}",
        "",
        "## Core Numbers",
        f"- Total nights analyzed: {total}",
        f"- Tropical nights (min > 25°C): {tropical} ({pct:.1f}%)",
        f"- Average: ~{tropical // len(yearly) if yearly else 0} tropical nights per year",
        "",
        "## Monthly Pattern",
        f"- Worst month: {worst['month_name']} — {worst['tropical_pct']}% of nights are tropical (avg min {worst['avg_min_temp']}°C)",
        f"- Best month: {best['month_name']} — {best['tropical_pct']}% tropical (avg min {best['avg_min_temp']}°C)",
        "",
        "## Monthly Breakdown",
    ]
    
    for m in monthly:
        lines.append(f"  {m['month_name']:>3}: {m['tropical_pct']:5.1f}% tropical | avg min {m['avg_min_temp']}°C")
    
    lines.extend([
        "",
        "## Consecutive Runs",
    ])
    
    if longest:
        lines.append(f"- Longest streak: {longest['length']} consecutive tropical nights (started {longest['start_date']})")
    if len(runs) > 1:
        lines.append(f"- 2nd longest: {runs[1]['length']} nights (started {runs[1]['start_date']})")
    if len(runs) > 2:
        lines.append(f"- 3rd longest: {runs[2]['length']} nights (started {runs[2]['start_date']})")
    
    lines.extend([
        "",
        "## Year-over-Year",
    ])
    for y in yearly:
        lines.append(f"  {y['year']}: {y['tropical_nights']} tropical nights ({y['tropical_pct']}%)")
    
    if first_year and last_full_year and first_year != last_full_year:
        change = last_full_year["tropical_nights"] - first_year["tropical_nights"]
        lines.append(f"\n  Change {first_year['year']}→{last_full_year['year']}: {change:+d} tropical nights")
    
    lines.extend([
        "",
        "## Methodology",
        "- Tropical night: any night where minimum temperature between 18:00–06:00 stays above 25°C",
        "- WHO/WMO standard definition",
        "- Source: Open-Meteo ERA5 reanalysis, hourly resolution",
        f"- Coordinates: {LAT}°N, {LON}°W (Nouakchott city center)",
        "- Period: 2019–2025",
        "- No modelling. No interpolation. Just counting.",
    ])
    
    text = "\n".join(lines)
    path = os.path.join(OUT_DIR, "08_final_summary.md")
    with open(path, "w") as f:
        f.write(text)
    
    print(f"\n{'='*60}")
    print(text)
    print(f"{'='*60}")
    print(f"\nSaved: {path}")


def main():
    print("=" * 60)
    print("Les Nuits du Sahel — Data Pipeline")
    print("Nouakchott Tropical Night Analysis")
    print("=" * 60)
    print()
    
    # Step 1: Fetch
    hours = fetch_data()
    if hours is None:
        print("\nFetch failed. Check your internet connection and try again.")
        return
    
    print()
    
    # Step 2: Nightly minimums
    nightly = compute_nightly_minimums(hours)
    print()
    
    # Step 3: Tropical nights
    tropical = compute_tropical_nights(nightly)
    print()
    
    # Step 4: Monthly summary
    monthly = monthly_summary(nightly)
    print()
    
    # Step 5: Yearly summary
    yearly = yearly_summary(nightly)
    print()
    
    # Step 6: Consecutive runs
    runs = consecutive_runs(nightly)
    print()
    
    # Step 7: Hourly night profiles
    profiles = hourly_night_profile(hours)
    print()
    
    # Step 8: Final summary
    write_summary(nightly, monthly, yearly, runs)
    
    print("\n" + "=" * 60)
    print("DONE. All files saved to ./nuits_data/")
    print("Upload the entire nuits_data/ folder to continue building the site.")
    print("=" * 60)


if __name__ == "__main__":
    main()