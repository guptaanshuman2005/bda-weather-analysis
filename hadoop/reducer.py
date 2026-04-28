#!/usr/bin/env python3
import sys


def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return None


year_stats = {}

for line in sys.stdin:
    try:
        year, temp_raw = line.strip().split()
        temp = safe_int(temp_raw)
        if temp is None:
            continue

        if year not in year_stats:
            year_stats[year] = {
                "sum": temp,
                "count": 1,
                "min": temp,
                "max": temp,
            }
        else:
            stats = year_stats[year]
            stats["sum"] += temp
            stats["count"] += 1
            stats["min"] = min(stats["min"], temp)
            stats["max"] = max(stats["max"], temp)
    except ValueError:
        continue

for year in sorted(year_stats.keys()):
    stats = year_stats[year]
    avg = stats["sum"] / stats["count"]
    print(f"{year}	{avg / 10:.1f}	{stats['min'] / 10:.1f}	{stats['max'] / 10:.1f}")
