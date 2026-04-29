import sys
import os
import time

base_dir = os.path.dirname(os.path.abspath(__file__))
input_file = os.path.join(base_dir, "data", "weather.txt")
output_dir = os.path.join(base_dir, "output")
output_file = os.path.join(output_dir, "hadoop_results.txt")

os.makedirs(output_dir, exist_ok=True)

print(f"Running MapReduce locally in Python on {input_file} (1.1 GB)...")
start_time = time.time()

# We act as both Mapper and Reducer simultaneously to save memory.
from hadoop.mapper import parse_line

year_stats = {}
lines_processed = 0

with open(input_file, "r") as f:
    for line in f:
        lines_processed += 1
        res = parse_line(line)
        if res is not None:
            year, temp = res
            if year not in year_stats:
                year_stats[year] = {"sum": temp, "count": 1, "min": temp, "max": temp}
            else:
                stats = year_stats[year]
                stats["sum"] += temp
                stats["count"] += 1
                if temp < stats["min"]: stats["min"] = temp
                if temp > stats["max"]: stats["max"] = temp
        
        if lines_processed % 2000000 == 0:
            print(f"Processed {lines_processed} lines...")

print(f"Map/Reduce phase completed in {time.time() - start_time:.2f} seconds.")

# Write output matching the Hadoop reducer output
with open(output_file, "w") as out:
    for year in sorted(year_stats.keys()):
        stats = year_stats[year]
        avg = stats["sum"] / stats["count"]
        # Format from reducer.py: print(f"{year}\t{avg / 10:.1f}\t{stats['min'] / 10:.1f}\t{stats['max'] / 10:.1f}")
        out.write(f"{year}\t{avg / 10:.1f}\t{stats['min'] / 10:.1f}\t{stats['max'] / 10:.1f}\n")

print(f"Output written to {output_file}")

