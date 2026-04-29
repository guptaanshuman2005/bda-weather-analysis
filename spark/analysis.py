import os
import sys
sys.setrecursionlimit(10000)
os.environ["PYSPARK_PYTHON"] = r"C:\Python314\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Python314\python.exe"
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INPUT_PATH = os.path.join(BASE_DIR, "data", "weather.txt")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

os.makedirs(OUTPUT_DIR, exist_ok=True)

spark = SparkSession.builder \
    .appName("Weather Analysis") \
    .master("local[*]") \
    .getOrCreate()

rdd = spark.sparkContext.textFile(INPUT_PATH)


def parse(line):
    if not line or len(line) < 19:
        return None

    try:
        year = line[15:19]
        parts = line.split("+")
        temp = int(parts[3])
        if temp == 9999:
            return None
        return year, temp
    except (ValueError, IndexError):
        return None


def merge_value(accumulator, value):
    sum_value, count, min_value, max_value = accumulator
    return sum_value + value, count + 1, min(min_value, value), max(max_value, value)


def merge_combiners(acc1, acc2):
    sum_a, count_a, min_a, max_a = acc1
    sum_b, count_b, min_b, max_b = acc2
    return sum_a + sum_b, count_a + count_b, min(min_a, min_b), max(max_a, max_b)


data = rdd.map(parse).filter(lambda x: x is not None)

stats_by_year = data.aggregateByKey(
    (0, 0, 99999, -99999),
    merge_value,
    merge_combiners,
)

scaled_stats = stats_by_year.mapValues(
    lambda values: (
        values[0] / values[1] / 10.0,
        values[2] / 10.0,
        values[3] / 10.0,
    )
)

results = sorted(scaled_stats.collect())

spark_stats_path = os.path.join(OUTPUT_DIR, "spark_stats.txt")
with open(spark_stats_path, "w") as f:
    f.write("year\tavg_temp\tmin_temp\tmax_temp\n")
    for year, (avg_temp, min_temp, max_temp) in results:
        f.write(f"{year}\t{avg_temp:.1f}\t{min_temp:.1f}\t{max_temp:.1f}\n")

threshold_hot = 30.0
threshold_cold = 10.0

classification = [
    (year, "HOT" if avg_temp > threshold_hot else "COLD" if avg_temp < threshold_cold else "NORMAL")
    for year, (avg_temp, _, _) in results
]

year_type_path = os.path.join(OUTPUT_DIR, "year_type.txt")
with open(year_type_path, "w") as f:
    f.write("year\tclassification\n")
    for year, label in classification:
        f.write(f"{year}\t{label}\n")

print(f"Saved year statistics to {spark_stats_path}")
print(f"Saved year classification to {year_type_path}")

spark.stop()
