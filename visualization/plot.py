import os
import matplotlib.pyplot as plt

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STATS_PATH = os.path.join(BASE_DIR, "output", "spark_stats.txt")
OUTPUT_IMAGE = os.path.join(BASE_DIR, "output", "temperature_plot.png")


def load_statistics(path):
    years = []
    avg_temps = []
    min_temps = []
    max_temps = []

    with open(path, "r") as f:
        f.readline()
        for line in f:
            parts = line.strip().split("	")
            if len(parts) < 4:
                continue
            years.append(int(parts[0]))
            avg_temps.append(float(parts[1]))
            min_temps.append(float(parts[2]))
            max_temps.append(float(parts[3]))

    return years, avg_temps, min_temps, max_temps


if not os.path.exists(STATS_PATH):
    raise FileNotFoundError(
        f"Statistics file not found. Run spark/analysis.py first to generate {STATS_PATH}"
    )

years, avg_temps, min_temps, max_temps = load_statistics(STATS_PATH)

plt.figure(figsize=(10, 6))
plt.plot(years, avg_temps, marker="o", linestyle="-", color="tab:blue", label="Average Temperature")
plt.fill_between(years, min_temps, max_temps, color="tab:cyan", alpha=0.2, label="Min/Max Range")

plt.xlabel("Year")
plt.ylabel("Temperature (°C)")
plt.title("Yearly Average Temperature with Min/Max Range")
plt.xticks(years)
plt.grid(True, linestyle="--", alpha=0.5)
plt.legend()

os.makedirs(os.path.dirname(OUTPUT_IMAGE), exist_ok=True)
plt.savefig(OUTPUT_IMAGE)
print(f"Saved temperature plot to {OUTPUT_IMAGE}")
plt.show()
