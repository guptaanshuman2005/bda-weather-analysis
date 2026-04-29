import random
import os

output_file = "data/weather.txt"

# Base temperatures
base_temp = 15.0 # Average 15C
amplitude = 15.0 # Variations between -0C and 30C roughly

# We need about 12 million lines to safely exceed 1 GB. (Each line is ~113 bytes)
TARGET_LINES = 12_000_000
CHUNK_SIZE = 1_000_000

print(f"Generating {TARGET_LINES} lines of weather data (approx 1.1 GB)...")

# Ensure directory exists
os.makedirs("data", exist_ok=True)

# Pre-calculate a specific mean for each year to make the graph jagged and realistic!
# If we calculate variance per point, it averages out to a flat line due to the law of large numbers.
year_profiles = {}
year_min_floors = {}
for y in range(2000, 2024):
    trend = (y - 2000) * 0.08  # Global warming trend
    # Add significant random variance PER YEAR, so some years are noticeably hotter or colder
    annual_variance = random.uniform(-2.5, 2.5)
    
    # Let's add some extreme outlier years!
    if y == 2003: annual_variance += 2.0  # Heatwave
    if y == 2011: annual_variance -= 2.0  # Cold snap
    
    year_profiles[y] = base_temp + trend + annual_variance
    # Assign a random minimum temperature floor for this year between 2.0C and 8.5C
    year_min_floors[y] = random.randint(20, 85)

with open(output_file, "w") as f:
    for chunk in range(TARGET_LINES // CHUNK_SIZE):
        lines = []
        for _ in range(CHUNK_SIZE):
            year = random.randint(2000, 2023)
            
            # Get the fixed base mean for THIS specific year
            year_base = year_profiles[year]
            
            # Daily fluctuation
            temp = year_base + random.uniform(-amplitude, amplitude)
            temp_int = int(temp * 10)
            
            # Keep it positive to match the + splitting logic expected
            # We use the randomly assigned floor for this specific year
            temp_int = max(year_min_floors[year], temp_int) 
            temp_str = f"{temp_int:04d}"
            
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            
            # Build the line
            line = f"USW000948890000{year:04d}{month:02d}{day:02d}00004+0000+99999+{temp_str}+0123+99999+99999+99999+99999+99999+99999+0123+99999+99999"
            lines.append(line)
            
        f.write("\n".join(lines) + "\n")
        print(f"Written chunk {chunk + 1}/{(TARGET_LINES // CHUNK_SIZE)}")

print(f"Finished generating data. File size: {os.path.getsize(output_file) / (1024*1024*1024):.2f} GB")
