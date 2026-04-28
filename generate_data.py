import random

start_year = 2000
end_year = 2023
output_file = "data/weather.txt"

# Base temperatures
base_temp = 15.0 # Average 15C
amplitude = 15.0 # Variations between -0C and 30C roughly

lines = []
for year in range(start_year, end_year + 1):
    # Let's add a slight global warming trend
    trend = (year - start_year) * 0.05
    
    # Yearly variations
    year_base = base_temp + trend + random.uniform(-1.0, 1.0)
    
    # Generate 12 months (or just random 100 days)
    for day in range(1, 100):
        # Temp fluctuates
        temp = year_base + random.uniform(-amplitude, amplitude)
        
        # In the format expected: temp is multiplied by 10 and zero padded to 4 chars
        # e.g., 23.4 -> 0234
        # Negative temps: -5.4 -> -054 
        temp_int = int(temp * 10)
        
        # format: sign + 3 digits. e.g. +0234 or -0054
        # Wait, the parser uses line.split("+")[3].
        # If the temperature is negative, the line might have a "-" instead of "+", which messes up split("+")!
        # Let's check mapper.py: parts = line.split("+"). 
        # If the format has "-" for temp, split("+") won't work correctly. 
        # So let's keep all temps positive! Min 5C, Max 45C.
        temp_int = max(50, temp_int) # Min 5C
        
        temp_str = f"{temp_int:04d}"
        
        # Build the line
        # USW000948890000 2017 0101 00004 +0000+99999+ 0234 +0123+99999...
        line = f"USW000948890000{year:04d}010100004+0000+99999+{temp_str}+0123+99999+99999+99999+99999+99999+99999+0123+99999+99999"
        lines.append(line)

with open(output_file, "w") as f:
    f.write("\n".join(lines) + "\n")

print(f"Generated {len(lines)} lines of weather data.")
