#!/usr/bin/env python3
import sys


def parse_line(line):
    line = line.strip()
    if len(line) < 19:
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


if __name__ == "__main__":
    for line in sys.stdin:
        result = parse_line(line)
        if result is not None:
            year, temp = result
            print(f"{year}	{temp}")
