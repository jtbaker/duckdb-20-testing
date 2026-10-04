from __future__ import annotations

import csv
import json
import re
import statistics
from collections import defaultdict
from pathlib import Path

RESULTS_DIR = Path("benchmark-results")
SIZE_PATTERN = re.compile(r"^([0-9.]+)([KMGTP]?i?B)$")
UNIT_MULTIPLIERS = {
    "B": 1,
    "kB": 1000,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KiB": 1024,
    "MiB": 1024**2,
    "GiB": 1024**3,
    "TiB": 1024**4,
}


def parse_size(value: str) -> int:
    match = SIZE_PATTERN.match(value.strip())
    if not match:
        raise ValueError(f"Unsupported size: {value}")
    return round(float(match.group(1)) * UNIT_MULTIPLIERS[match.group(2)])


def main() -> None:
    samples: dict[tuple[str, str], list[dict[str, float]]] = defaultdict(list)
    for path in sorted(RESULTS_DIR.glob("container-stats-*.csv")):
        phase = path.stem.removeprefix("container-stats-")
        with path.open(newline="") as source:
            for row in csv.DictReader(source):
                memory_used = row["memory_usage"].split("/")[0].strip()
                samples[(phase, row["container"])].append(
                    {
                        "cpu_percent": float(row["cpu_percent"].rstrip("%")),
                        "memory_bytes": parse_size(memory_used),
                        "memory_percent": float(row["memory_percent"].rstrip("%")),
                    }
                )

    report: dict[str, dict[str, dict[str, float | int]]] = defaultdict(dict)
    for (phase, container), values in samples.items():
        cpu = [value["cpu_percent"] for value in values]
        memory = [value["memory_bytes"] for value in values]
        report[phase][container] = {
            "samples": len(values),
            "mean_cpu_percent": round(statistics.mean(cpu), 3),
            "max_cpu_percent": round(max(cpu), 3),
            "mean_memory_mib": round(statistics.mean(memory) / 1024**2, 3),
            "peak_memory_mib": round(max(memory) / 1024**2, 3),
        }

    output = RESULTS_DIR / "resource-summary.json"
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
