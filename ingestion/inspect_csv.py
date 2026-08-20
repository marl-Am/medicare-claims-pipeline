"""
ingestion/inspect_csv.py

Reports the shape of one or more CSV files without loading them into memory,
and compares their columns against each other. Use this to confirm a download
is the file you expected, and to check for schema drift before loading a second
year into tables built from the first.

Usage, from the project root with the venv active:

    python -m ingestion.inspect_csv data/2024/some_file.csv
    python -m ingestion.inspect_csv data/2023/a.csv data/2024/b.csv
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb

from ingestion.pipeline import quote_literal

BYTES_PER_GIB: int = 1024**3


@dataclass(frozen=True)
class CsvProfile:
    """A summary of one CSV file's shape."""

    path: Path
    size_bytes: int
    row_count: int
    columns: tuple[str, ...]

    @property
    def size_gib(self) -> float:
        return self.size_bytes / BYTES_PER_GIB


def profile_csv(con: duckdb.DuckDBPyConnection, csv_path: Path) -> CsvProfile:
    """Read the header and count the rows, streaming rather than loading."""
    if not csv_path.exists():
        raise FileNotFoundError(f"No such file: {csv_path}")

    quoted = quote_literal(str(csv_path))

    described = con.execute(
        f"DESCRIBE SELECT * FROM read_csv_auto({quoted})"
    ).fetchall()
    columns = tuple(str(row[0]) for row in described)

    counted = con.execute(f"SELECT COUNT(*) FROM read_csv_auto({quoted})").fetchone()
    if counted is None:
        raise RuntimeError(f"Row count returned nothing for {csv_path}")

    return CsvProfile(
        path=csv_path,
        size_bytes=csv_path.stat().st_size,
        row_count=int(str(counted[0])),
        columns=columns,
    )


def describe(profile: CsvProfile) -> str:
    """Render a single file's profile as readable text."""
    lines = [
        f"File:    {profile.path}",
        f"Size:    {profile.size_gib:.2f} GiB",
        f"Rows:    {profile.row_count:,}",
        f"Columns: {len(profile.columns)}",
        "",
        "Column names:",
    ]
    lines.extend(
        f"  {index:>3}. {name}" for index, name in enumerate(profile.columns, 1)
    )
    return "\n".join(lines)


def compare(first: CsvProfile, second: CsvProfile) -> str:
    """Report how two files' columns differ, if at all."""
    first_set = set(first.columns)
    second_set = set(second.columns)

    lines = [f"Comparing {first.path.name}", f"     with {second.path.name}", ""]

    if first.columns == second.columns:
        lines.append("Columns are identical, in the same order. No drift.")
        return "\n".join(lines)

    if first_set == second_set:
        lines.append("Same column names, different order.")
        lines.append("A SELECT * load would still work, but column positions differ.")
        return "\n".join(lines)

    only_first = sorted(first_set - second_set)
    only_second = sorted(second_set - first_set)

    lines.append("Schema drift found.")
    if only_first:
        lines.append("")
        lines.append(f"Only in {first.path.name}:")
        lines.extend(f"  {name}" for name in only_first)
    if only_second:
        lines.append("")
        lines.append(f"Only in {second.path.name}:")
        lines.extend(f"  {name}" for name in only_second)

    shared = len(first_set & second_set)
    lines.append("")
    lines.append(f"Shared columns: {shared}")
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 1

    con: duckdb.DuckDBPyConnection = duckdb.connect()
    try:
        profiles = [profile_csv(con, Path(argument)) for argument in argv]
    except (FileNotFoundError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    finally:
        con.close()

    for index, profile in enumerate(profiles):
        if index:
            print()
        print(describe(profile))

    for left, right in zip(profiles, profiles[1:]):
        print()
        print("-" * 60)
        print(compare(left, right))

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
