import csv
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .normalize import address_hash, normalize_row
from .validate import validate_row, validate_schema


@dataclass
class IngestResult:
    source_file: str
    total_rows: int = 0
    loaded: int = 0
    rejected: int = 0
    duplicates_skipped: int = 0
    errors: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        lines = [
            f"Source:     {self.source_file}",
            f"Total rows: {self.total_rows}",
            f"Loaded:     {self.loaded}",
            f"Rejected:   {self.rejected}",
            f"Duplicates: {self.duplicates_skipped}",
        ]
        if self.errors:
            lines.append("Errors:")
            lines.extend(f"  {e}" for e in self.errors)
        return "\n".join(lines)


def load_csv(
    csv_path: str | Path,
    conn: sqlite3.Connection,
    retention_days: int = 90,
    source_file: str | None = None,
) -> IngestResult:
    """
    Validate, normalize, hash, and load addresses from a CSV into raw_addresses.

    Rejects rows with missing required fields (id, street, city, state) with a
    specific error message. Missing zip is allowed. Duplicate hashes are skipped
    and counted. Returns an IngestResult summary; never raises on row-level errors.

    Raises ValueError if the CSV schema is invalid (checked before any rows are read).
    """
    csv_path = Path(csv_path)
    source_file = source_file or csv_path.name

    now = datetime.now(timezone.utc)
    uploaded_at = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    purge_after = (now + timedelta(days=retention_days)).strftime("%Y-%m-%dT%H:%M:%SZ")

    result = IngestResult(source_file=source_file)

    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        validate_schema(reader.fieldnames or [])  # fail fast before reading any rows

        for row_num, row in enumerate(reader, start=2):
            result.total_rows += 1

            errors = validate_row(row, row_num)
            if errors:
                result.rejected += 1
                row_id = (row.get("id") or "").strip()
                for msg in errors:
                    result.errors.append(f"Row {row_num} (id={row_id!r}): {msg}")
                continue

            normalized = normalize_row(row)
            h = address_hash(normalized)

            try:
                conn.execute(
                    """
                    INSERT INTO raw_addresses
                        (id, address_hash, street, city, state, zip,
                         source_file, uploaded_at, retention_purge_after)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized["id"],
                        h,
                        normalized["street"],
                        normalized["city"],
                        normalized["state"],
                        normalized["zip"] or None,
                        source_file,
                        uploaded_at,
                        purge_after,
                    ),
                )
                result.loaded += 1
            except sqlite3.IntegrityError as exc:
                if "UNIQUE constraint" in str(exc):
                    result.duplicates_skipped += 1
                else:
                    result.rejected += 1
                    result.errors.append(
                        f"Row {row_num} (id={normalized['id']!r}): DB error: {exc}"
                    )

    return result
