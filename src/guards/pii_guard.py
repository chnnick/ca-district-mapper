from pathlib import Path

# Column names that indicate raw PII. If any of these appear in a CSV being
# written to a protected output directory, the write is refused.
_PII_COLUMNS = frozenset({
    "street", "address", "address1", "address2", "address_line_1", "address_line_2",
    "name", "first_name", "last_name", "full_name",
    "email", "phone", "phone_number",
})

# Directories under the project root that must never contain raw address data.
_PROTECTED_DIRS = frozenset({"reports", "docs", "logs"})


def check_csv_columns(columns: list[str], output_path: Path) -> None:
    """
    Raise ValueError if any PII column names are being written to a protected
    output directory (reports/, docs/, logs/).

    Call this before writing any CSV file that could potentially contain address
    data, even if you believe the data is clean — the guard is a last-resort check.
    """
    parts = {p.lower() for p in output_path.parts}
    if not (parts & _PROTECTED_DIRS):
        return

    pii_found = _PII_COLUMNS & {c.strip().lower() for c in columns}
    if pii_found:
        raise ValueError(
            f"Refusing to write PII columns {sorted(pii_found)} to protected "
            f"path {output_path}. Reports must contain only aggregate data."
        )
