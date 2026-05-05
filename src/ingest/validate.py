REQUIRED_COLUMNS = {"id", "street", "city", "state", "zip"}
REQUIRED_NONEMPTY = {"id", "street", "city", "state"}


def validate_schema(headers: list[str]) -> None:
    """Raise ValueError upfront if any required columns are absent."""
    present = {h.strip().lower() for h in (headers or [])}
    missing = REQUIRED_COLUMNS - present
    if missing:
        raise ValueError(f"CSV missing required columns: {sorted(missing)}")


def validate_row(row: dict, row_num: int) -> list[str]:
    """Return a list of error strings for this row; empty means valid."""
    errors = []
    for field in REQUIRED_NONEMPTY:
        if not (row.get(field) or "").strip():
            errors.append(f"missing required field: {field!r}")
    return errors
