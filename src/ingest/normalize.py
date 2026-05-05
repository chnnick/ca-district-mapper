import hashlib
import re


def normalize_row(row: dict) -> dict:
    """
    Return a new dict with whitespace collapsed, fields uppercased.
    zip may be empty string if absent or blank — that's valid.
    """
    def clean(val: str | None) -> str:
        return re.sub(r"\s+", " ", (val or "").strip()).upper()

    return {
        "id":     row["id"].strip(),
        "street": clean(row["street"]),
        "city":   clean(row["city"]),
        "state":  clean(row["state"]),
        "zip":    clean(row.get("zip") or ""),
    }


def address_hash(normalized: dict) -> str:
    """SHA-256 of a canonical pipe-delimited address string."""
    canonical = "{street}|{city}|{state}|{zip}".format(**normalized)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
