import sqlite3
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def get_connection(db_path: str | Path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def apply_migrations(db_path: str | Path, migrations_dir: str | Path) -> None:
    migrations_dir = Path(migrations_dir)
    scripts = sorted(migrations_dir.glob("*.sql"))

    with get_connection(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )
        applied = {row["version"] for row in conn.execute("SELECT version FROM schema_migrations")}

        for script in scripts:
            version = int(script.stem.split("_")[0])
            if version in applied:
                continue
            sql = script.read_text()
            conn.executescript(sql)
            conn.execute(
                "INSERT OR IGNORE INTO schema_migrations (version, applied_at) VALUES (?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))",
                (version,),
            )
