from pathlib import Path
import sys
import sqlite3

# Añadir raíz del proyecto al sys.path
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.etl.run_etl import run_etl

DB_PATH = ROOT_DIR / "app" / "db" / "library.db"

# Si la BD no existe, ejecutamos el ETL una vez
if not DB_PATH.exists():
    run_etl()


def test_tables_exist():
    """Las tablas principales deben existir en la BD."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    for table in ["USER", "BOOK", "COPY", "RATING"]:
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table,),
        )
        row = cur.fetchone()
        assert row is not None, f"La tabla {table} no existe"

    conn.close()
