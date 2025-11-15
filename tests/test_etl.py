from pathlib import Path
import sys
import sqlite3

# Asegurarnos de que la raíz del proyecto (PD_Grupo_1) está en sys.path
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.etl.run_etl import run_etl

BASE_DIR = ROOT_DIR
DB_PATH = BASE_DIR / "app" / "db" / "library.db"
