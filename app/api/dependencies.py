from pathlib import Path
from typing import Optional  # <-- AÑADIDO
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# Raíz del proyecto y ruta a la BD SQLite
BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "app" / "db" / "library.db"

_engine: Optional[Engine] = None   # <-- sustituye Engine | None por Optional[Engine]


def get_engine() -> Engine:
    """
    Devuelve un engine de SQLAlchemy reutilizable.
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            f"sqlite:///{DB_PATH}",
            connect_args={"check_same_thread": False},  # necesario para FastAPI + SQLite
        )
    return _engine
