from pathlib import Path
import sys

import pytest
from sqlalchemy import text

# Añadir raíz del proyecto al sys.path
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.etl.run_etl import run_etl
from app.api.dependencies import get_engine
from app.recommender.collaborative import get_recommendations_for_user

DB_PATH = ROOT_DIR / "app" / "db" / "library.db"
if not DB_PATH.exists():
    run_etl()

engine = get_engine()


def _get_user_with_enough_ratings(min_count: int = 3) -> int:
    """Devuelve un user_id con al menos min_count ratings."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT user_id, COUNT(*) AS c
                FROM RATING
                GROUP BY user_id
                HAVING COUNT(*) >= :min_count
                LIMIT 1
                """
            ),
            {"min_count": min_count},
        ).first()

    assert row is not None, "No se encontró ningún usuario con suficientes ratings"
    return row.user_id


def test_recommendations_do_not_include_already_rated_books():
    """Las recomendaciones no deben incluir libros ya valorados por el usuario."""
    user_id = _get_user_with_enough_ratings()

    # Libros ya valorados por el usuario
    with engine.connect() as conn:
        rated_books = conn.execute(
            text(
                """
                SELECT DISTINCT c.book_id
                FROM RATING r
                JOIN COPY c ON r.copy_id = c.copy_id
                WHERE r.user_id = :uid
                """
            ),
            {"uid": user_id},
        ).scalars().all()

    rated_set = set(rated_books)

    recs = get_recommendations_for_user(user_id=user_id, n=20, min_ratings=1)

    if recs.empty:
        pytest.skip("El recomendador no devolvió resultados para este usuario")

    rec_books = set(recs["book_id"].tolist())

    assert rated_set.isdisjoint(rec_books), (
        "Las recomendaciones incluyen libros ya valorados por el usuario"
    )
