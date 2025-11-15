from pathlib import Path
import sys

from fastapi.testclient import TestClient
from sqlalchemy import text

# Añadir raíz del proyecto al sys.path
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.etl.run_etl import run_etl
from app.api.dependencies import get_engine
from app.api.main import app

DB_PATH = ROOT_DIR / "app" / "db" / "library.db"
if not DB_PATH.exists():
    run_etl()

engine = get_engine()
client = TestClient(app)


def _get_any_book_id():
    with engine.connect() as conn:
        row = conn.execute(text("SELECT book_id FROM BOOK LIMIT 1")).first()
    assert row is not None, "No hay libros en la tabla BOOK"
    return row.book_id


def _get_user_id_with_ratings(min_count: int = 1):
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT user_id, COUNT(*) AS c
                FROM RATING
                GROUP BY user_id
                HAVING COUNT(*) >= :minc
                LIMIT 1
                """
            ),
            {"minc": min_count},
        ).first()
    assert row is not None, "No se encontró usuario con suficientes ratings"
    return row.user_id


def _get_any_user_and_copy():
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT user_id, copy_id
                FROM RATING
                LIMIT 1
                """
            )
        ).first()
    assert row is not None, "No hay filas en RATING"
    return row.user_id, row.copy_id


def test_get_books_endpoint():
    resp = client.get("/books?limit=5")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    assert len(data) <= 5


def test_get_book_detail_endpoint():
    book_id = _get_any_book_id()
    resp = client.get(f"/books/{book_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["book_id"] == book_id


def test_user_recommendations_endpoint():
    user_id = _get_user_id_with_ratings()
    resp = client.get(f"/users/{user_id}/recommendations?n=5&min_ratings=1")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


def test_post_rating_endpoint():
    user_id, copy_id = _get_any_user_and_copy()
    payload = {"user_id": user_id, "copy_id": copy_id, "rating": 4}

    resp = client.post("/ratings", json=payload)
    assert resp.status_code in (200, 201)

    data = resp.json()
    assert data["user_id"] == user_id
    assert data["copy_id"] == copy_id
    assert data["rating"] == 4
