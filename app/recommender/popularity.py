from pathlib import Path
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

# Ruta a la base de datos SQLite generada por el ETL
BASE_DIR = Path(__file__).resolve().parents[2]
DB_PATH = BASE_DIR / "app" / "db" / "library.db"


def get_engine():
    """Crea una conexión SQLAlchemy a la base de datos SQLite."""
    return create_engine(f"sqlite:///{DB_PATH}")


def _base_book_stats(engine=None) -> pd.DataFrame:
    """
    Calcula estadísticas básicas de popularidad por libro:
    - num_ratings
    - mean_rating

    Devuelve un DataFrame con columnas:
    [book_id, title, authors, language_code, num_ratings, mean_rating]
    """
    if engine is None:
        engine = get_engine()

    query = """
    SELECT
        b.book_id,
        b.title,
        b.authors,
        b.language_code,
        COUNT(r.rating) AS num_ratings,
        AVG(r.rating)   AS mean_rating
    FROM BOOK b
    JOIN COPY c   ON c.book_id = b.book_id
    JOIN RATING r ON r.copy_id = c.copy_id
    GROUP BY b.book_id, b.title, b.authors, b.language_code
    """
    df = pd.read_sql(query, engine)
    return df


def _apply_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Añade una columna 'score' que combina media y nº de ratings:
    score = mean_rating * log(1 + num_ratings)
    """
    df = df.copy()
    df["score"] = df["mean_rating"] * np.log1p(df["num_ratings"])
    return df


def get_top_books_global(n: int = 10, min_ratings: int = 50) -> pd.DataFrame:
    """
    Devuelve el top N de libros más populares a nivel global.

    - Filtra libros con al menos `min_ratings` valoraciones.
    - Ordena por 'score' (media ponderada por nº de ratings).
    """
    engine = get_engine()
    df = _base_book_stats(engine)
    df = df[df["num_ratings"] >= min_ratings]
    df = _apply_score(df)
    df = df.sort_values("score", ascending=False)
    return df.head(n).reset_index(drop=True)


def get_top_books_by_genre(genre: str, n: int = 10, min_ratings: int = 20) -> pd.DataFrame:
    """
    Versión simplificada: usamos language_code como 'genre'.

    En el dataset no tenemos géneros explícitos, así que por ahora
    interpretamos 'genre' como el código de idioma (language_code),
    por ejemplo: 'eng', 'spa', etc.
    """
    engine = get_engine()
    df = _base_book_stats(engine)

    # Aquí genre == language_code (ej.: 'eng')
    df = df[df["language_code"] == genre]

    df = df[df["num_ratings"] >= min_ratings]
    df = _apply_score(df)
    df = df.sort_values("score", ascending=False)
    return df.head(n).reset_index(drop=True)


def get_top_books_for_age_range(
    age_min: int,
    age_max: int,
    n: int = 10,
    min_ratings: int = 5,
    reference_year: int = 2025,
) -> pd.DataFrame:
    """
    Devuelve libros populares entre usuarios cuya edad está en [age_min, age_max].

    Se usa fecha_nacimiento de la tabla USER (solo la tienen ~500 usuarios),
    así que el resultado se basa en ese subconjunto.
    """
    engine = get_engine()
    query = """
    SELECT
        b.book_id,
        b.title,
        b.authors,
        b.language_code,
        r.rating,
        u.fecha_nacimiento
    FROM RATING r
    JOIN COPY c ON r.copy_id = c.copy_id
    JOIN BOOK b ON c.book_id = b.book_id
    JOIN USER u ON u.user_id = r.user_id
    WHERE u.fecha_nacimiento IS NOT NULL
    """
    df = pd.read_sql(query, engine)

    # Pasar fecha_nacimiento a datetime y calcular edad aproximada
    df["fecha_nacimiento"] = pd.to_datetime(df["fecha_nacimiento"], errors="coerce")
    df = df.dropna(subset=["fecha_nacimiento"])

    df["edad"] = reference_year - df["fecha_nacimiento"].dt.year
    df = df[(df["edad"] >= age_min) & (df["edad"] <= age_max)]

    if df.empty:
        return df  # No hay datos para ese rango de edad

    # Agregar por libro dentro de ese rango de edad
    agg = (
        df.groupby(["book_id", "title", "authors", "language_code"])["rating"]
        .agg(num_ratings="count", mean_rating="mean")
        .reset_index()
    )

    agg = agg[agg["num_ratings"] >= min_ratings]
    if agg.empty:
        return agg

    agg = _apply_score(agg)
    agg = agg.sort_values("score", ascending=False)
    return agg.head(n).reset_index(drop=True)
