from typing import Optional
import pandas as pd

from app.recommender.popularity import get_engine, _base_book_stats


def get_recommendations_for_user(
    user_id: int,
    n: int = 10,
    min_ratings: int = 20,
) -> pd.DataFrame:
    """
    Recomendaciones para un usuario concreto.

    Versión inicial (baseline):
    - Calcula popularidad global de libros.
    - Excluye los libros que el usuario ya ha valorado.
    - Devuelve los N libros más recomendados.

    Más adelante se puede sustituir la parte de popularidad global
    por un modelo colaborativo user-based o item-based.
    """
    engine = get_engine()

    # Libros que el usuario YA ha valorado
    query_rated = """
    SELECT DISTINCT c.book_id
    FROM RATING r
    JOIN COPY c ON r.copy_id = c.copy_id
    WHERE r.user_id = :user_id
    """
    rated = pd.read_sql(query_rated, engine, params={"user_id": user_id})
    already_read = set(rated["book_id"].tolist())

    # Popularidad global
    stats = _base_book_stats(engine)
    stats = stats[stats["num_ratings"] >= min_ratings]

    # Añadimos score (mismo criterio que en popularity.py)
    stats = stats.copy()
    import numpy as np
    stats["score"] = stats["mean_rating"] * np.log1p(stats["num_ratings"])

    # Excluimos libros ya leídos por el usuario
    if already_read:
        stats = stats[~stats["book_id"].isin(already_read)]

    # Devolvemos top N
    stats = stats.sort_values("score", ascending=False)
    return stats.head(n).reset_index(drop=True)
