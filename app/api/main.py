from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import text

from app.api.dependencies import get_engine
from app.recommender.collaborative import get_recommendations_for_user

app = FastAPI(
    title="Book Recommender API",
    description="API para catálogo y recomendaciones de la biblioteca",
    version="0.1.0",
)

engine = get_engine()


# ---------- MODELOS Pydantic ----------

class BookOut(BaseModel):
    book_id: int
    title: str
    authors: Optional[str] = None
    language_code: Optional[str] = None
    original_publication_year: Optional[int] = None


class RecommendationOut(BaseModel):
    book_id: int
    title: str
    authors: Optional[str] = None
    language_code: Optional[str] = None
    num_ratings: int
    mean_rating: float
    score: float


class RatingIn(BaseModel):
    user_id: int
    copy_id: int
    # Usamos Field con restricciones, en vez de conint
    rating: int = Field(..., ge=1, le=5)  # entero 1–5


class RatingOut(BaseModel):
    user_id: int
    copy_id: int
    rating: int


# ---------- ENDPOINTS ----------

@app.get("/books", response_model=List[BookOut])
def list_books(
    q: Optional[str] = Query(None, description="Buscar en título o autores"),
    language_code: Optional[str] = Query(None, description="Filtrar por código de idioma (ej. 'eng')"),
    year_from: Optional[int] = Query(None, description="Año mínimo de publicación"),
    year_to: Optional[int] = Query(None, description="Año máximo de publicación"),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """
    Lista de libros con filtros básicos.
    """
    sql = """
    SELECT
        book_id,
        title,
        authors,
        language_code,
        original_publication_year
    FROM BOOK
    WHERE 1=1
    """
    params: dict = {}

    if q:
        sql += " AND (LOWER(title) LIKE :q OR LOWER(authors) LIKE :q)"
        params["q"] = f"%{q.lower()}%"

    if language_code:
        sql += " AND language_code = :lang"
        params["lang"] = language_code

    if year_from is not None:
        sql += " AND original_publication_year >= :y_from"
        params["y_from"] = year_from

    if year_to is not None:
        sql += " AND original_publication_year <= :y_to"
        params["y_to"] = year_to

    sql += """
    ORDER BY COALESCE(original_publication_year, 0) DESC, title ASC
    LIMIT :limit OFFSET :offset
    """
    params["limit"] = limit
    params["offset"] = offset

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).mappings().all()

    return [BookOut(**row) for row in rows]


@app.get("/books/{book_id}", response_model=BookOut)
def get_book(book_id: int):
    """
    Detalle de un libro por book_id.
    """
    sql = """
    SELECT
        book_id,
        title,
        authors,
        language_code,
        original_publication_year
    FROM BOOK
    WHERE book_id = :id
    """

    with engine.connect() as conn:
        row = conn.execute(text(sql), {"id": book_id}).mappings().first()

    if not row:
        raise HTTPException(status_code=404, detail="Book not found")

    return BookOut(**row)


@app.get(
    "/users/{user_id}/recommendations",
    response_model=List[RecommendationOut],
)
def user_recommendations(
    user_id: int,
    n: int = Query(10, ge=1, le=50),
    min_ratings: int = Query(20, ge=1, le=1000),
):
    """
    Recomendaciones para un usuario.

    Usa la función get_recommendations_for_user del módulo collaborative,
    que aplica un baseline basado en popularidad filtrando libros ya leídos.
    """
    # Comprobamos que el usuario existe
    with engine.connect() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM USER WHERE user_id = :uid"),
            {"uid": user_id},
        ).first()

    if not exists:
        raise HTTPException(status_code=404, detail="User not found")

    df = get_recommendations_for_user(user_id=user_id, n=n, min_ratings=min_ratings)
    if df.empty:
        return []

    records = df.to_dict(orient="records")
    return [RecommendationOut(**rec) for rec in records]


@app.post("/ratings", response_model=RatingOut, status_code=status.HTTP_201_CREATED)
def create_or_update_rating(payload: RatingIn):
    """
    Inserta o actualiza una valoración de usuario sobre una copia.

    - Valida que existan USER y COPY.
    - Si ya existe rating para (user_id, copy_id), lo actualiza.
    - Si no, inserta uno nuevo.
    """
    params = {
        "uid": payload.user_id,
        "cid": payload.copy_id,
        "rating": payload.rating,
    }

    with engine.begin() as conn:
        # Comprobar existencia de COPY
        copy_exists = conn.execute(
            text("SELECT 1 FROM COPY WHERE copy_id = :cid"),
            {"cid": payload.copy_id},
        ).first()
        if not copy_exists:
            raise HTTPException(status_code=400, detail="copy_id does not exist")

        # Comprobar existencia de USER
        user_exists = conn.execute(
            text("SELECT 1 FROM USER WHERE user_id = :uid"),
            {"uid": payload.user_id},
        ).first()
        if not user_exists:
            raise HTTPException(status_code=400, detail="user_id does not exist")

        # Intentar actualizar primero
        result = conn.execute(
            text(
                """
                UPDATE RATING
                SET rating = :rating
                WHERE user_id = :uid AND copy_id = :cid
                """
            ),
            params,
        )
        if result.rowcount == 0:
            # Si no había fila, insertamos
            conn.execute(
                text(
                    """
                    INSERT INTO RATING (user_id, copy_id, rating)
                    VALUES (:uid, :cid, :rating)
                    """
                ),
                params,
            )

    return RatingOut(
        user_id=payload.user_id,
        copy_id=payload.copy_id,
        rating=payload.rating,
    )
