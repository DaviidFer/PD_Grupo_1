from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

from app.etl.clean_books import clean_books
from app.etl.clean_copies import clean_copies
from app.etl.clean_users import clean_users
from app.etl.clean_ratings import clean_ratings


BASE_DIR = Path(__file__).resolve().parents[2]  # raíz del proyecto
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
REPORTS_DIR = BASE_DIR / "docs" / "reportes"
DB_PATH = BASE_DIR / "app" / "db" / "library.db"


def run_etl():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    stats = []

    # 1. Limpieza individual de ficheros
    stats.append(clean_books(raw_path=RAW_DIR / "books.csv",
                             out_path=PROCESSED_DIR / "books_clean.csv"))
    stats.append(clean_copies(raw_path=RAW_DIR / "copies(ejemplares).csv",
                              out_path=PROCESSED_DIR / "copies_clean.csv"))
    stats.append(clean_users(raw_path=RAW_DIR / "user_info.csv",
                             out_path=PROCESSED_DIR / "users_clean.csv"))
    stats.append(clean_ratings(raw_path=RAW_DIR / "ratings.csv",
                               out_path=PROCESSED_DIR / "ratings_clean.csv"))

    # 2. Cargar datos limpios
    books = pd.read_csv(PROCESSED_DIR / "books_clean.csv")
    copies = pd.read_csv(PROCESSED_DIR / "copies_clean.csv")
    ratings = pd.read_csv(PROCESSED_DIR / "ratings_clean.csv")
    users_info = pd.read_csv(PROCESSED_DIR / "users_clean.csv")

    # 3. Limpieza cruzada e integridad referencial

    # 3.1. Filtrar copies cuyo book_id no exista en books
    valid_book_ids = set(books["book_id"])
    copies_before = len(copies)
    copies = copies[copies["book_id"].isin(valid_book_ids)]
    copies_dropped_fk = copies_before - len(copies)

    # 3.2. Filtrar ratings cuyo copy_id no exista en copies
    valid_copy_ids = set(copies["copy_id"])
    ratings_before = len(ratings)
    ratings = ratings[ratings["copy_id"].isin(valid_copy_ids)]
    ratings_dropped_fk = ratings_before - len(ratings)

    # 3.3. Construir tabla USER completa a partir de ratings + user_info
    all_user_ids = pd.Index(sorted(ratings["user_id"].unique()), name="user_id")
    users_full = pd.DataFrame(all_user_ids)

    # unir datos demográficos si existen
    users_full = users_full.merge(users_info, on="user_id", how="left")

    # tiene_info_demografica = True si alguna de las columnas de info no es nula
    info_cols = ["sexo", "comentario", "fecha_nacimiento"]
    for col in info_cols:
        if col not in users_full.columns:
            users_full[col] = pd.NA

    users_full["tiene_info_demografica"] = users_full[info_cols].notna().any(axis=1)

    # 4. Insertar en SQLite usando SQLAlchemy
    engine = create_engine(f"sqlite:///{DB_PATH}")

    with engine.begin() as conn:
        users_full.to_sql("USER", conn, if_exists="replace", index=False)
        books.to_sql("BOOK", conn, if_exists="replace", index=False)
        copies.to_sql("COPY", conn, if_exists="replace", index=False)
        ratings.to_sql("RATING", conn, if_exists="replace", index=False)

    # 5. Generar informe simple
    lines = []
    lines.append("# Informe ETL\n")
    lines.append(f"Base de datos: {DB_PATH}\n\n")

    for s in stats:
        lines.append(f"## {s['table']}\n")
        lines.append(f"- Filas de entrada: {s['input_rows']}\n")
        lines.append(f"- Filas de salida: {s['output_rows']}\n")
        lines.append(f"- Filas descartadas: {s['dropped_rows']}\n")
        lines.append(f"- Fichero limpio: `{s['output_path']}`\n\n")

    lines.append("## Integridad referencial\n")
    lines.append(f"- Ejemplares descartados por FK (book_id inexistente): {copies_dropped_fk}\n")
    lines.append(f"- Ratings descartados por FK (copy_id inexistente): {ratings_dropped_fk}\n")
    lines.append(f"- Usuarios finales en USER: {len(users_full)}\n")
    lines.append(f"- Libros finales en BOOK: {len(books)}\n")
    lines.append(f"- Ejemplares finales en COPY: {len(copies)}\n")
    lines.append(f"- Ratings finales en RATING: {len(ratings)}\n")

    log_path = REPORTS_DIR / "etl_log.md"
    log_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"ETL completado. Informe en {log_path}")
    print(f"Base de datos SQLite en {DB_PATH}")


if __name__ == "__main__":
    run_etl()
