from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def clean_books(
    raw_path: Path = RAW_DIR / "books.csv",
    out_path: Path = PROCESSED_DIR / "books_clean.csv",
) -> dict:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    # Leemos el CSV original; on_bad_lines="skip" para saltar líneas corruptas
    df = pd.read_csv(raw_path, on_bad_lines="skip")

    n_in = len(df)

    # Eliminamos duplicados por book_id (nos quedamos con la primera aparición)
    df = df.drop_duplicates(subset=["book_id"], keep="first")

    # Tipos
    df["book_id"] = df["book_id"].astype(int)

    # Limpieza básica de strings
    str_cols = ["isbn", "authors", "original_title", "title", "language_code", "image_url"]
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Año de publicación a entero (cuando se pueda)
    if "original_publication_year" in df.columns:
        df["original_publication_year"] = pd.to_numeric(
            df["original_publication_year"], errors="coerce"
        ).astype("Int64")

    n_out = len(df)

    df.to_csv(out_path, index=False)

    return {
        "table": "books",
        "input_rows": int(n_in),
        "output_rows": int(n_out),
        "dropped_rows": int(n_in - n_out),
        "output_path": str(out_path),
    }


if __name__ == "__main__":
    stats = clean_books()
    print(stats)
