from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def clean_ratings(
    raw_path: Path = RAW_DIR / "ratings.csv",
    out_path: Path = PROCESSED_DIR / "ratings_clean.csv",
) -> dict:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(raw_path)

    n_in = len(df)

    # Tipos
    df["user_id"] = df["user_id"].astype(int)
    df["copy_id"] = df["copy_id"].astype(int)
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce").astype("Int64")

    # Filtro de ratings válidos (1 a 5)
    df = df[df["rating"].between(1, 5)]

    # Eliminar duplicados (user_id, copy_id)
    df = df.drop_duplicates(subset=["user_id", "copy_id"], keep="first")

    n_out = len(df)

    df.to_csv(out_path, index=False)

    return {
        "table": "ratings",
        "input_rows": int(n_in),
        "output_rows": int(n_out),
        "dropped_rows": int(n_in - n_out),
        "output_path": str(out_path),
    }


if __name__ == "__main__":
    stats = clean_ratings()
    print(stats)
