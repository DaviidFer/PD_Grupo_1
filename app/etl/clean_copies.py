from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def clean_copies(
    raw_path: Path = RAW_DIR / "copies(ejemplares).csv",
    out_path: Path = PROCESSED_DIR / "copies_clean.csv",
) -> dict:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(raw_path)

    n_in = len(df)

    # Tipos
    df["copy_id"] = df["copy_id"].astype(int)
    df["book_id"] = df["book_id"].astype(int)

    # Duplicados por copy_id
    df = df.drop_duplicates(subset=["copy_id"], keep="first")

    n_out = len(df)

    df.to_csv(out_path, index=False)

    return {
        "table": "copies",
        "input_rows": int(n_in),
        "output_rows": int(n_out),
        "dropped_rows": int(n_in - n_out),
        "output_path": str(out_path),
    }


if __name__ == "__main__":
    stats = clean_copies()
    print(stats)
