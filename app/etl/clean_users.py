from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")


def clean_users(
    raw_path: Path = RAW_DIR / "user_info.csv",
    out_path: Path = PROCESSED_DIR / "users_clean.csv",
) -> dict:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(raw_path)

    n_in = len(df)

    # Tipos
    df["user_id"] = df["user_id"].astype(int)

    # Limpiar strings
    for col in ["sexo", "comentario"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Parsear fecha_nacimiento (DD/MM/YYYY) a fecha
    if "fecha_nacimiento" in df.columns:
        fechas = pd.to_datetime(
            df["fecha_nacimiento"], format="%d/%m/%Y", errors="coerce"
        )
        df["fecha_nacimiento"] = fechas.dt.date

    # Eliminar posibles duplicados de user_id
    df = df.drop_duplicates(subset=["user_id"], keep="first")

    n_out = len(df)

    df.to_csv(out_path, index=False)

    return {
        "table": "users_info",
        "input_rows": int(n_in),
        "output_rows": int(n_out),
        "dropped_rows": int(n_in - n_out),
        "output_path": str(out_path),
    }


if __name__ == "__main__":
    stats = clean_users()
    print(stats)
