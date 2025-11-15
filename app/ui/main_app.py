import sys
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import text

# Asegurarnos de que la raíz del proyecto está en sys.path
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.api.dependencies import get_engine
from app.recommender.popularity import get_top_books_global
from app.recommender.collaborative import get_recommendations_for_user

# Engine global a la base de datos
engine = get_engine()


# =========================
# Funciones auxiliares BD
# =========================

def load_catalog(language=None, year_from=None, year_to=None, limit=200) -> pd.DataFrame:
    """
    Devuelve un catálogo de libros con algunas métricas de popularidad.
    Permite filtrar por idioma y año de publicación.
    """
    sql = """
    SELECT
        b.book_id,
        b.title,
        b.authors,
        b.language_code,
        b.original_publication_year,
        COUNT(r.rating) AS num_ratings,
        AVG(r.rating)   AS mean_rating
    FROM BOOK b
    LEFT JOIN COPY c   ON c.book_id = b.book_id
    LEFT JOIN RATING r ON r.copy_id = c.copy_id
    WHERE 1=1
    """
    params = {}

    if language:
        sql += " AND b.language_code = :lang"
        params["lang"] = language

    if year_from is not None:
        sql += " AND b.original_publication_year >= :y_from"
        params["y_from"] = year_from

    if year_to is not None:
        sql += " AND b.original_publication_year <= :y_to"
        params["y_to"] = year_to

    sql += """
    GROUP BY
        b.book_id, b.title, b.authors, b.language_code, b.original_publication_year
    ORDER BY
        COALESCE(b.original_publication_year, 0) DESC,
        num_ratings DESC
    LIMIT :limit
    """
    params["limit"] = limit

    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params=params)

    return df


def get_user_ratings(user_id: int) -> pd.DataFrame:
    """
    Devuelve las puntuaciones de un usuario con info básica del libro.
    """
    sql = """
    SELECT
        r.user_id,
        r.copy_id,
        r.rating,
        b.book_id,
        b.title,
        b.authors,
        b.language_code
    FROM RATING r
    JOIN COPY c ON r.copy_id = c.copy_id
    JOIN BOOK b ON c.book_id = b.book_id
    WHERE r.user_id = :uid
    ORDER BY r.rating DESC
    """
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"uid": user_id})
    return df


def upsert_rating(user_id: int, copy_id: int, rating: int):
    """
    Inserta o actualiza un rating (user_id, copy_id).
    Devuelve (ok: bool, mensaje: str).
    """
    params = {"uid": user_id, "cid": copy_id, "rating": rating}

    with engine.begin() as conn:
        # Comprobar COPY
        copy_exists = conn.execute(
            text("SELECT 1 FROM COPY WHERE copy_id = :cid"),
            {"cid": copy_id},
        ).first()
        if not copy_exists:
            return False, "El copy_id no existe en la base de datos."

        # Comprobar USER
        user_exists = conn.execute(
            text("SELECT 1 FROM USER WHERE user_id = :uid"),
            {"uid": user_id},
        ).first()
        if not user_exists:
            return False, "El user_id no existe en la base de datos."

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
            # Si no había, insertamos
            conn.execute(
                text(
                    """
                    INSERT INTO RATING (user_id, copy_id, rating)
                    VALUES (:uid, :cid, :rating)
                    """
                ),
                params,
            )

    return True, "Rating guardado correctamente."


# =========================
# Páginas de Streamlit
# =========================

def render_home():
    st.title("Sistema de recomendación de libros – Grupo 1")
    st.markdown(
        """
        Bienvenido/a a la **Casa de la Cultura Next** 📚  

        Este MVP incluye:

        - Un **pipeline ETL** que carga los CSV originales a una base de datos SQLite.
        - Un **motor de recomendación** basado en popularidad y en el historial del usuario.
        - Una **API FastAPI** para exponer catálogo y recomendaciones.
        - Esta **interfaz en Streamlit** para que cualquier persona pueda explorar el catálogo,
          ver recomendaciones y registrar nuevas puntuaciones.
        """
    )
    st.info("Usa el menú lateral para navegar entre catálogo, recomendaciones, puntuaciones y dashboards.")


def render_catalog():
    st.title("Catálogo de libros")

    # Filtros en la barra lateral
    st.sidebar.subheader("Filtros catálogo")

    with engine.connect() as conn:
        langs = pd.read_sql(
            text("SELECT DISTINCT language_code FROM BOOK WHERE language_code IS NOT NULL ORDER BY language_code"),
            conn,
        )["language_code"].tolist()

        years = pd.read_sql(
            text("SELECT DISTINCT original_publication_year FROM BOOK WHERE original_publication_year IS NOT NULL ORDER BY original_publication_year"),
            conn,
        )["original_publication_year"].tolist()

    language = st.sidebar.selectbox("Idioma (language_code)", ["(todos)"] + langs)
    year_min = st.sidebar.selectbox("Año mínimo", ["(sin mínimo)"] + years)
    year_max = st.sidebar.selectbox("Año máximo", ["(sin máximo)"] + years)
    limit = st.sidebar.slider("Número máximo de libros", min_value=50, max_value=500, value=200, step=50)

    lang_filter = None if language == "(todos)" else language
    y_from = None if year_min == "(sin mínimo)" else int(year_min)
    y_to = None if year_max == "(sin máximo)" else int(year_max)

    df = load_catalog(language=lang_filter, year_from=y_from, year_to=y_to, limit=limit)

    st.write(f"Mostrando {len(df)} libros")
    st.dataframe(df)


def render_recommendations():
    st.title("Mis recomendaciones")

    st.markdown(
        """
        Introduce tu `user_id` para obtener recomendaciones basadas en tu historial.
        """
    )

    user_id = st.number_input("User ID", min_value=1, step=1, format="%d")
    n = st.slider("Número de recomendaciones", min_value=5, max_value=50, value=10, step=5)
    min_ratings = st.slider("Mínimo de ratings por libro", min_value=1, max_value=200, value=20, step=5)

    if st.button("Ver recomendaciones"):
        if not user_id:
            st.error("Debes introducir un user_id válido.")
            return

        # Comprobar que el usuario existe
        with engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM USER WHERE user_id = :uid"),
                {"uid": int(user_id)},
            ).first()

        if not exists:
            st.error("El user_id no existe en la base de datos.")
            return

        df = get_recommendations_for_user(
            user_id=int(user_id),
            n=int(n),
            min_ratings=int(min_ratings),
        )

        if df.empty:
            st.warning("No se han encontrado recomendaciones para este usuario (quizá tiene muy pocas valoraciones).")
        else:
            st.success("Recomendaciones generadas:")
            st.dataframe(df)


def render_ratings():
    st.title("Mis puntuaciones")

    st.markdown("Consulta tus puntuaciones y añade/actualiza ratings.")

    user_id = st.number_input("User ID para consultar tus puntuaciones", min_value=1, step=1, format="%d")

    if user_id:
        df = get_user_ratings(int(user_id))
        if df.empty:
            st.info("Este usuario aún no tiene puntuaciones registradas.")
        else:
            st.subheader("Puntuaciones existentes")
            st.dataframe(df)

    st.subheader("Añadir / actualizar una puntuación")

    with st.form("form_rating"):
        f_user_id = st.number_input("User ID", min_value=1, step=1, format="%d", key="rating_user_id")
        copy_id = st.number_input("Copy ID", min_value=1, step=1, format="%d")
        rating = st.slider("Rating", min_value=1, max_value=5, value=3, step=1)
        submitted = st.form_submit_button("Guardar rating")

    if submitted:
        ok, msg = upsert_rating(int(f_user_id), int(copy_id), int(rating))
        if ok:
            st.success(msg)
        else:
            st.error(msg)


def render_dashboards():
    st.title("Dashboards de uso y estadísticas")

    # Métricas básicas
    with engine.connect() as conn:
        n_users = conn.execute(text("SELECT COUNT(*) FROM USER")).scalar()
        n_books = conn.execute(text("SELECT COUNT(*) FROM BOOK")).scalar()
        n_ratings = conn.execute(text("SELECT COUNT(*) FROM RATING")).scalar()

    col1, col2, col3 = st.columns(3)
    col1.metric("Usuarios", n_users)
    col2.metric("Libros", n_books)
    col3.metric("Ratings", n_ratings)

    # Top libros por nº de ratings (usamos el recomendador de popularidad)
    st.subheader("Libros más populares (por número de ratings)")
    top_pop = get_top_books_global(n=10, min_ratings=50)
    if not top_pop.empty:
        st.dataframe(top_pop[["title", "authors", "num_ratings", "mean_rating"]])

        chart_data = top_pop.set_index("title")[["num_ratings"]]
        st.bar_chart(chart_data)
    else:
        st.info("No se ha podido calcular el top de popularidad.")

    # Distribución de edad de usuarios
    st.subheader("Distribución de edad de usuarios (usuarios con fecha de nacimiento)")
    with engine.connect() as conn:
        df_users = pd.read_sql(
            text("SELECT fecha_nacimiento FROM USER WHERE fecha_nacimiento IS NOT NULL"),
            conn,
        )

    if not df_users.empty:
        df_users["fecha_nacimiento"] = pd.to_datetime(df_users["fecha_nacimiento"], errors="coerce")
        df_users = df_users.dropna(subset=["fecha_nacimiento"])
        current_year = pd.Timestamp.now().year
        df_users["edad"] = current_year - df_users["fecha_nacimiento"].dt.year

        st.write(f"Nº usuarios con edad conocida: {len(df_users)}")
        st.bar_chart(df_users["edad"].value_counts().sort_index())
    else:
        st.info("No hay información de edad disponible.")

    # Evolución por año de publicación (proxy temporal)
    st.subheader("Número de libros por año de publicación")
    with engine.connect() as conn:
        df_years = pd.read_sql(
            text(
                """
                SELECT
                    original_publication_year AS year,
                    COUNT(*) AS num_books
                FROM BOOK
                WHERE original_publication_year IS NOT NULL
                GROUP BY original_publication_year
                ORDER BY original_publication_year
                """
            ),
            conn,
        )

    if not df_years.empty:
        df_years = df_years.set_index("year")
        st.line_chart(df_years["num_books"])
    else:
        st.info("No hay datos de año de publicación para los libros.")


# =========================
# Entry point
# =========================

def main():
    st.set_page_config(page_title="Book Recommender – Grupo 1", layout="wide")

    st.sidebar.title("Navegación")
    page = st.sidebar.radio(
        "Ir a:",
        ("Home", "Catálogo", "Mis recomendaciones", "Mis puntuaciones", "Dashboards"),
    )

    if page == "Home":
        render_home()
    elif page == "Catálogo":
        render_catalog()
    elif page == "Mis recomendaciones":
        render_recommendations()
    elif page == "Mis puntuaciones":
        render_ratings()
    elif page == "Dashboards":
        render_dashboards()


if __name__ == "__main__":
    main()
