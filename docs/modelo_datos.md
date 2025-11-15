# Modelo de datos – Sistema de recomendación de libros

## 1. Entidades y tablas

### 1.1. USER

Información de usuarios del sistema.

- **user_id** (INT, PK)
- **sexo** (VARCHAR(30), NULLable)
- **fecha_nacimiento** (DATE, NULLable)
- **comentario** (TEXT, NULLable)
- **tiene_info_demografica** (BOOL, default FALSE)

Regla: la tabla USER contendrá **todos** los usuarios que aparecen en `ratings`.  
Los 501 usuarios de `user_info.csv` se insertarán con sus datos demográficos y `tiene_info_demografica = TRUE`; el resto se insertará con sexo/fecha/comentario a NULL.

---

### 1.2. BOOK

Información de libros.

- **book_id** (INT, PK)
- **isbn** (VARCHAR(20), NULLable)
- **title** (TEXT, NOT NULL)
- **original_title** (TEXT, NULLable)
- **original_publication_year** (INT, NULLable)
- **language_code** (VARCHAR(10), NULLable)
- **image_url** (TEXT, NULLable)

Reglas:
- `book_id` viene del CSV y se respeta como clave principal.
- ISBN no siempre está presente ni es único; se usará solo como dato informativo.
- Si un libro viene con datos inconsistentes en distintas filas, se prioriza la primera fila válida.

---

### 1.3. COPY

Ejemplares físicos del libro (cada copia que puede prestarse).

- **copy_id** (INT, PK)
- **book_id** (INT, NOT NULL, FK → BOOK.book_id)
- **fecha_alta** (DATE, NULLable, futura ampliación)
- **estado** (VARCHAR(20), NULLable, futura ampliación)
- **ubicacion_actual** (VARCHAR(100), NULLable, futura ampliación)

Reglas:
- Cada `copy_id` referencia exactamente a un `book_id`.
- Si en el CSV aparece un `book_id` que no existe en BOOK, se considerará error de calidad de datos.  
  Para este dataset hay al menos dos casos (p.ej. 6582 y 9265); se decidirá:
  - O bien eliminar esos ejemplares y sus ratings,
  - O bien crear el BOOK con metadatos vacíos.

---

### 1.4. RATING

Valoraciones de usuarios sobre ejemplares.

- **rating_id** (INT, PK, autoincremental)
- **user_id** (INT, FK → USER.user_id)
- **copy_id** (INT, FK → COPY.copy_id)
- **score** (SMALLINT, NOT NULL, entre 1 y 5)
- **created_at** (DATETIME, NULLable, futura ampliación)

Reglas:
- La combinación (`user_id`, `copy_id`) debe ser única (un usuario valora una copia como máximo una vez).  
- Si hubiera duplicados, se podría:
  - Conservar solo la valoración más reciente,
  - O hacer una media; en este dataset no hay duplicados.

---

### 1.5. AUTHOR y BOOK_AUTHOR

A partir de la columna `authors` de `books.csv`.

**AUTHOR**

- **author_id** (INT, PK)
- **name** (TEXT, UNIQUE NOT NULL)

**BOOK_AUTHOR**

- **book_id** (INT, FK → BOOK.book_id)
- **author_id** (INT, FK → AUTHOR.author_id)
- PK compuesta: (book_id, author_id)

Reglas:
- La columna `books.authors` puede tener uno o varios autores separados por comas.
- En el ETL se dividirá esa columna y se generarán filas en BOOK_AUTHOR.

---

### 1.6. GENRE y BOOK_GENRE (futura ampliación)

No existe columna de género explícita en el CSV de libros; se deja preparada la estructura.

**GENRE**

- **genre_id** (INT, PK)
- **name** (TEXT, UNIQUE NOT NULL)

**BOOK_GENRE**

- **book_id** (INT, FK → BOOK.book_id)
- **genre_id** (INT, FK → GENRE.genre_id)
- PK compuesta: (book_id, genre_id)

---

## 2. Diagrama ER (simplificado, texto)

```text
USER (user_id, ...)
   1  ─────<  RATING (rating_id, user_id, copy_id, score, ...)
                      ^
                      |
COPY (copy_id, book_id, ...)
   N  ─────<  

BOOK (book_id, isbn, title, ...)
   1  ─────<  COPY (copy_id, book_id, ...)
   1  ─────<  BOOK_AUTHOR (book_id, author_id)  >────  AUTHOR (author_id, name)
   1  ─────<  BOOK_GENRE  (book_id, genre_id)   >────  GENRE  (genre_id, name)
