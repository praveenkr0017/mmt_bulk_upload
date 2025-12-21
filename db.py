import datetime
import pymysql
import polars as pl

from typing import List, Optional
from bulk_upload.config import DB_CRED

# fetch from db

def fetch_from_db(
    host: str,
    user: str,
    password: str,
    database: str,
    table: str,
    attributes: list = [],
    port: int = 3306
) -> pl.DataFrame:

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        cursorclass=pymysql.cursors.DictCursor
    )

    with conn.cursor() as cursor:
        if attributes:
            try:
                cursor.execute(f"SELECT {','.join(attributes)} FROM `{table}`;")
            except Exception:
                cursor.execute(f"SELECT * FROM `{table}`;")
        else:
            cursor.execute(f"SELECT * FROM `{table}`;")

        rows = cursor.fetchall()

    conn.close()

    # 🔥 Normalize datetime → string
    for row in rows:
        for k, v in row.items():
            if isinstance(v, (datetime.datetime, datetime.date)):
                row[k] = v.isoformat()

    return pl.DataFrame(rows)

# insert into db
def insert_into_db(
    df: pl.DataFrame,
    host: str,
    user: str,
    password: str,
    database: str,
    table: str,
    attributes: Optional[List[str]] = None,
    port: int = 3306,
) -> int:

    if df.is_empty():
        raise ValueError("DataFrame is empty — nothing to insert.")

    if df.height != 1:
        raise ValueError("This function supports exactly one row per insert.")

    # Lock column order
    columns = attributes if attributes else list(df.columns)

    # Validate columns
    missing_cols = set(columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Columns missing in DataFrame: {missing_cols}")

    # Build INSERT query
    col_str = ", ".join(f"`{c}`" for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    insert_query = f"""
        INSERT INTO `{table}` ({col_str})
        VALUES ({placeholders})
    """

    # Extract single row
    values = (
        df
        .select(columns)
        .to_numpy()
        .tolist()[0]
    )

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        autocommit=False
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(insert_query, values)
            new_id = cursor.lastrowid
        conn.commit()
        return new_id

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Insert failed: {e}")

    finally:
        conn.close()