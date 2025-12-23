import datetime
import pymysql
import polars as pl

from typing import List, Optional
from bulk_upload.config import DB_CRED
from bulk_upload.config import UPLOAD_PROCESS_LOGS

# fetch from db
def fetch_from_db(
    table: str,
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
    attributes: list = [],
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
    table: str,
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
    attributes: Optional[List[str]] = None,
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


# create_job_entry
def create_job_entry(
    process_id: str,
    total_record: int,
    uploaded_file_name: str,
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
) -> None:

    query = f"""
        INSERT INTO {UPLOAD_PROCESS_LOGS}
        (
            process_id,
            total_record,
            records_completed,
            uploaded_file_name,
            status,
            is_deleted
        )
        VALUES (%s, %s, 0, %s, 'PENDING', 0)
    """

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        autocommit=False,
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                query,
                (process_id, total_record, uploaded_file_name)
            )
        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to create job entry: {e}")

    finally:
        conn.close()


# update job progress
def update_job_progress(
    process_id: str,
    completed_inc: int,
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
) -> None:

    query = f"""
        UPDATE {UPLOAD_PROCESS_LOGS}
        SET
            records_completed = records_completed + %s,
            status = 'PROCESSING'
        WHERE process_id = %s
          AND is_deleted = 0
    """

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        autocommit=False,
    )

    try:
        with conn.cursor() as cursor:
            affected = cursor.execute(
                query,
                (completed_inc, process_id)
            )

            if affected == 0:
                raise ValueError(
                    f"No active job found with process_id={process_id}"
                )

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to update job progress: {e}")

    finally:
        conn.close()


# mark job completed 
def mark_job_completed(
    process_id: str,
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
) -> None:

    query = f"""
        UPDATE {UPLOAD_PROCESS_LOGS}
        SET status = 'COMPLETED'
        WHERE process_id = %s
          AND is_deleted = 0
    """

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        autocommit=False,
    )

    try:
        with conn.cursor() as cursor:
            affected = cursor.execute(query, (process_id,))

            if affected == 0:
                raise ValueError(
                    f"No active job found with process_id={process_id}"
                )

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to mark job completed: {e}")

    finally:
        conn.close()

# mark job failed
def mark_job_failed(
    process_id: str,
    host: str,
    user: str,
    password: str,
    database: str,
    port: int = 3306,
) -> None:

    query = f"""
        UPDATE {UPLOAD_PROCESS_LOGS}
        SET status = 'FAILED'
        WHERE process_id = %s
          AND is_deleted = 0
    """

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        database=database,
        port=port,
        autocommit=False,
    )

    try:
        with conn.cursor() as cursor:
            affected = cursor.execute(query, (process_id,))

            if affected == 0:
                raise ValueError(
                    f"No active job found with process_id={process_id}"
                )

        conn.commit()

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to mark job failed: {e}")

    finally:
        conn.close()

