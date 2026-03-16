"""
database.py — SQLite engine + helpers
"""
import sqlite3
from contextlib import contextmanager
from app.config import settings


def get_connection() -> sqlite3.Connection:
    """Abre conexion SQLite con row_factory = Row."""
    conn = sqlite3.connect(settings.db_url)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


@contextmanager
def get_db():
    """Context manager que hace commit automatico o rollback."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query(sql: str, params: tuple = ()) -> list[dict]:
    """Ejecuta SELECT y retorna lista de dicts."""
    with get_db() as conn:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def query_one(sql: str, params: tuple = ()) -> dict | None:
    """Ejecuta SELECT y retorna un dict o None."""
    with get_db() as conn:
        row = conn.execute(sql, params).fetchone()
        return dict(row) if row else None


def execute(sql: str, params: tuple = ()) -> int:
    """Ejecuta INSERT/UPDATE/DELETE y retorna rowcount."""
    with get_db() as conn:
        cursor = conn.execute(sql, params)
        return cursor.rowcount


def executemany(sql: str, params_list: list[tuple]) -> int:
    """Ejecuta INSERT/UPDATE/DELETE en batch."""
    with get_db() as conn:
        cursor = conn.executemany(sql, params_list)
        return cursor.rowcount


def executescript(sql: str):
    """Ejecuta un script SQL completo (CREATE TABLE, etc)."""
    with get_db() as conn:
        conn.executescript(sql)
