#!/usr/bin/python3
from seed import connect_to_prodev


def paginate_users(page_size, offset):
    conn = connect_to_prodev()
    if not conn:
        return []
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            f"SELECT user_id, name, email, age FROM user_data "
            f"LIMIT {page_size} OFFSET {offset}"
        )
        rows = cursor.fetchall()
        return rows
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


def lazy_pagination(page_size):
    """
    Generator that lazily loads each page on demand.
    Only one loop is used.
    Yields: list[dict] (a page)
    """
    offset = 0
    while True:  # <-- one loop
        page = paginate_users(page_size, offset)
        if not page:
            break
        yield page
        offset += page_size
