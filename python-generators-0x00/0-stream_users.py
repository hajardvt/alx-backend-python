#!/usr/bin/python3
from seed import connect_to_prodev


def stream_users():
    """
    Uses a generator to fetch rows one by one from user_data.
    Exactly one loop.
    Yields dicts: {'user_id':..., 'name':..., 'email':..., 'age':...}
    """
    conn = connect_to_prodev()
    if not conn:
        return
    try:
        # dictionary=True gives us dict rows; unbuffered so we stream
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT user_id, name, email, age FROM user_data")
        for row in cursor:  # <-- one loop
            yield row
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()
