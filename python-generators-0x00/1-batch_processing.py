#!/usr/bin/python3
from seed import connect_to_prodev


def stream_users_in_batches(batch_size):
    """
    Fetch rows in batches and yield each batch (list of dicts).
    (Loop count budget <= 3 overall across both functions.)
    """
    conn = connect_to_prodev()
    if not conn:
        return
    try:
        cursor = conn.cursor(dictionary=True)
        offset = 0
        while True:  # loop #1
            cursor.execute(
                f"SELECT user_id, name, email, age FROM user_data "
                f"LIMIT {batch_size} OFFSET {offset}"
            )
            batch = cursor.fetchall()
            if not batch:
                break
            yield batch
            offset += batch_size
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


def batch_processing(batch_size):
    """
    Processes each batch to filter users over the age of 25.
    Uses the above generator and prints each matching user.
    Total loops across module ≤ 3.
    """
    for batch in stream_users_in_batches(batch_size):  # loop #2
        for user in batch:  # loop #3
            if int(user["age"]) > 25:
                print(user)
                print()  # matches the blank line spacing in your sample
