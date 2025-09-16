#!/usr/bin/python3
from seed import connect_to_prodev


def stream_user_ages():
    """
    Generator that yields ages one by one.
    Exactly one loop here.
    """
    conn = connect_to_prodev()
    if not conn:
        return
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT age FROM user_data")
        for (age,) in cursor:  # one loop
            # ensure numeric
            yield int(age)
    finally:
        try:
            cursor.close()
        except Exception:
            pass
        conn.close()


def print_average_age():
    """
    Uses the generator to compute the average without loading all rows.
    Overall at most two loops in this script (we only loop once here).
    """
    total = 0
    count = 0
    for age in stream_user_ages():  # second (and only) loop at this level
        total += age
        count += 1
    avg = (total / count) if count else 0
    print(f"Average age of users: {avg:.2f}")


if __name__ == "__main__":
    print_average_age()
