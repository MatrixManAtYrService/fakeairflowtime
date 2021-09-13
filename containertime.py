import textwrap
import sqlite3
import psycopg2
from airflow import settings
import sys
from datetime import datetime

create_sqlite = textwrap.dedent(
    """
    CREATE TABLE IF NOT EXISTS faketime (
        actual_time_of_first_call TEXT NOT NULL
    );
    """)

create_postgres = textwrap.dedent(
    """
    CREATE TABLE IF NOT EXISTS faketime (
        actual_time_of_first_call VARCHAR(255) NOT NULL
    );
    """)

get_offset = "SELECT * FROM faketime;"

def get_or_set(now, faked) -> datetime:

    # create the 'faketime' table
    url = settings.engine.url
    if url.get_backend_name() == 'sqlite':
        print("Using sqlite backend", file=sys.stderr)
        conn = sqlite3.connect(url.database)
        cursor = conn.cursor()

        print(create_sqlite, file=sys.stderr)
        cursor.execute(create_sqlite)

    elif url.get_backend_name() == 'postgresql':
        print("Using postgresql backend", file=sys.stderr)
        host = url.host or ""
        port = str(url.port or "5432")
        user = url.username or ""
        password = url.password or ""
        schema = url.database
        conn = psycopg2.connect(host=host, user=user, port=port, password=password, dbname=schema)
        cursor = conn.cursor()

        print(create_postgres, file=sys.stderr)
        cursor.execute(create_postgres)

    # see if it has data
    print(get_offset, file=sys.stderr)
    cursor.execute(get_offset)
    start_clock_str = cursor.fetchone()

    if start_clock_str:
        start_clock_str = start_clock_str[0]

        print(f"Found previous faketime call at {start_clock_str}.  Offsetting...", file=sys.stderr)
        start_clock = datetime.fromisoformat(start_clock_str)

    else:
        print("No previously faketime call, assuming this is the first.", file=sys.stderr)
        sql = f"INSERT INTO faketime VALUES('{now.isoformat()}');"
        print(sql, file=sys.stderr)
        cursor.execute(sql)
        start_clock = now

    # mischeif managed
    cursor.close()
    conn.commit()
    conn.close()

    # this faketime call = first faketime call + time elapsed since that first call
    new_fake = faked + (now - start_clock)
    print(f"Now: {now}, First Fake: {start_clock}", file=sys.stderr)
    print(f"Faking time: {new_fake.isoformat()}", file=sys.stderr)
    return new_fake


if __name__ == "__main__":
    now = datetime.now()
    faked = datetime.fromisoformat(sys.argv[1])
    offset_fake = get_or_set(now, faked)
    print(offset_fake.isoformat())
