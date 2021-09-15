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
    """
)

create_postgres = textwrap.dedent(
    """
    CREATE TABLE IF NOT EXISTS faketime (
        actual_time_of_first_call VARCHAR(255) NOT NULL
    );
    """
)

get_offset = "SELECT * FROM faketime;"


def execute_wrapper(conn, cursor, sql):
    print("query:", file=sys.stderr)
    cursor.execute(sql)
    print(textwrap.indent(sql, "    "), file=sys.stderr)
    for notice in conn.notices:
        if 'NOTICE:  relation "faketime" already exists, skipping' not in notice:
            print(notice, file=sys.stderr)


def get_or_set(now, faked) -> datetime:

    # create the 'faketime' table
    url = settings.engine.url
    if url.get_backend_name() == "sqlite":
        print("Using sqlite backend", file=sys.stderr)
        conn = sqlite3.connect(url.database)
        cursor = conn.cursor()
        execute_wrapper(conn, cursor, create_sqlite)

    elif url.get_backend_name() == "postgresql":
        print("Using postgresql backend", file=sys.stderr)
        host = url.host or ""
        port = str(url.port or "5432")
        user = url.username or ""
        password = url.password or ""
        schema = url.database
        conn = psycopg2.connect(
            host=host, user=user, port=port, password=password, dbname=schema
        )
        cursor = conn.cursor()
        execute_wrapper(conn, cursor, create_postgres)

    # see if it has data
    execute_wrapper(conn, cursor, get_offset)
    start_clock_str = cursor.fetchone()

    if start_clock_str:
        start_clock_str = start_clock_str[0]

        print(
            f"Found previous faketime call at {start_clock_str}.  Offsetting...",
            file=sys.stderr,
        )
        start_clock = datetime.fromisoformat(start_clock_str)

    else:
        print(
            "No previously faketime call, assuming this is the first.", file=sys.stderr
        )
        sql = f"INSERT INTO faketime VALUES('{now.isoformat()}');"
        execute_wrapper(conn, cursor, sql)
        start_clock = now

    if url.get_backend_name() == "sqlite":
        # no need to fake sqlite's time
        # it's a subprocess, so in inherits the faked time
        pass

    elif url.get_backend_name() == "postgresql":

        # rename now() to system_now() only if system_now() doesn't already exist
        shim_function = textwrap.dedent(
            """
            DO $$
                DECLARE
                found_func pg_proc%rowtype;
                BEGIN
                SELECT * FROM pg_proc WHERE proname = 'system_now' INTO found_func;
                IF FOUND THEN
                    RAISE NOTICE 'DB Already Faked';
                ELSE
                    RAISE NOTICE'Faking DB Time';
                    ALTER FUNCTION now RENAME TO system_now;
                END IF;
            END $$;
            """
        )
        execute_wrapper(conn, cursor, shim_function)
 
        # and write a new now() which includes the offset
        if start_clock < faked:
            # faking a future time
            offset = faked - start_clock
            op = "+"
        else:
            # faking a past time
            offset = start_clock - faked
            op = "-"
        print(f"faked: {faked}, start_clock: {start_clock}, offset: {offset}, direction: {op}", file=sys.stderr)

        replace_original = textwrap.dedent(
            """
                CREATE OR REPLACE FUNCTION now() RETURNS timestamptz
                AS $func$
                    SELECT system_now() {op} INTERVAL '{seconds} seconds';
                $func$ LANGUAGE SQL;
                """.format(
                op=op, seconds=offset.total_seconds()
            )
        )
        execute_wrapper(conn, cursor, replace_original)


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
