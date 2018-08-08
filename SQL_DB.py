import sqlite3

with sqlite3.connect("IMDB_testing.db") as conn:
    c = conn.cursor()

    try:
        c.execute('''CREATE TABLE test_main_table (
            id PRIMARY KEY,
            name VARCRCHAR(100) UNIQUE NOT NULL,
            employees INTEGER DEFAULT 0)''')
    except sqlite3.OperationalError as e:
        print('sqlite error:', e.args[0])  # table companies already exists

    conn.commit()
