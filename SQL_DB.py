import sqlite3
import os

class DB(object):
    def __init__(self):
        self.db_name = "IMDB_data.db"

    # the Ratings column is out because its more than one row and we make a differ table for it
    def _create_main_table(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()

            try:
                c.execute('''CREATE TABLE main_IMDB_api_data (
                    id PRIMARY KEY,
                    Actors VARCRCHAR(400),
                    Awards VARCRCHAR(400),
                    BoxOffice VARCRCHAR(400),
                    Country VARCRCHAR(400),
                    DVD VARCRCHAR(400),
                    Director VARCRCHAR(400),
                    Genre VARCRCHAR(400),
                    Language VARCRCHAR(400),
                    Metascore DOUBLE,
                    Plot VARCRCHAR(400),
                    Poster VARCRCHAR(400),
                    Production VARCRCHAR(400),
                    Rated VARCRCHAR(400),
                    Released VARCRCHAR(400),
                    Response VARCRCHAR(100),
                    Runtime VARCRCHAR(400),
                    Title VARCRCHAR(400),
                    Type VARCRCHAR(400),
                    Website VARCRCHAR(400),
                    Writer VARCRCHAR(400),
                    Year INTEGER,
                    imdbID VARCRCHAR(400) UNIQUE NOT NULL,
                    imdbRating DOUBLE,
                    imdbVotes VARCRCHAR(400)''')
            except sqlite3.OperationalError as e:
                print('sqlite error:', e.args[0])  # table companies already exists

            conn.commit()

    def _create_rating_table(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()

            try:
                c.execute('''CREATE TABLE Ratings_IMDB_api_data (
                    id PRIMARY KEY,
                    imdbID VARCRCHAR(400) NOT NULL,
                    Value VARCRCHAR(100),
                    Source VARCRCHAR(100)
                    ''')
            except sqlite3.OperationalError as e:
                print('sqlite error:', e.args[0])  # table companies already exists

            conn.commit()

    def create_DB(self):
        if not os.path.exists(self.db_name):
            self._create_main_table()
            self._create_rating_table()

    def insert_data(self):

        with sqlite3.connect("sample.db") as conn:
            c = conn.cursor()
            companies = [
                ('Foo', 12),
                ('Bar', 7),
                ('Moo', 99),
            ]

            try:
                sql = '''INSERT INTO companies (name, employees) VALUES (?, ?)'''
                c.executemany(sql, companies)
            except sqlite3.IntegrityError as e:
                print('sqlite error: ', e.args[0])  # column name is not unique
            conn.commit()
if __name__ == '__main__':
    db = DB()

    db.create_DB()