import sqlite3
import os
import json
import re
import glob

class DB(object):
    def __init__(self, db_name="IMDB_data.db"):
        self.db_name = db_name
        self.raw_data_path_folder = r'raw_data'
        self.create_DB()

    # the Ratings column is out because its more than one row and we make a differ table for it
    def _create_main_table(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()

            try:
                c.execute('''CREATE TABLE movies (
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
                    imdbVotes VARCRCHAR(400))''')
            except sqlite3.OperationalError as e:
                print('sqlite error:', e.args[0])  # table companies already exists

            conn.commit()
            print('Created the movies table')

    def _create_rating_table(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()

            try:
                c.execute('''CREATE TABLE Ratings (
                    id PRIMARY KEY,
                    imdbID VARCRCHAR(400) NOT NULL,
                    Value VARCRCHAR(100),
                    Source VARCRCHAR(100))
                    ''')
            except sqlite3.OperationalError as e:
                print('sqlite error:', e.args[0])  # table companies already exists

            conn.commit()
            print('Created the Ratings table')

    def create_DB(self):
        if os.path.exists(self.db_name):
          print('Great the {} is exists'.format(self.db_name))
          #todo: check if all the tables exist
          #todo: check that all the json raw data is in and if not insert it
        else:
            self._create_main_table()
            self._create_rating_table()

    def _insert_data(self,data, table):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()

            # keys = data[0].keys()
            # values = []
            # for movie_json in data:
            #     values.append(tuple(movie_json[k] for k in keys))

            keys_name = ', '.join(data[0].keys())
            place_holders = ', '.join(['?'] * len(data[0].keys()))
            value = [tuple(movie_json.values()) for movie_json in data]

            try:
                sql = '''INSERT INTO {} ({}) VALUES ({})'''.format(table, keys_name, place_holders)
                print(sql)
                c.executemany(sql, value)
            except sqlite3.IntegrityError as e:
                print('sqlite error: ', e.args[0])
            conn.commit()
        print('INSERT {} rows into {}'.format(len(data), table))

    def prepare_data_and_insert(self, over_right=False):
        json_names_to_load = self._raw_jsons()
        if over_right:
            # todo: delete exisiting ids
            print('None')
        else:
            json_names_to_load -= set(self._already_in_db())

        main_data, ratings_data = self._load_jsons(json_names_to_load)
        self._insert_data(data=main_data, table='movies')
        self._insert_data(data=ratings_data, table='Ratings')

    def _already_in_db(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()
            return [row[0] for row in c.execute('SELECT imdbID FROM movies')]

    def _raw_jsons(self):
        list_of_json_files = glob.glob(os.path.join(self.raw_data_path_folder, '*.json'))
        return set(map(lambda name: re.search(r'tt\d+', name).group(0), list_of_json_files))

    def _load_jsons(self, json_names):
        main_jsons = []
        ratings_jsons = []
        for name in json_names:
            with open(os.path.join(self.raw_data_path_folder, '{}.json'.format(name)), 'r') as jfile:
                j = json.load(jfile)

                j_rating = j['Ratings']
                for rating in j_rating:
                    rating['imdbID'] = j['imdbID']

                j.pop('Ratings', None)
                main_jsons.append(j)
                ratings_jsons += j_rating
        return main_jsons, ratings_jsons

if __name__ == '__main__':
    db = DB('imdb_test.db')
    # db.prepare_data_and_insert()
    print(db._already_in_db())