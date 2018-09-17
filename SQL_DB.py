import sqlite3
import os
import json
import re
import glob

class DB(object):
    def __init__(self, db_name="IMDB_data.db"):
        self.db_name = db_name
        self.raw_data_path_folder = r'raw_data'
        self.table_metadata = {'movies': {'Actors':'VARCRCHAR(400)',
                                                       'Awards':'VARCRCHAR(400)',
                                                       'BoxOffice':'VARCRCHAR(400)',
                                                       'Country':'VARCRCHAR(400)',
                                                       'DVD':'VARCRCHAR(400)',
                                                       'Director':'VARCRCHAR(400)',
                                                       'Genre':'VARCRCHAR(400)',
                                                       'Language':'VARCRCHAR(400)',
                                                       'Metascore':'DOUBLE',
                                                       'Plot':'VARCRCHAR(400)',
                                                       'Poster':'VARCRCHAR(400)',
                                                       'Production':'VARCRCHAR(400)',
                                                       'Rated':'VARCRCHAR(400)',
                                                       'Released':'VARCRCHAR(400)',
                                                       'Response':'VARCRCHAR(100)',
                                                       'Runtime':'VARCRCHAR(400)',
                                                       'Title':'VARCRCHAR(400)',
                                                       'Type':'VARCRCHAR(400)',
                                                       'Website':'VARCRCHAR(400)',
                                                       'Writer':'VARCRCHAR(400)',
                                                       'Year':'VARCRCHAR(100)',
                                                       'totalSeasons':'INTEGER',
                                                       'imdbID':'VARCRCHAR(400) UNIQUE NOT NULL',
                                                       'imdbRating':'DOUBLE',
                                                       'imdbVotes':'VARCRCHAR(400)'},
                               'ratings': {'imdbID':'VARCRCHAR(400) NOT NULL',
                                                        'Value':'VARCRCHAR(100)',
                                                        'Source':'VARCRCHAR(100)'},
                               'wiki_media': {'IMDB_ID': 'VARCRCHAR(400) NOT NULL',
                                              'Wiki_ID': 'VARCRCHAR(400) UNIQUE NOT NULL',
                                              'text': 'BLOB'}
                                }  # {table_name:{column_name:column_type}}
        self.validate_field_names()
        self.create_DB()

    def validate_field_names(self):
        for table_name, field_n_type in self.table_metadata.items():
            # check if fields have invalid characters
            for field in field_n_type:
                if re.match('\W', field):
                    raise ValueError('{} fields have invalid characters'.format(field))

    # the Ratings column is out because its more than one row and we make a differ table for it
    def _create_table(self, table_name, table_columns):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()

            column_type = ['{} {}'.format(col, sql_type) for col, sql_type in table_columns.items()]
            try:
                c.execute('''CREATE table IF NOT EXISTS {} (
                    id PRIMARY KEY,
                    {})'''.format(table_name, ', '.join(column_type)))
            except sqlite3.OperationalError as e:
                print('sqlite error:', e.args[0])

            conn.commit()

    def create_DB(self):
        for table_name, table_columns_types in self.table_metadata.items():
            self._create_table(table_name, table_columns_types)

        # Insert raw_data
        self.prepare_data_and_insert()

    def _insert_data(self, data, table):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()


            table_fields = list(self.table_metadata[table].keys())

            values = []
            for movie_details in data:
                values_of_one_movie = tuple(movie_details[k] if k in movie_details else 'Not_existing' for k in table_fields)
                values.append(values_of_one_movie)

            field_names = ', '.join(table_fields)
            place_holders = ', '.join(['?'] * len(table_fields))

            try:
                sql = '''INSERT INTO {} ({}) VALUES ({})'''.format(table, field_names, place_holders)
                c.executemany(sql, values)
            except sqlite3.IntegrityError as e:
                print('sqlite error: ', e.args[0])
            conn.commit()
        print('INSERT {} rows into {}'.format(len(data), table))

    def prepare_data_and_insert(self, over_write=False):
        json_names_to_load = self._raw_jsons()
        if over_write:
            # todo: delete exisiting ids
            print('None')
        else:
            json_names_to_load -= set(self._already_in_db())

        main_data, ratings_data = self._load_jsons(json_names_to_load)
        self._insert_data(data=main_data, table='movies')
        self._insert_data(data=ratings_data, table='ratings')

    def _already_in_db(self):
        with sqlite3.connect(self.db_name) as conn:
            c = conn.cursor()
            if c.execute('SELECT count(0) FROM movies').fetchone()[0] > 0:
                return [row[0] for row in c.execute('SELECT imdbID FROM movies')]
            else:
                return []

    def _raw_jsons(self):
        list_of_json_files = glob.glob(os.path.join(self.raw_data_path_folder, '*.json'))
        return set(map(lambda name: re.search(r'tt\d+', name).group(0), list_of_json_files))

    def _load_jsons(self, json_names):
        main_jsons = []
        ratings_jsons = []
        for name in json_names:
            with open(os.path.join(self.raw_data_path_folder, '{}.json'.format(name)), 'r') as jfile:
                movie_details = json.load(jfile)

                if 'Ratings' in movie_details:
                    for rating in movie_details['Ratings']:
                        rating['imdbID'] = movie_details['imdbID']

                    ratings_jsons += movie_details.pop('Ratings')

                main_jsons.append(movie_details)

        return main_jsons, ratings_jsons

if __name__ == '__main__':
    db = DB('imdb_test.db') #todo: deal with series and not only movies