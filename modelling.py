import sqlalchemy
import os
import re
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer


class MovieVectorization(object):
    def __init__(self, db_name):
        self.sql_engine = sqlalchemy.create_engine('sqlite:///{}'.format(os.path.join(os.getcwd(), db_name)))
        self.movies_table = pd.read_sql("""select * from movies""", con=self.sql_engine)

    @staticmethod
    def extract_win_and_nominated_awards(awards_string):
        win_number, nominate_number = 0, 0
        for i in re.split('[&,\.]', str(awards_string)):
            if 'nominat' in i.lower():
                nominate_number += int(re.search('\d+', i).group(0))
            elif ('win' in i.lower()) or ('won' in i.lower()):
                win_number += int(re.search('\d+', i).group(0))

        return win_number, nominate_number

    @staticmethod
    def extract_from_comma_separated_strings(full_df, column_name):
        vec = CountVectorizer(tokenizer=lambda t: re.split(' , |, |,| ,', t))

        df_array = vec.fit_transform(full_df[column_name].fillna('Not_provided')).toarray()
        fields = ['{}_{}'.format(column_name, col) for col in vec.get_feature_names()]

        return pd.DataFrame(df_array, columns=fields)

    def movies_cleaning(self):
        # drop unrelevant columns
        self.movies_table.drop(['id', 'DVD', 'Website', 'Response', 'Poster', 'Released'], axis=1, inplace=True)
        # conver to nan
        self.movies_table.replace('N/A', np.nan, inplace=True)

        self.movies_table['imdbVotes'] = self.movies_table['imdbVotes'].str.replace(',', '').astype(float)

        box_office_pound = self.movies_table.dropna()[~self.movies_table['BoxOffice'].dropna().str.contains('\$')]

        self.movies_table['BoxOffice'] = self.movies_table.BoxOffice.str.replace('\D', '').replace('', np.nan)
        self.movies_table['BoxOffice'] = self.movies_table['BoxOffice'].astype(float)
        self.movies_table.loc[box_office_pound.index, 'BoxOffice'] *= 1.3

        self.movies_table['Year'] = self.movies_table['Year'].apply(lambda years:
                                                                    np.mean([float(year)
                                                                             for year in str(years).split(r'–')])
                                                                    )

        self.movies_table['Runtime'] = self.movies_table.Runtime.str.extract('(\d+)', expand=True).astype(float)

    def movies_extract_features(self):
        self.movies_table['Awards_wins'], self.movies_table['Awards_nominate'] =\
            zip(*self.movies_table['Awards'].apply(self.extract_win_and_nominated_awards))

        self.movies_table.drop('Awards', axis=1, inplace=True)

        for column_name in ['Country', 'Director', 'Genre', 'Language', 'Actors', 'Production', 'Writer']:
            if column_name == 'Director':  # There are some co-directors which is noted with perentesis
                self.movies_table['Director'] = self.movies_table['Director'].str.replace('\(.+\)', '')
                self.movies_table = self.movies_table.join(self.extract_from_comma_sperated_strings(self.movies_table,
                                                                                                    column_name))
            self.movies_table.drop(column_name, axis=1, inplace=True)

        return self.movies_table


if __name__ == '__main__':
    movies = MovieVectorization(db_name='imdb_test.db')
    movies.movies_cleaning()
    movies.movies_extract_features()
    print(movies.movies_table.head())

    # movies_after_feature_extraction.to_sql(name='movies_extracted', con=engine)
