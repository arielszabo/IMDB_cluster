import sqlalchemy
import os
import re
import json
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer


class data_loader(object):
    def __init__(self):
        self.movies_n_series, self.ratings = self.load_imdb()
        self.wiki = self.load_wiki()

    @staticmethod
    def load_imdb():
        path_for_movie_jsons = 'raw_data'
        movies_n_series = []
        ratings = []
        for file in os.listdir(path_for_movie_jsons):
            if '.json' in file and not ('!' in file or '?' in file):
                full_file_path = os.path.join(path_for_movie_jsons, file)
                with open(full_file_path, 'r') as jfile:
                    movie_details = json.load(jfile)

                if 'Ratings' in movie_details:
                    for rating in movie_details['Ratings']:
                        rating['imdbID'] = movie_details['imdbID']

                    ratings += movie_details.pop('Ratings')
                movies_n_series.append(movie_details)

        return pd.DataFrame(movies_n_series), pd.DataFrame(ratings)

    @staticmethod
    def load_wiki():
        wiki = []
        path_for_wiki_jsons = os.path.join('raw_data', 'wiki_data')
        for file in os.listdir(path_for_wiki_jsons):
            if '.json' in file and not ('!' in file or '?' in file):
                full_file_path = os.path.join(path_for_wiki_jsons, file)
                with open(full_file_path, 'r') as jfile:
                    wiki.append(json.load(jfile))

        return pd.DataFrame(wiki)


class MovieVectorization(object):
    def __init__(self, movies_n_series_df, rating_df, wiki_df):
        self.movies_n_series_table = movies_n_series_df
        self.ratings = rating_df
        self.wiki_data = wiki_df

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
        self.movies_n_series_table.drop(['id', 'DVD', 'Website', 'Response', 'Poster', 'Released'], axis=1, inplace=True)
        # conver to nan
        self.movies_n_series_table.replace('N/A', np.nan, inplace=True)

        self.movies_n_series_table['imdbVotes'] = self.movies_n_series_table['imdbVotes'].str.replace(',', '').astype(float)

        box_office_pound = self.movies_n_series_table.dropna()[~self.movies_n_series_table['BoxOffice'].dropna().str.contains('\$')]

        self.movies_n_series_table['BoxOffice'] = self.movies_n_series_table.BoxOffice.str.replace('\D', '').replace('', np.nan)
        self.movies_n_series_table['BoxOffice'] = self.movies_n_series_table['BoxOffice'].astype(float)
        self.movies_n_series_table.loc[box_office_pound.index, 'BoxOffice'] *= 1.3

        self.movies_n_series_table['Year'] = self.movies_n_series_table['Year'].apply(lambda years:
                                                                    np.mean([float(year)
                                                                             for year in str(years).split(r'–')])
                                                                                      )

        self.movies_n_series_table['Runtime'] = self.movies_n_series_table.Runtime.str.extract('(\d+)', expand=True).astype(float)

    def movies_extract_features(self):
        self.movies_n_series_table['Awards_wins'], self.movies_n_series_table['Awards_nominate'] =\
            zip(*self.movies_n_series_table['Awards'].apply(self.extract_win_and_nominated_awards))

        self.movies_n_series_table.drop('Awards', axis=1, inplace=True)

        for column_name in ['Country', 'Director', 'Genre', 'Language', 'Actors', 'Production', 'Writer']:
            if column_name == 'Director':  # There are some co-directors which is noted with perentesis
                self.movies_n_series_table['Director'] = self.movies_n_series_table['Director'].str.replace('\(.+\)', '')
                self.movies_n_series_table = self.movies_n_series_table.join(self.extract_from_comma_sperated_strings(self.movies_n_series_table,
                                                                                                                      column_name))
            self.movies_n_series_table.drop(column_name, axis=1, inplace=True)

        return self.movies_n_series_table


if __name__ == '__main__':
    dfs = {
        'movies_n_series_df': data_loader().movies_n_series,
        'rating_df': data_loader().ratings,
        'wiki_df': data_loader().wiki
         }
    movies = MovieVectorization(**dfs)
    movies.movies_cleaning()
    movies.movies_extract_features()
    print(movies.movies_n_series_table.head())

    # movies_after_feature_extraction.to_sql(name='movies_extracted', con=engine)
