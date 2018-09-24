import os
import re
import json
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer


class DataLoader(object):
    def __init__(self):
        self.movies_n_series, self.ratings = self.load_imdb()
        self.wiki = self.load_wiki()

    @staticmethod
    def open_json(full_file_path):
        with open(full_file_path, 'r') as jfile:
            return json.load(jfile)

    def load_from_path(self, path_for_data_files):
        full_path_file_names = list(map(lambda p: os.path.join(path_for_data_files, p), os.listdir(path_for_data_files)))
        only_valid_files = list(filter(lambda file: '.json' in file and not ('!' in file or '?' in file),
                                       full_path_file_names))
        return list(map(self.open_json, only_valid_files))

    def load_imdb(self):
        movies_n_series = []
        ratings = []
        path_for_movie_jsons = 'raw_data'
        for movie_details in self.load_from_path(path_for_movie_jsons):
                if 'Ratings' in movie_details:
                    for rating in movie_details['Ratings']:
                        rating['imdbID'] = movie_details['imdbID']

                    ratings += movie_details.pop('Ratings')
                movies_n_series.append(movie_details)

        return pd.DataFrame(movies_n_series), pd.DataFrame(ratings)

    def load_wiki(self):
        path_for_wiki_jsons = os.path.join('raw_data', 'wiki_data')
        wiki = self.load_from_path(path_for_wiki_jsons)

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
        self.movies_n_series_table.drop(['DVD', 'Website', 'Response', 'Poster', 'Released'], axis=1, inplace=True)
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

    def outsource_ratings_features(self):
        def rating_cleaner(rating_value):
            if '/' in rating_value:
                place = rating_value.find('/')
                left = rating_value[:place]
                right = rating_value[place + 1:]
                return float(left) / float(right)
            if '%' in rating_value:
                return float(rating_value.replace('%', '')) / 100

        outsource_ratings_features_list = []
        self.ratings.set_index('imdbID', inplace=True)

        # feature of how much outsource ratings where
        index_count = self.ratings.index.value_counts().rename('outsource_rating_count')
        outsource_ratings_features_list.append(index_count)

        # feature of which source gave each movie a rating
        rating_source_dummies = pd.get_dummies(self.ratings['Source'])
        rating_source_dummies = rating_source_dummies.groupby(rating_source_dummies.index).sum()
        rating_source_dummies.columns = [f'rating_outsource_{col}' for col in rating_source_dummies.columns]
        outsource_ratings_features_list.append(rating_source_dummies)

        # features of statistics about the outsource_ratings given each movie:
        outsource_ratings_value = self.ratings.Value.apply(rating_cleaner)
        outsource_ratings = outsource_ratings_value.groupby(outsource_ratings_value.index).agg(['min', 'max',
                                                                                                'mean', 'median'])
        outsource_ratings.columns = [f'outsource_rating_score_{col}' for col in outsource_ratings.columns]
        outsource_ratings_features_list.append(outsource_ratings)

        return outsource_ratings_features_list

    def movies_extract_features(self):
        self.movies_n_series_table.set_index('imdbID', inplace=True)
        self.movies_n_series_table = self.movies_n_series_table.join(self.wiki_data.set_index('imdb_id')['text'])

        for outsource_ratings_feature in self.outsource_ratings_features():
            self.movies_n_series_table = self.movies_n_series_table.join(outsource_ratings_feature)

        self.movies_n_series_table['Awards_wins'], self.movies_n_series_table['Awards_nominate'] =\
            zip(*self.movies_n_series_table['Awards'].apply(self.extract_win_and_nominated_awards))

        self.movies_n_series_table.drop('Awards', axis=1, inplace=True)

        for column_name in ['Country', 'Director', 'Genre', 'Language', 'Actors', 'Production', 'Writer']:
            if column_name == 'Director':  # There are some co-directors which is noted with parentheses
                self.movies_n_series_table['Director'] = self.movies_n_series_table['Director'].str.replace('\(.+\)', '')
                self.movies_n_series_table = self.movies_n_series_table.join(
                    self.extract_from_comma_separated_strings(self.movies_n_series_table, column_name)
                )
            self.movies_n_series_table.drop(column_name, axis=1, inplace=True)

        return self.movies_n_series_table


if __name__ == '__main__':
    dfs = {
        'movies_n_series_df': DataLoader().movies_n_series,
        'rating_df': DataLoader().ratings,
        'wiki_df': DataLoader().wiki
         }

    movies = MovieVectorization(**dfs)
    movies.movies_cleaning()
    # todo: create for each feature a method so it will be easy with a config file to setup
    movies.movies_extract_features()
    print(movies.movies_n_series_table.head())

    # movies_after_feature_extraction.to_sql(name='movies_extracted', con=engine)
