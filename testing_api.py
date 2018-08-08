import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import json

def get_movie_ids(url):
    page = requests.get(url=url)
    soup = BeautifulSoup(page.text, 'html.parser')

    ids = [id for link in soup.find_all('a') for id in re.findall('tt\d+', str(link))]
    return set(ids)


def extract_data(ids_to_query):
    user_api_key = '93359a2c'
    dfs = []
    for i, movie_id in enumerate(ids_to_query):
        r = requests.get('http://www.omdbapi.com/?i={}&apikey={}&?plot=full'.format(movie_id, user_api_key))
        response = r.json()
        dfs.append(pd.DataFrame.from_dict(response))
        print('Loaded:')
        print('{}/{}'.format(i + 1, len(ids_to_query)))
    return pd.concat(dfs)


if __name__ == '__main__':
    ids = get_movie_ids('https://www.imdb.com/chart/top?ref_=nv_mv_250')

    total_movies = extract_data(ids)
    total_movies.to_excel(r'top250.xlsx')