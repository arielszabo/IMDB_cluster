import requests
import re
import os
from bs4 import BeautifulSoup
import json
import glob

class IMDBApiExtractor(object):
    def __init__(self):
        self.user_api_key = '93359a2c'
        self.existing_movie_ids = self._load_existing_movie_ids

    def _load_existing_movie_ids(self):
        def find_id_in_name(name):
            return re.match(r'tt\d+', name).group(0)

        return list(map(find_id_in_name, glob.glob('data/*.json')))


    def get_movie_ids(self, html_url):
        page = requests.get(url=html_url)
        soup = BeautifulSoup(page.text, 'html.parser')

        ids = [id for link in soup.find_all('a') for id in re.findall('tt\d+', str(link))]
        return set(ids).difference(self.existing_movie_ids)

    def extract_data(self, ids_to_query):
        for i, movie_id in enumerate(ids_to_query):
            response = requests.get('http://www.omdbapi.com/?i={}&apikey={}&?plot=full'.format(movie_id, self.user_api_key))
            with open(os.path.join('data', '{}.json'.format(movie_id), 'w')) as j_file:
                json.dump(response.json(), j_file)

            percent_queried = 100*(i + 1)/len(ids_to_query)
            print('Loaded: {}%'.format(round(percent_queried, 2)), end='\r')


if __name__ == '__main__':
    for num in range(1, 25):
        name = r'top_action_page_{}'.format(num)
        url = r'https://www.imdb.com/search/title?genres=action&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=A6GFCX6H09E3SXSYTG8M&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt'.format(num)
        extractor = IMDBApiExtractor()
        ids = extractor.get_movie_ids(url)
        if ids == set([]):
            continue
        extractor.extract_data(ids)