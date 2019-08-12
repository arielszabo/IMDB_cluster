import requests
import re
import os
from bs4 import BeautifulSoup
import json
import glob
import time


class IMDBApiExtractor(object):
    def __init__(self, user_api_key='93359a2c'):
        self.user_api_key = user_api_key
        self.existing_movie_ids = self._load_existing_movie_ids_from_raw_json()

    @staticmethod
    def _load_existing_movie_ids_from_raw_json():
        """
        Find all the movie ids based already extracted json files in the 'raw_data'.
        :return: A list of existing movie ids' based on the json file names.
        """
        return list(map(lambda name: re.search(r'tt\d+', name).group(0), glob.glob('raw_data/*.json')))

    def _get_movie_ids(self, html_url):
        """
        Extract movies' id in the html from the given url.
        :param [str] html_url: a url from to extract movies by their id
        :return: A list of all movie ids which have not already been extracted.
        """
        page = requests.get(url=html_url)
        soup = BeautifulSoup(page.text, 'html.parser')

        ids = []
        for link in soup.find_all('a'):
            for id in re.findall(r'tt\d+', str(link)):
                ids.append(id)

        return set(ids).difference(self.existing_movie_ids)

    def _extract_data(self, ids_to_query):
        """
        Query and save movie data. Alert and raise errors if we reach the request limit or if the 'Response' is false.
        :param [list] ids_to_query: A list of movie ids to query the IMDB API
        :return: None, save the data json files
        """
        print('Extract {} movie data:'.format(len(ids_to_query)))
        print(ids_to_query)
        for i, movie_id in enumerate(ids_to_query):
            response = requests.get('http://www.omdbapi.com/?i={}&apikey={}&?plot=full'.format(movie_id,
                                                                                               self.user_api_key))

            if response.json() == {"Error": "Request limit reached!", "Response": "False"}:
                print("Request limit reached! Lets wait 24 hours")
                time.sleep(86500)
                response = requests.get('http://www.omdbapi.com/?i={}&apikey={}&?plot=full'.format(movie_id,
                                                                                                   self.user_api_key))
                # raise TypeError("Request limit reached!")

            if response.json()['Response'] == 'False':
                print("Response == False ? at {}".format(movie_id))
                continue
                # raise ValueError("Response == False ? at {}".format(movie_id))

            with open(os.path.join('raw_data', '{}.json'.format(movie_id)), 'w') as j_file:
                json.dump(response.json(), j_file)

            percent_queried = 100*(i + 1)/len(ids_to_query)
            print('Finished: {}%'.format(round(percent_queried, 2)))

    def get_and_save_from_html_page(self, url):
        """
        Extract and save movies' data json based on movie ids'extracted from the given url's html.
        :param [str] url: a url from to extract movies by their id
        :return: None, saves the jsons
        """
        movie_ids = self._get_movie_ids(html_url=url)
        if movie_ids == set([]):
            return None
        self._extract_data(movie_ids)


class WikiApiExtractor(object):
    def __init__(self):
        self.api_url = r'https://en.wikipedia.org/w/api.php'

    def get_page_id_by_text_search(self, text_to_search_for):
        if len(text_to_search_for) > 300: # WIKI Search request have a maximum allowed length of 300
            text_to_search_for = text_to_search_for[:300]
        get_params = {
            'action': 'query',
            'format': 'json',
            'list': 'search',
            'srsearch': text_to_search_for
        }
        response = requests.get(url=self.api_url, params=get_params)

        if int(response.status_code) != 200:
            print(f'The request for "{text_to_search_for}" returned with status_code: {response.status_code}')
            # todo: something better
            return None

        if response.json()['query']['searchinfo']['totalhits'] == 0:
            print(f'"{text_to_search_for}" have no results')
            return None  # todo: something better

        if 'error' in response.json():
            print(f'"{text_to_search_for}" had an error')
            return None  # todo: something better

        return response.json()["query"]["search"][0]["pageid"]  # the first one is the best

    def extract_text_first_section(self, page_id):
        params = {
            'action': 'query',
            'format': 'json',
            'prop': 'extracts',
            'exintro': 'True',
            'pageids': page_id
        }
        response = requests.get(url=self.api_url, params=params)

        if int(response.status_code) != 200:
            assert ValueError(
                f'The request for "{page_id}" returned with status_code: {response.status_code}'
            ) #todo: something better

        html_content = response.json()['query']['pages'][str(page_id)]['extract']
        return BeautifulSoup(html_content, 'html.parser').get_text()

    def extract_and_save(self, movie_text, imdb_id):
        wiki_page_id = self.get_page_id_by_text_search(movie_text)
        if wiki_page_id:
            text_content = self.extract_text_first_section(wiki_page_id)

            wiki_json = {'text': text_content,
                         'wiki_page_id': wiki_page_id,
                         'imdb_id': imdb_id}

            wiki_data_path = os.path.join('raw_data', 'wiki_data')
            os.makedirs(wiki_data_path, exist_ok=True)
            with open(os.path.join(wiki_data_path, 'wiki_data_for_{}.json'.format(imdb_id)), 'w') as j_file:
                json.dump(wiki_json, j_file)


if __name__ == '__main__':
    links = [
        'https://www.imdb.com/search/title?genres=Action&explore=title_type,genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=75c37eae-37a7-4027-a7ca-3fd76067dd90&pf_rd_r=J4TJ30NNSXTY329MNNC9&pf_rd_s=center-1&pf_rd_t=15051&pf_rd_i=genre&page={}&ref_=adv_nxt',
        'https://www.imdb.com/search/title?genres=Music&explore=title_type,genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=75c37eae-37a7-4027-a7ca-3fd76067dd90&pf_rd_r=J4TJ30NNSXTY329MNNC9&pf_rd_s=center-1&pf_rd_t=15051&pf_rd_i=genre&page={}&ref_=adv_nxt',
        'https://www.imdb.com/search/title?genres=Talk-Show&explore=title_type,genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=75c37eae-37a7-4027-a7ca-3fd76067dd90&pf_rd_r=J4TJ30NNSXTY329MNNC9&pf_rd_s=center-1&pf_rd_t=15051&pf_rd_i=genre&page={}&ref_=adv_nxt',
        'https://www.imdb.com/search/title?genres=Family&explore=title_type,genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=75c37eae-37a7-4027-a7ca-3fd76067dd90&pf_rd_r=J4TJ30NNSXTY329MNNC9&pf_rd_s=center-1&pf_rd_t=15051&pf_rd_i=genre&page={}&ref_=adv_nxt',
        'https://www.imdb.com/search/title?genres=Drama&explore=title_type,genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=75c37eae-37a7-4027-a7ca-3fd76067dd90&pf_rd_r=J4TJ30NNSXTY329MNNC9&pf_rd_s=center-1&pf_rd_t=15051&pf_rd_i=genre&page={}&ref_=adv_nxt',
        'https://www.imdb.com/search/title?genres=Sci-Fi&explore=title_type,genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=75c37eae-37a7-4027-a7ca-3fd76067dd90&pf_rd_r=J4TJ30NNSXTY329MNNC9&pf_rd_s=center-1&pf_rd_t=15051&pf_rd_i=genre&page={}&ref_=adv_nxt',
        'https://www.imdb.com/search/title?genres=Game-Show&explore=title_type,genres&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=75c37eae-37a7-4027-a7ca-3fd76067dd90&pf_rd_r=J4TJ30NNSXTY329MNNC9&pf_rd_s=center-1&pf_rd_t=15051&pf_rd_i=genre&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=drama&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=family&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=fantasy&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=film_noir&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&ref_=chttp_gnr_10',
        # 'https://www.imdb.com/search/title?genres=history&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=horror&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=music&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=musical&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=mystery&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=romance&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=sci_fi&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=sport&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=thriller&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=war&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
        # 'https://www.imdb.com/search/title?genres=western&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt'
             ]
    for link in links:
        print(link[link.find('genres=')+7:link.find('&')])
        for num in range(1, 20000):
            print('page', num)
            url = link.format(num)
            IMDBApiExtractor().get_and_save_from_html_page(url)

    # Extract The wiki data for the movie
    existing_movies_wiki = [re.search(r'tt\d+\.json', wiki_json).group(0)
                            for wiki_json in os.listdir(os.path.join('raw_data', 'wiki_data'))]

    for movie_json_file_name in os.listdir('raw_data'):
        if movie_json_file_name in existing_movies_wiki:
            print(f'{movie_json_file_name} exist')
            continue
        if '.json' in movie_json_file_name and not ('!' in movie_json_file_name or '?' in movie_json_file_name):
            with open(os.path.join('raw_data', movie_json_file_name), 'r') as movie_json_file:
                print(movie_json_file_name)
                movie_json = json.load(movie_json_file)

                query_info = [movie_json['Title'], movie_json['Year'], movie_json['Type'], movie_json['Director']]
                imdbid = movie_json['imdbID']
                WikiApiExtractor().extract_and_save(movie_text=' '.join(query_info),
                                                    imdb_id=imdbid)
