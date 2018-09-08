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

if __name__ == '__main__':
    # links = [
    #     'https://www.imdb.com/search/title?genres=drama&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=family&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=fantasy&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=film_noir&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&ref_=chttp_gnr_10',
    #     'https://www.imdb.com/search/title?genres=history&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=horror&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=music&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=musical&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=mystery&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=romance&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=sci_fi&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt'
    #     'https://www.imdb.com/search/title?genres=sport&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={]&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=thriller&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=war&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt',
    #     'https://www.imdb.com/search/title?genres=western&sort=user_rating,desc&title_type=feature&num_votes=25000,&pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=5aab685f-35eb-40f3-95f7-c53f09d542c3&pf_rd_r=VTZ711NYM2Q1KSKFRENE&pf_rd_s=right-6&pf_rd_t=15506&pf_rd_i=top&page={}&ref_=adv_nxt'
    #          ]
    # for link in links:
    #     for num in range(1, 48):
    #         print('page', num)
    #         url = link.format(num)
    #         IMDBApiExtractor().get_and_save_from_html_page(url)

    movie_name = 'TheGodFather1995'
    PARAMS = {
        'action': 'query',
        'format': 'json',
        'list': 'search',
        'srsearch': movie_name
    }
    r = requests.get(url=r'https://en.wikipedia.org/w/api.php', params=PARAMS)
    # for movie in r.json()["query"]["search"]:
    #     page_id = movie["pageid"]

    page_id = r.json()["query"]["search"][0]["pageid"]  # the first one is the closest?

    PARAMS = {
        'action': 'parse',
        'pageid': page_id,
        'format': 'json',
        'prop': 'text'
    }
    r = requests.get(url=r'https://en.wikipedia.org/w/api.php', params=PARAMS)
    print(r.json()['parse']['text']['*'])
    # soup = BeautifulSoup(r.json()['parse']['text']['*'], 'html.parser')
    # print(soup.get_text())