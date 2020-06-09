# Python Setup
import os
import sys
import requests
import logging
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import re
# from models.cleaning_utils import cleaning_vote_outcomes
from tqdm.auto import tqdm

# ################################################
# ################################################
# # Get Vote Outcomes
# ################################################
# ################################################
#
# def get_vote_outcomes(old_file=None):
#     if old_file and os.path.isfile(old_file):
#         old_data = pd.read_csv(old_file, delimiter='\t')
#         print('found %s old records' % (old_data.shape[1] - 2))
#         start = int(old_data.columns[-1].replace('vote_', '')) + 1
#     else:
#         start = 1
#
#     print("\nScrapping vote outcomes...")
#
#     # Get Number of Votes:
#     url = "http://www2.assemblee-nationale.fr/scrutins/liste/(legislature)/15/(type)/TOUS/(idDossier)/TOUS"
#     request = requests.get(url)
#     print(request)
#     soup = BeautifulSoup(request.content, 'html.parser')
#     nb_votes = soup.find(class_='scrutins').find('tbody').findAll('tr')[0].find(class_='denom').text
#     try:
#         nb_votes = int(nb_votes)
#     except:
#         nb_votes = int(nb_votes[0:-1]) # Catches exceptional case where the vote is numbered with "*" suffix
#     print("Total votes: {}".format(nb_votes))
#
#     if nb_votes < start:
#         raise RuntimeError('Invalid: total votes found on website is less than previous scrape')
#
#     # Scrape Votes:
#
#     df = pd.DataFrame()
#
#     for i in tqdm(range(start, nb_votes+1)):
#
#         # print("Scraping: {}/{}".format(i, nb_votes), end = '\r')
#
#         ind_l = []
#         party_l = []
#         vote_l = []
#
#         url = "http://www2.assemblee-nationale.fr/scrutins/detail/(legislature)/15/(num)/{}".format(i)
#         request = requests.get(url)
#         soup = BeautifulSoup(request.content, 'html.parser')
#
#         parties = soup.find(id='analyse').findAll(class_='TTgroupe topmargin-lg')
#
#         for party in parties:
#
#             outcomes = party.findAll('div')
#             for outcome in outcomes:
#                 inds = outcome.find(class_='deputes')
#                 if len(inds.findAll('li'))==0:
#                     inds = [inds]
#                 else:
#                     inds = inds.findAll('li')
#                 for ind in inds:
#                     ind_l.append(ind.text)
#                     party_l.append(party.find(class_='nomgroupe').text)
#                     vote_l.append(outcome.find(class_='typevote').text)
#
#         temp = pd.DataFrame({'ind':ind_l, 'party_temp':party_l, 'vote_{}'.format(i):vote_l})
#
#         # Cleaning:
#         temp = cleaning_vote_outcomes(temp, i)
#
#         if df.empty:
#             df = temp
#         else:
#             df = pd.merge(df, temp, how='outer', on=['ind', 'party'])
#
#     print("\nDone !")
#
#     if old_file and os.path.isfile(old_file):
#         if old_data.shape[0] < df.shape[0]:
#             logging.error('new data has more number of individuals (%s) than before (%s)' % (df.shape[0], old_data.shape[0]))
#             import pdb; pdb.set_trace()
#         else: # it's fine if less ppl votes on new polls because not all ppl vote on all polls everytime
#             df = pd.merge(old_data, df, on=['ind', 'party'], how='left')
#             # left merge discards new individuals, do a full scrape to fix it
#
#     return df

class FranceAssembleeScrapper():

    def __init__(self, legislature_num=15, level='debug'):
        self.nums = []
        self.descs = []
        self.failed_num = []
        self.failed_url = []
        self.already_covered_url = {}
        self.nb_pages = None
        self.nb_votes = None
        self.legislature_num = legislature_num

        if level == 'debug':
            logging.basicConfig(level=logging.DEBUG)
        elif level == 'info':
            logging.basicConfig(level=logging.INFO)
        else:
            logging.basicConfig(level=logging.WARNING)

    @staticmethod
    def _get_whole_file_in_one_page(url):
        # change url to get to page with full text
        for pos in range(len(url)-1, -1, -1):
            if url[pos] == '/':
                return url[:pos] + '/alt' + url[pos:]

    @staticmethod
    def _scrape_full_desc(vote_url):
        #get full text of legislation from url

        vote_url = FullDescScrapper._get_whole_file_in_one_page(vote_url)
        # check url; some polls has no full text, skip them
        if vote_url.startswith('http'):
            current_vote = BeautifulSoup(requests.get(vote_url).content, 'html.parser')
            vote_text = current_vote.find(class_='card-body').find_all('p')
            vote_text = ''.join([p.text.replace('\xa0', ' ').replace('  ', ' ') for p in vote_text]).strip()
            return vote_text

        else:
            return None

    @staticmethod
    def _change_url_for_old_page(vote_url):
        vote_url = vote_url.replace('.asp', '')
        if 'dyn' in vote_url:
            return vote_url
        else:
            updated_url = vote_url.split('.fr')
            updated_url.insert(1, '.fr/dyn')
            return ''.join(updated_url)

    def _find_number_votes(self):
        url = "http://www2.assemblee-nationale.fr/scrutins/liste/(legislature)/{}/(type)/TOUS/(idDossier)/TOUS".format(self.legislature_num)
        request = requests.get(url)
        soup = BeautifulSoup(request.content, 'html.parser')
        nb_votes = soup.find(class_='scrutins').find('tbody').findAll('tr')[0].find(class_='denom').text
        try:
            nb_votes = int(nb_votes)
        except:
            nb_votes = int(nb_votes[0:-1]) # Catches exceptional case where the vote is numbered with "*" suffix
        self.nb_votes = nb_votes

    def get_num_pages(self):
        # Get Number of Pages:
        url = "http://www2.assemblee-nationale.fr/scrutins/liste/(legislature)/{}/(type)/TOUS/(idDossier)/TOUS".format(self.legislature_num)
        request = requests.get(url)
        soup = BeautifulSoup(request.content, 'html.parser')
        nb_pages = soup.find(class_ = 'bottommargin pagination-bootstrap pagination-right pagination-small').findAll('li')[-2].text
        self.nb_pages = int(nb_pages)
        print("Number of pages of votes: {}".format(nb_pages))

    def record_vote_data(self, vote_num, vote_text, vote_url):
        logging.info('success with', vote_num)
        self.nums.append(vote_num)
        self.descs.append(vote_text)
        self.already_covered_url[vote_url] = vote_text

    def record_failure(self, vote_num, vote_url, error):
        logging.debug('failure with', vote_num, 'because of', error)
        self.failed_num.append(vote_num)
        self.failed_url.append(vote_url)
        self.already_covered_url[vote_url] = False

    def scrape_vote_outcome(self, i):

        url = "http://www2.assemblee-nationale.fr/scrutins/detail/(legislature)/{}/(num)/{}".format(self.legislature_num, i)
        request = requests.get(url)
        soup = BeautifulSoup(request.content, 'html.parser')

        parties = soup.find(id='analyse').findAll(class_='TTgroupe topmargin-lg')

        ind_l, party_l, vote_l = [], [], []
        for party in parties:

            outcomes = party.findAll('div')
            for outcome in outcomes:
                inds = outcome.find(class_='deputes')
                if len(inds.findAll('li'))==0:
                    inds = [inds]
                else:
                    inds = inds.findAll('li')
                for ind in inds:
                    ind_l.append(ind.text)
                    party_l.append(party.find(class_='nomgroupe').text)
                    vote_l.append(outcome.find(class_='typevote').text)

        party_l = [re.sub(r'\([^)]*\)$', '', elem).strip() for elem in party_l]
        vote_l = [re.sub(r'\: \d+$', '', elem).strip() for elem in vote_l]
        ind_l = [elem.strip() for elem in ind_l]

        temp = pd.DataFrame({'ind':ind_l, 'party':party_l, 'vote':vote_l})
        temp['vote_num'] = i
        return temp

    def get_outcome(self):

        self._find_number_votes()

        self.outcome_df = pd.DataFrame()
        for i in tqdm(range(1, self.nb_votes+1)):
            temp = self.scrape_vote_outcome(i)
            self.outcome_df = pd.concat([self.outcome_df, temp])
        return self.outcome_df

    def scrape_desc(self, page=None, vote_num=None, debug=False):
        if page is None and vote_num is None:
            raise ValueError('page and vote number both empty')

        if page:
            offset = 100*(page-1)
        else:
            offset = self.nb_votes - vote_num

        # get list of poll numbers and urls on page
        current_page = requests.get(
            "http://www2.assemblee-nationale.fr/scrutins/liste/(offset)/{}/(legislature)/15/(type)/TOUS/(idDossier)/TOUS".format(offset))
        current_page = BeautifulSoup(current_page.content, 'html.parser')
        all_votes = current_page.find_all(class_='desc')
        all_num = [num.text for num in current_page.find_all(class_='denom')[1:]] # first one is 'poll number'

        if all([n in self.nums for n in all_num]): # have gone through entire page
            return

        for idx in range(len(all_votes)):
            # for each vote
            vote_num = all_num[idx]
            if vote_num in self.nums:
                continue
            # if vote_num == '600':
            #     import pdb; pdb.set_trace()

            vote_url = all_votes[idx].find('a').attrs['href']

            if vote_url in self.already_covered_url:
                vote_text = self.already_covered_url[vote_url]
                # import pdb; pdb.set_trace()
                if vote_text:
                    self.record_vote_data(vote_num, vote_text, vote_url)
                else:
                    self.record_failure(vote_num, vote_url, 'failed already')

            else:
                logging.info('scraping vote number', vote_num)
                try:
                    vote_text = FullDescScrapper._scrape_full_desc(vote_url)
                    if vote_text:
                        self.record_vote_data(vote_num, vote_text, vote_url)
                    else:
                        # throw error to get into catch phrase and try other method
                        raise RuntimeError('empty result returned')

                except Exception as e:
                    try:
                        # if failed, change the url to historical format and try again
                        new_vote_url = FullDescScrapper._change_url_for_old_page(vote_url)
                        vote_text = FullDescScrapper._scrape_full_desc(new_vote_url)
                        if vote_text:
                            self.record_vote_data(vote_num, vote_text, vote_url)
                        else:
                            self.record_failure(vote_num, vote_url, e)

                    except Exception as e:
                        # give up, debug later
                        self.record_failure(vote_num, vote_url, e)
                        # import pdb; pdb.set_trace()
                        if debug:
                            print(vote_num, e, vote_url)

    def debug(self):
        return self.failed_num, self.failed_url

    @staticmethod
    def sort_df(df):
        df['temp'] = df['num'].apply(lambda x: int(x) if x[-1]!='*' else int(x[:-1]))
        df.sort_values('temp', inplace=True)
        df = df.drop('temp', axis=1)
        return df

    def get_vote_information(self, old_file=None, debug=False, continue_from_vote_num=None):
        if old_file and os.path.isfile(old_file):
            old_data = pd.read_csv(old_file, delimiter='\t')
            print('found %s old records' % old_data.shape[0])
            self.nums.extend(old_data.num.tolist())

        else:
            print('no previous input, starting afresh')

        print("\nScrapping vote outcomes...")
        self.scrape_outcomes()

        print("\nScrapping vote descriptions...")
        if continue_from_vote_num is None:
            if len(self.already_covered_url) > 0:
                raise RuntimeError('cache (already_covered_url) is not clean')

            self.get_num_pages()
            if type(self.nb_pages) is not int:
                raise ValueError('invalid number of pages: %s' % self.nb_pages)

            for page in tqdm(range(1, self.nb_pages+1)):
                self.scrape_description(page=page, debug=debug)

        else:
            self._find_number_votes()

            for vote_num in tqdm(range(continue_from_vote_num, self.nb_votes+1, 100)):
                self.scrape_description(vote_num=vote_num, debug=debug)

        if len(self.descs) == 0:
            print('no new result, exit')
            sys.exit(1)

        if old_file and os.path.isfile(old_file):
            df = pd.DataFrame({'num':self.nums[old_data.shape[0]:], 'desc':self.descs})
            df['desc'] = df['desc'].str.replace(r'\r', '')
            df = pd.concat([df, old_data])
            df = FullDescScrapper.sort_df(df)
        else:
            df = pd.DataFrame({'num':self.nums, 'desc':self.descs})
            df['desc'] = df['desc'].str.replace(r'\r', '')

        print('number of failed tasks:', len(self.failed_num))
        return df
