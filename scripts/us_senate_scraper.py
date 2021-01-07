# python3 scripts/us_senate_scraper.py 2020 --outpath ../../data_lake/parliament_voting_data/us_senate_voting/

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import sys
from io import StringIO
import time
import math
import re
import argparse

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('year', help='which year to scrape')
    parser.add_argument('--outpath', default = '', type=str, help='where to save the data')
    args = parser.parse_args()

    # Convert year into congress session
    congress_num = math.floor((int(args.year)-1787)/2)
    session = (int(args.year)+1)%2 + 1
    print("Congress Number {}, Session {}".format(congress_num, session))

    # Check out list of all Voting Records
    base_url = requests.get(
        "https://www.senate.gov/legislative/LIS/roll_call_lists/vote_menu_{}_{}.htm".format(congress_num, session)
    )

    soup = BeautifulSoup(base_url.content, 'html.parser')

    votes = soup.find(id = 'listOfVotes').findAll('tr')[1:]
    nb_votes = int(re.sub('\\xa0.+$', '', votes[0].findAll('td')[0].find('a').get_text()))
    print("Number of Roll Call votes: {}".format(nb_votes))

    # Scrape the vote descriptions

    columns = ['vote', 'result', 'question', 'issue', 'date']
    vote_desc = pd.DataFrame("", index=np.arange(nb_votes), columns=columns)

    vote_desc_table = soup.findAll(class_='contenttext')
    vote_desc_table

    for i in range(1, nb_votes+1):
        vote_desc['vote'][nb_votes-i]=vote_desc_table[5*i].get_text()
        vote_desc['result'][nb_votes-i]=vote_desc_table[5*i+1].get_text()
        vote_desc['question'][nb_votes-i]=vote_desc_table[5*i+2].get_text()
        vote_desc['issue'][nb_votes-i]=vote_desc_table[5*i+3].get_text()
        vote_desc['date'][nb_votes-i]=vote_desc_table[5*i+4].get_text()

    vote_desc.to_csv(
        args.outpath+"congress_{}_{}_vote_description.tsv".format(congress_num, session), sep='\t', index = False
    )

    # Scrape each vote
    all_votes = pd.DataFrame()

    for i in range(nb_votes):
        print("Scraping {} out of {} votes.".format(i+1, nb_votes), end = '\r')

        vote_num = str(i+1).zfill(5)
        headers = {'User-Agent': 'Chrome/41.0.2228.0'}
        url = (
            "https://www.senate.gov/legislative/LIS/roll_call_lists/roll_call_vote_cfm.cfm?congress={}&session={}&vote={}".format(congress_num, session, vote_num)
        )

        # While there are errors, keep trying
        j = 0
        while j<100:
            request = requests.get(url, headers=headers)
            soup = BeautifulSoup(request.content, 'html.parser')
            if soup.findAll(class_='newspaperDisplay_3column') == []:
                j += 1
                time.sleep(1)
            else:
                break

        if j==100:
            print("\nCould not parse vote {} ({}).\n".format(vote_num, url))

        else:
            vote_results = soup.findAll(class_='newspaperDisplay_3column')[0].findAll(class_='contenttext')[0].get_text()
            vote_results = vote_results.replace(", Giving Live Pair", "")
            vote_results_csv = StringIO(vote_results)
            vote_results = pd.read_csv(vote_results_csv, sep=",", header=None)
            vote_results.columns = ['senator', 'vote']
            vote_results['senator'] = vote_results['senator'].str.strip()
            vote_results['name'] = vote_results['senator'].str[:-7].str.strip()
            vote_results['party'] = vote_results['senator'].str[-5:-4].str.strip()
            vote_results['state'] = vote_results['senator'].str[-3:-1].str.strip()
            vote_results['vote_num'] = vote_num
            vote_results['vote'] = vote_results['vote'].str.strip()

            all_votes = pd.concat([all_votes, vote_results], axis=0)

    print("\nDone!")

    # If the code is stuck on a vote, try opening the vote's page on your internet browser.

    all_votes[['name', 'party', 'state', 'vote', 'vote_num']].to_csv(
        args.outpath+"congress_{}_{}_vote_outcome.tsv".format(congress_num, session), sep='\t', index = False
    )
