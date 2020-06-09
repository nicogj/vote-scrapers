import os
import argparse
import pandas as pd
from utils.scrapers import FranceAssembleeScrapper

OUT_PATH = '/Users/nico/Dropbox (MIT)/data_lake/parliament_voting_data/france_assemblee/'

VOTE_OUTCOME_FILE_PATH = 'voting_data.tsv'
VOTE_DESC_FILE_PATH = 'vote_description.tsv'
FULL_DESC_FILE_PATH = 'vote_full_description.tsv'

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('legislature_num',
                        help='which legislature to scrape')
    # parser.add_argument('--outcome', action='store_true',
    #                     help='scrape vote outcome')
    # parser.add_argument('--desc', action='store_true',
    #                     help='scrape vote description')
    # parser.add_argument('--full_desc', action='store_true',
    #                     help='scrape full description')
    # parser.add_argument('--debug', action='store_true',
    #                     help='whether to show detailed debugging message')
    # parser.add_argument('--info', action='store_true',
    #                     help='whether to show most detailed message')
    # parser.add_argument('--start_from', '-s', type=int,
    #                     help='optionally, where to start scraping full description from')
    # parser.add_argument('--update', action='store_true',
    #                     help='only incrementally update new votes instead of scraping everything again')
    args = parser.parse_args()

    if not any(vars(args).values()):
        # person is not using argparse, keep old default behavior and run all tasks
        not_set_up = True
    else:
        not_set_up = False

    # # Run Scraper
    # if args.outcome or not_set_up:
    #     if args.update:
    #         df = get_vote_outcomes(old_file=VOTE_OUTCOME_FILE_PATH)
    #     else:
    #         df = get_vote_outcomes()
    #     df.to_csv(VOTE_OUTCOME_FILE_PATH, sep='\t', index=False)
    #
    # if args.desc or not_set_up:
    #     if args.update:
    #         desc = get_vote_descriptions(old_file=VOTE_DESC_FILE_PATH)
    #     else:
    #         desc = get_vote_descriptions()
    #     desc.to_csv(VOTE_DESC_FILE_PATH, sep='\t', index=False)

    # if args.full_desc or not_set_up:

    scraper = FranceAssembleeScrapper()
    # if args.debug:
    #     level = 'debug'
    # elif args.info:
    #     level = 'info'
    # else:
    #     level = 'warning'
    # full_desc = scraper.get_vote_full_descriptions(old_file=FULL_DESC_FILE_PATH, level=level, continue_from_vote_num=args.start_from)
    # full_desc.to_csv(FULL_DESC_FILE_PATH, sep='\t', index=False)

    outcome = scraper.get_outcome()
    outcome.to_csv(OUT_PATH+'legislature_{}_vote_outcome.tsv'.format(args.legislature_num), sep='\t', index=False)
