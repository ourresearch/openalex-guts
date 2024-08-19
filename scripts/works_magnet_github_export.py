# -*- coding: utf-8 -*-

DESCRIPTION = """Get a table of works-magnet corrections from Github issues."""

import sys, os, time
from pathlib import Path
from datetime import datetime
from timeit import default_timer as timer
try:
    from humanfriendly import format_timespan
except ImportError:
    def format_timespan(seconds):
        return "{:.2f} seconds".format(seconds)
import requests
import pandas as pd

import logging
root_logger = logging.getLogger()
logger = root_logger.getChild(__name__)

GITHUB_PAT = os.getenv('GITHUB_PAT')

fields = ['raw_affiliation_name', 'new_rors', 'previous_rors', 'works_examples', 'contact']
def parse_body(e):
    elt = {}
    for lx, line in enumerate(e.split('\n')):
        for f in fields:
            if f+':' in line:
                elt[f] = line.replace(f+':', '').strip()
    if elt.get('contact'):
        elt['contact_domain'] = elt['contact'].split('@')[-1].strip()
    new_rors_set = set(ror for ror in elt.get('new_rors', '').split(';') if ror)
    previous_rors_set = set(ror for ror in elt.get('previous_rors', '').split(';') if ror)
    added_rors = new_rors_set.difference(previous_rors_set)
    removed_rors = previous_rors_set.difference(new_rors_set)
    elt['has_added_rors'] = len(added_rors) > 0
    elt['has_removed_rors'] = len(removed_rors) > 0
    elt['added_rors'] = ';'.join(added_rors)
    elt['removed_rors'] = ';'.join(removed_rors)
    return elt

def main(args):
    headers = {
        "Accept":  "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_PAT}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    issues = []
    url = 'https://api.github.com/repos/dataesr/openalex-affiliations/issues'
    logger.info(f"Requesting issues from the github API: ({url})")
    for p in range(1, 100000):
        params = {'page': p}
        res = requests.get(url, params=params, headers=headers)
        current_issues=res.json()
        if len(current_issues):
            issues += current_issues
        else:
            break
    logger.info(f"{len(issues)} have been retrieved")

    for issue in issues:
        parsed = parse_body(issue['body'])
        issue.update(parsed)
        issue_id = issue['url'].split('/')[-1]
        issue['issue_number'] = int(issue_id)
    df_issues = pd.DataFrame(issues)
    df_issues[fields+['issue_number']].sort_values('issue_number')

    cols = ['issue_number', 'has_added_rors', 'has_removed_rors', 'new_rors', 'previous_rors', 'raw_affiliation_name', 'added_rors', 'removed_rors', 'works_examples', 'contact', 'contact_domain']

    logger.info(f"Writing output csv to: {args.output}")
    df_issues[cols].to_csv(args.output, index=False)


if __name__ == "__main__":
    total_start = timer()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(fmt="%(asctime)s %(name)s.%(lineno)d %(levelname)s : %(message)s", datefmt="%H:%M:%S"))
    root_logger.addHandler(handler)
    root_logger.setLevel(logging.INFO)
    logger.info(" ".join(sys.argv))
    logger.info( '{:%Y-%m-%d %H:%M:%S}'.format(datetime.now()) )
    logger.info("pid: {}".format(os.getpid()))
    import argparse
    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument("output", help="path to output CSV file")
    parser.add_argument("--debug", action='store_true', help="output debugging info")
    global args
    args = parser.parse_args()
    if args.debug:
        root_logger.setLevel(logging.DEBUG)
        logger.debug('debug mode is on')
    main(args)
    total_end = timer()
    logger.info('all finished. total time: {}'.format(format_timespan(total_end-total_start)))