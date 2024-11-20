import json
import os
from argparse import ArgumentParser
from datetime import datetime

from bs4 import BeautifulSoup
from sqlalchemy import any_

from models import Publisher
from models.source import Source
from app import db

import requests

from scripts.add_things_queue import enqueue_dois


def get_auth_token():
    username, password = os.environ.get('ISSN_PORTAL_CREDENTIALS').split(':')

    if not username or not password:
        raise ValueError(
            "ISSN Portal credentials not found in environment variables")

    auth_url = f"https://api.issn.org/authenticate/{username}/{password}"
    response = requests.get(auth_url, headers={"Accept": "application/json"})
    response.raise_for_status()

    return response.json()['token']


def fetch_issn_record(issn, token):
    url = f"https://api.issn.org/notice/{issn}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"JWT {token}"
    }
    params = {
        "json": "true"
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def extract_issn_from_id(id_string):
    if not isinstance(id_string, str):
        return None
    parts = id_string.split('/')
    for part in parts:
        if '-' in part and len(part) == 9:
            return part
    return None


def get_publisher_id(publisher_name):
    params = {
        'search': publisher_name
    }
    r = requests.get('https://api.openalex.org/publishers', params=params)
    r.raise_for_status()
    j = r.json()
    if not j['results']:
        return None
    return j['results'][0]['id'].split('/P')[-1]


def add_publisher(publisher_name):
    params = {'query': publisher_name}
    r = requests.get('https://api.ror.org/organizations', params=params)
    r.raise_for_status()
    j = r.json()

    if not j['items']:
        return None

    ror_obj = j['items'][0]

    p = Publisher()
    p.created_date = datetime.now()
    p.display_name = ror_obj.get('name')
    p.alternate_titles = ror_obj.get('aliases', [])
    p.country_codes = [ror_obj.get('country', {}).get('country_code')]
    p.parent_publisher = None
    p.hierarchy_level = None
    p.ror_id = ror_obj.get('id')
    p.wikidata_id = ror_obj.get('external_ids', {}).get('Wikidata', {}).get(
        'preferred')
    if p.wikidata_id:
        p.wikidata_id = 'https://www.wikipedia.org/entity/' + p.wikidata_id
    p.homepage_url = ror_obj.get('links', [None])[0]
    p.image_url = None
    p.image_thumbnail_url = None
    p.country_name = ror_obj.get('country', {}).get('country_name')

    db.session.add(p)
    db.session.commit()
    p.store()
    return p



def crossref_issn_works(issn):
    api_key = os.environ['CROSSREF_API_KEY']
    cursor = '*'
    params = {'select': 'DOI', 'cursor': cursor, 'rows': '100'}
    headers = {
        "crossref-api-key": api_key
    }
    while cursor:
        r = requests.get(f'https://api.crossref.org/journals/{issn}/works',
                         headers=headers,
                         params=params)
        r.raise_for_status()
        j = r.json()

        total_results = j['message'].get('total-results', 0)

        for result in j['message']['items']:
            yield result['DOI'], total_results

        cursor = j.get('message', {}).get('next-cursor')


def enqueue_issn_works_to_slow_queue(issn):
    chunk_size = 100
    chunk = []
    count = 0
    for i, (doi, total_results) in enumerate(crossref_issn_works(issn)):
        chunk.append(doi)
        total_works = total_results
        count += 1

        if len(chunk) >= chunk_size:
            enqueue_dois(chunk, priority=-1, fast_queue_priority=-1)
            chunk.clear()
            print(f'{count}/{total_works} works enqueued to slow queue')


def parse_journal(issn_record):
    record = None
    for item in issn_record['@graph']:
        if isinstance(item, dict) and 'mainTitle' in item:
            record = item
            break

    if not record:
        raise ValueError("Could not find main journal record in ISSN data")

    journal = {
        'display_name': record.get('mainTitle'),
        'normalized_name': record.get('mainTitle',
                                      '').lower().strip() if record.get(
            'mainTitle') else None,

        'issn': None,
        'issns': set(),

        'publisher': None,
        'original_publisher': None,

        'webpage': record.get('url'),

        'type': 'journal',

        'country': None,
        'country_code': None,

        'alternate_titles': [],
        'abbreviated_title': None
    }

    main_issn = extract_issn_from_id(record.get('@id'))
    if main_issn:
        journal['issns'].add(main_issn)
        if not journal['issn']:
            journal['issn'] = main_issn

    if 'identifiedBy' in record:
        for identifier_ref in record['identifiedBy']:
            issn = extract_issn_from_id(identifier_ref)
            if issn:
                journal['issns'].add(issn)

    for item in issn_record['@graph']:
        issn = extract_issn_from_id(item.get('@id', ''))
        if issn:
            journal['issns'].add(issn)

        if 'value' in item and isinstance(item.get('@type'), (list, str)):
            type_str = str(item.get('@type'))
            if 'ISSN' in type_str:
                value = item['value']
                if len(value) == 8 or (
                        len(value) == 9 and value[-1].upper() == 'X'):
                    journal['issns'].add(value)

    if 'publisher' in record:
        publisher_id = record['publisher']
        for item in issn_record['@graph']:
            if item.get('@id') == publisher_id:
                journal['publisher'] = item.get('name')
                journal['original_publisher'] = item.get('name')
                break

    if not journal['publisher'] and 'provisionActivityStatement' in record:
        journal['publisher'] = record['provisionActivityStatement'].split()[-1]
        journal['original_publisher'] = \
            record['provisionActivityStatement'].split()[-1]

    if 'name' in record:
        names = record['name'] if isinstance(record['name'], list) else [
            record['name']]
        journal['alternate_titles'] = [name for name in names if
                                       name != journal['display_name']]

    if 'alternateName' in record:
        journal['abbreviated_title'] = record['alternateName']

    if 'spatial' in record:
        spatial_ref = record['spatial']
        for item in issn_record['@graph']:
            if item.get('@id') == spatial_ref and 'label' in item:
                journal['country'] = item['label']
                if isinstance(spatial_ref, str):
                    journal['country_code'] = spatial_ref.split('/')[-1].upper()
                break

    journal['issns'] = list(journal['issns'])

    if not journal['issn'] and journal['issns']:
        journal['issn'] = journal['issns'][0]

    return journal


def check_issn_exists(issn: str):
    existing_journal = db.session.query(Source).filter(
        issn == any_(Source.issns_text_array)
    ).first()

    return existing_journal is not None, existing_journal


def doaj_response(issn: str):
    r = requests.get(f'https://doaj.org/toc/{issn}')
    if not r.ok:
        return {'is_in_doaj': False, 'apc_found': False}
    soup = BeautifulSoup(r.text, parser='lxml', features='lxml')
    zero_apc_tag = soup.find('article', lambda tag: tag.text.contains(
        'no publication fees'))
    if zero_apc_tag:
        apc_prices = {'price': 0, 'currency': 'USD'}
        return {'is_in_doaj': True, 'apc_prices': apc_prices, 'apc_usd': 0,
                'apc_found': True}
    apc_tag = soup.find('article',
                        lambda tag: tag.text.contains('journal charges up to'))
    if not apc_tag:
        raise Exception('APC data not found in DOAJ')
    apc_prices = []
    apc_list = apc_tag.find('li')
    if not apc_list:
        raise Exception('APC data not found in DOAJ')
    for tag in apc_list:
        price, currency = tag.text.split()
        apc_prices.append({'price': int(price), 'currency': currency})
    return {'is_in_doaj': True,
            'apc_prices': apc_prices,
            'apc_usd':
                [price for price in apc_prices if price['currency'] == 'USD'][
                    0],
            'apc_found': True}


def ingest_issn(issn: str = None, publisher_id=None, is_core=False, is_oa=False,
                overwrite_journal_id=None) -> tuple[Source, str]:
    if overwrite_journal_id:
        existing_journal = db.session.query(Source).filter(
            Source.id == overwrite_journal_id).first()
        if not existing_journal:
            return None, f'Journal with ID {overwrite_journal_id} not found'
        issn = existing_journal.issn
        if not issn:
            return None, f'Journal with ID {overwrite_journal_id} has no ISSN'
    elif not issn:
        return None, 'ISSN must be provided when not overwriting'

    token = get_auth_token()
    issn_record = fetch_issn_record(issn, token)
    parsed_journal = parse_journal(issn_record)

    if not overwrite_journal_id:
        for issn_to_check in parsed_journal['issns']:
            exists, existing_journal = check_issn_exists(issn_to_check)
            if exists:
                return existing_journal, f'Journal already exists: {existing_journal}'

    doaj_data = doaj_response(issn)
    if not publisher_id:
        publisher_id = get_publisher_id(
            publisher_name=parsed_journal['publisher'])
        if not publisher_id:
            if p := add_publisher(parsed_journal['publisher']):
                publisher_id = p.publisher_id
                print(f'New Publisher created: {p}')
            else:
                return None, f'Publisher "{parsed_journal["publisher"]}" not found in OpenAlex. Unable to ingest.'

    journal_data = {
        **doaj_data,
        'is_core': is_core,
        'is_oa': doaj_data['is_in_doaj'] or is_oa,
        'display_name': parsed_journal['display_name'],
        'normalized_name': parsed_journal['normalized_name'],
        'issn': parsed_journal['issn'],
        'issns': json.dumps(parsed_journal['issns']),
        'issns_text_array': parsed_journal['issns'],
        'publisher': parsed_journal['publisher'],
        'publisher_id': publisher_id,
        'original_publisher': parsed_journal['publisher'],
        'webpage': parsed_journal['webpage'],
        'type': parsed_journal['type'],
        'country': parsed_journal['country'],
        'country_code': parsed_journal['country_code'],
        'alternate_titles': parsed_journal['alternate_titles'],
        'abbreviated_title': parsed_journal['abbreviated_title'],
        'updated_date': datetime.now()
    }

    try:
        if overwrite_journal_id:
            for key, value in journal_data.items():
                setattr(existing_journal, key, value)

            db.session.commit()
            return existing_journal, None
        else:
            # Create new journal
            journal_data['created_date'] = datetime.now()
            new_journal = Source(**journal_data)
            db.session.add(new_journal)
            db.session.commit()
            print('Enqueueing works with matching ISSN to slow queue')
            enqueue_issn_works_to_slow_queue(issn)
            return new_journal, None

    except Exception as e:
        db.session.rollback()
        raise Exception(
            f"Failed to {'update' if overwrite_journal_id else 'insert'} journal record: {str(e)}") from e


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--issn', type=str,
                        help='ISSN to ingest (not required when using --overwrite_journal_id)',
                        required=False)
    parser.add_argument('--publisher_id', type=int,
                        help='Publisher ID override', required=False,
                        default=None)
    parser.add_argument('--is_core', type=bool, help='is_core override',
                        required=False, default=False)
    parser.add_argument('--is_oa', type=bool, help='is_oa override',
                        required=False, default=False)
    parser.add_argument('--overwrite_journal_id', type=int,
                        help='Journal ID to overwrite', required=False,
                        default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.issn and not args.overwrite_journal_id:
        print("Error: Either --issn or --overwrite_journal_id must be provided")
        return

    source, error = ingest_issn(args.issn, args.publisher_id, args.is_core,
                                args.is_oa, args.overwrite_journal_id)
    if error:
        print(error)
        return
    print(
        f'Journal {"updated" if args.overwrite_journal_id else "created"}: {source}')


if __name__ == '__main__':
    main()