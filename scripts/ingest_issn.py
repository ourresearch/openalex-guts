import json
import os
import time
from argparse import ArgumentParser
from datetime import datetime
from threading import Thread

from bs4 import BeautifulSoup
from sqlalchemy import any_

from models import Publisher
from models.source import Source
from app import db

import requests

from util import normalize_title_like_sql
from .journal_issn_util import enqueue_issn_works_to_slow_queue


def get_auth_token():
    username, password = os.environ.get('ISSN_PORTAL_CREDENTIALS').split(':')

    if not username or not password:
        raise ValueError(
            "ISSN Portal credentials not found in environment variables")

    auth_url = f"https://api.issn.org/authenticate/{username}/{password}"
    response = requests.get(auth_url, headers={"Accept": "application/json"})
    response.raise_for_status()

    return response.json()['token']


TOKEN = get_auth_token()

def refresh_token():
    global TOKEN
    while True:
        time.sleep(60*30)
        print('Refreshing ISSN Portal auth token')
        TOKEN = get_auth_token()
        print('Done refreshing ISSN Portal auth token')


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
    if p := db.session.query(Publisher).filter(Publisher.display_name == publisher_name).first():
        return p.publisher_id
    params = {
        'search': publisher_name
    }
    r = requests.get('https://api.openalex.org/publishers', params=params)
    r.raise_for_status()
    j = r.json()
    if not j['results']:
        return None
    return j['results'][0]['id'].split('/P')[-1]


def is_publisher_entity(entity):
    publisher_claims = {'Q3918', 'Q2516866', 'Q45400320', 'Q2085381', 'Q3972943'}
    claims = entity.get('claims', {})
    instance_of_claims = claims.get('P31', [])

    for claim in instance_of_claims:
        mainsnak = claim.get('mainsnak', {})
        datavalue = mainsnak.get('datavalue', {})
        value = datavalue.get('value', {})

        if value.get('id') in publisher_claims:
            return True

    return False


def search_ror_api(name):
    params = {
        'query': name
    }
    response = requests.get('https://api.ror.org/organizations', params=params)
    response.raise_for_status()
    results = response.json()

    for item in results.get('items', []):
        external_ids = item.get('external_ids', {})
        wikidata = external_ids.get('Wikidata', {})
        if wikidata and wikidata.get('preferred'):
            return wikidata['preferred'].split('/')[-1]

    return None


def find_publisher_entity(name):
    params = {
        'action': 'wbsearchentities',
        'format': 'json',
        'search': name,
        'language': 'en',
        'limit': 50
    }
    response = requests.get('https://www.wikidata.org/w/api.php', params=params)
    response.raise_for_status()
    results = response.json()

    # Check each Wikidata result until we find a publisher
    if results.get('search'):
        for result in results['search']:
            entity = get_wikidata_entity(result['id'])
            publisher_exists = db.session.query(Publisher).filter(
                Publisher.wikidata_id.ilike(f'%{entity["id"]}%')).first()
            if entity and is_publisher_entity(entity) and not publisher_exists:
                return entity

    # If no valid publisher found in Wikidata search, try ROR API
    wikidata_id = None
    try:
        wikidata_id = search_ror_api(name)
    except Exception as e:
        pass
    if wikidata_id:
        entity = get_wikidata_entity(wikidata_id)
        publisher_exists = db.session.query(Publisher).filter(Publisher.wikidata_id.ilike(f'%{entity["id"]}%')).first()
        if entity and is_publisher_entity(entity) and not publisher_exists:
            return entity

    return None


def get_wikidata_entity(entity_id, props=None):
    params = {
        'action': 'wbgetentities',
        'format': 'json',
        'ids': entity_id,
        'language': 'en'
    }
    if props:
        params['props'] = props

    response = requests.get('https://www.wikidata.org/w/api.php', params=params)
    response.raise_for_status()
    data = response.json()

    if not data.get('entities') or entity_id not in data['entities']:
        return None

    return data['entities'][entity_id]


def get_claim_value(claims, property_id):
    return claims.get(property_id, [{}])[0].get('mainsnak', {}).get('datavalue',
                                                                    {}).get(
        'value')


def add_publisher(publisher_name):
    entity = find_publisher_entity(publisher_name)
    if not entity:
        return None

    claims = entity.get('claims', {})

    p = Publisher()
    p.created_date = datetime.now()
    p.display_name = entity.get('labels', {}).get('en', {}).get('value')
    p.alternate_titles = [alias['value'] for alias in
                         entity.get('aliases', {}).get('en', [])]

    country_claim = get_claim_value(claims, 'P17')
    if country_claim:
        country_id = country_claim.get('id')
        country_entity = get_wikidata_entity(country_id, 'claims|labels')

        if country_entity:
            country_claims = country_entity.get('claims', {})
            iso_code = get_claim_value(country_claims, 'P298')
            if iso_code:
                p.country_codes = [iso_code]
                p.country_code = iso_code
            else:
                p.country_codes = []

            p.country_name = country_entity.get('labels', {}).get('en', {}).get('value')
    else:
        p.country_codes = []
        p.country_name = None

    p.parent_publisher = None
    p.hierarchy_level = None
    entity_id = entity['id']
    p.wikidata_id = f'https://www.wikidata.org/entity/{entity_id}'

    homepage_url = get_claim_value(claims, 'P856')
    p.homepage_url = homepage_url if homepage_url else None

    p.image_url = None
    p.image_thumbnail_url = None

    ror_id = get_claim_value(claims, 'P6782')
    if ror_id:
        p.ror_id = f"https://ror.org/{ror_id}"

    db.session.add(p)
    db.session.commit()
    p.store()
    return p


def parse_journal(issn_record):
    record = None
    for item in issn_record['@graph']:
        if isinstance(item, dict) and 'mainTitle' in item:
            record = item
            break

    if not record:
        raise ValueError("Could not find main journal record in ISSN data")

    main_title = record['mainTitle']
    if isinstance(main_title, list):
        main_title = main_title[0]

    journal = {
        'display_name': main_title,
        'normalized_name': normalize_title_like_sql(main_title),

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
        if isinstance(publisher_id, list):
            publisher_id = publisher_id[0]
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
        if isinstance(record['alternateName'], list):
            journal['abbreviated_title'] = min(record['alternateName'], key=len)
        elif isinstance(record['alternateName'], str):
            journal['abbreviated_title'] = record['alternateName']

    if 'isFormatOf' in record:
        journal['issns'].add(extract_issn_from_id(record['isFormatOf']))

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
    soup = BeautifulSoup(r.content, parser='lxml', features='lxml')
    zero_apc_tag = soup.find(lambda tag: tag.name == 'article' and 'no publication fees' in tag.get_text().lower())
    if zero_apc_tag:
        apc_prices = [{'price': 0, 'currency': 'USD'}]
        return {'is_in_doaj': True, 'apc_prices': apc_prices, 'apc_usd': 0,
                'apc_found': True}
    apc_tag = soup.find(lambda tag: tag.name == 'article' and 'journal charges up to' in tag.get_text().lower())
    if not apc_tag:
        raise Exception('APC data not found in DOAJ')
    apc_prices = []
    apc_list = apc_tag.find_all('li')
    if not apc_list:
        raise Exception('APC data not found in DOAJ')
    for tag in apc_list:
        price, currency = tag.text.split()
        apc_prices.append({'price': int(price), 'currency': currency})
    return {'is_in_doaj': True,
            'apc_prices': apc_prices,
            'apc_usd':
                [price for price in apc_prices if price['currency'] == 'USD'][
                    0]['price'],
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

    issn_record = fetch_issn_record(issn, TOKEN)
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
        if publisher_id is None:
            if p := add_publisher(parsed_journal['publisher']):
                publisher_id = p.publisher_id
                print(f'New Publisher created: {p}')
            else:
                print(f'Publisher "{parsed_journal["publisher"]}" not found. Creating source without publisher.')

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
            enqueue_issn_works_to_slow_queue(issn, new_journal.journal_id)
            return new_journal, None

    except Exception as e:
        db.session.rollback()
        raise Exception(
            f"Failed to {'update' if overwrite_journal_id else 'insert'} journal record: {str(e)}") from e


def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--issn', type=str, nargs='+',
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

    if len(args.issn) == 1:
        source, error = ingest_issn(args.issn[0], args.publisher_id, args.is_core,
                                    args.is_oa, args.overwrite_journal_id)

        if error:
            print(error)
            return
        print(
            f'Journal {"updated" if args.overwrite_journal_id else "created"}: {source}')
    else:
        Thread(target=refresh_token, daemon=True).start()
        for i, issn in enumerate(args.issn):
            print(f'Ingesting ISSN {issn} ({i + 1} / {len(args.issn)})')
            try:
                source, error = ingest_issn(issn)
                if error:
                    print(error)
                    continue
                print(
                    f'Journal created: {source} ({i + 1} / {len(args.issn)})')
            except Exception as e:
                print(f'Error ingesting ISSN {issn}: {e}')
                if 'psycopg2' in str(e):
                    db.session.rollback()




if __name__ == '__main__':
    main()