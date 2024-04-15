import json
import re
from copy import deepcopy

import math

from app import logger
from util import normalize

PARSED_RECORD_TYPES = {'crossref_parseland', 'parsed_pdf'}


def affiliations_probably_invalid(parsed_record):
    return max(
        [len(author.get('affiliations', [])) for author in parsed_record.authors_json]) > 10 and parsed_record.record_type == 'parsed_pdf'


def merge_crossref_with_parsed(crossref_record, parsed_record):
    from models import Record

    if not crossref_record:
        return None

    if not (
            crossref_record and crossref_record.record_type == 'crossref_doi'
            and parsed_record and parsed_record.record_type in PARSED_RECORD_TYPES
            and not affiliations_probably_invalid(parsed_record)
    ):
        return crossref_record

    logger.info(
        f"merging record {crossref_record.id} with parsed record {parsed_record.id}")

    exclude_attrs = {'unpaywall', 'parseland_record', '_sa_instance_state',
                     'insert_dict'}
    crossref_record_d = {k: v for k, v in crossref_record.__dict__.items() if
                         k not in exclude_attrs}
    cloned_crossref_record = Record(**crossref_record_d)

    parsed_dict = _parsed_record_dict(parsed_record)
    pl_authors = parsed_dict.get('authors', [])
    normalized_pl_authors = [normalize(author.get('raw', '')) for author in
                             pl_authors]

    crossref_authors = json.loads(cloned_crossref_record.authors or '[]')
    if not crossref_authors and pl_authors:
        cloned_crossref_record.authors = json.dumps(pl_authors)
        return cloned_crossref_record
    for crossref_author_idx, crossref_author in enumerate(crossref_authors):
        best_match_idx = _match_parsed_author(
            crossref_author,
            crossref_author_idx,
            normalized_pl_authors
        )

        if best_match_idx > -1:
            pl_author = pl_authors[best_match_idx]
            crossref_author['is_corresponding'] = pl_author.get(
                'is_corresponding', '')
            crossref_author['affiliation'] = _reconcile_affiliations(
                crossref_author,
                pl_author,
                crossref_record.doi
            )

    cloned_crossref_record.abstract = crossref_record.abstract or parsed_dict.get(
        'abstract')
    cloned_crossref_record.authors = json.dumps(crossref_authors)
    cloned_crossref_record.citations = crossref_record.citations if len(
        crossref_record.citations or '[]') > 3 else json.dumps(
        parsed_dict.get('citations'))

    return cloned_crossref_record


def _reconcile_affiliations(crossref_author, pl_author, doi):
    if '/nejm' in doi.lower():
        return pl_author['affiliation']
    final_affs = []
    pl_affs = pl_author['affiliation'].copy()
    # We probably only want English affiliations from Parseland
    # Sometimes Crossref will have English version and Parseland will have version in another language
    # We probably don't want to keep version that is not in English
    pl_affs = [aff for aff in pl_affs if aff['name'].isascii()] if \
        crossref_author['affiliation'] else pl_affs
    for aff in crossref_author['affiliation']:
        # Assume crossref affiliation is better version initially
        if all((aff.get('department'), aff.get('id'), not pl_affs,
                not aff['name'])):
            final_affs.append(aff)
            continue
        best_aff_version = aff['name']
        pl_aff_idx = _match_affiliation(aff['name'],
                                        [aff['name'] for aff in pl_affs])
        if pl_aff_idx > -1:
            # If a match is found, pick the better one and set best_aff_version to this one
            pl_aff = pl_affs.pop(pl_aff_idx)
            best_aff_version = _best_affiliation(aff['name'], pl_aff['name'])
        final_affs.append({'name': best_aff_version})

    # If there are remaining parseland affiliations, this means that they are not present in crossref. Add them to list of final affs
    final_affs.extend(pl_affs)
    return final_affs


def _best_affiliation(aff_ver1, aff_ver2):
    aff_ver1 = _cleanup_affiliation(aff_ver1)
    aff_ver2 = _cleanup_affiliation(aff_ver2)
    if len(aff_ver1) > len(aff_ver2):
        return aff_ver1
    return aff_ver2


def _cleanup_affiliation(aff):
    aff = re.sub(r'^[a-z] +', '', aff)
    return re.sub(r' +', ' ', aff)


def _match_affiliation(aff, other_affs):
    # Splitting by non-alpha chars (\W) is automatically going to trim/strip commas, spaces, periods, etc from each word
    aff_capitalized_words = set()

    if aff:
        aff_capitalized_words = set(
            [word for word in re.split(r'\W', aff) if
             word and word[0].isupper()])
    best_match_idx = -1
    highest_match_count = 0
    for i, other_aff in enumerate(other_affs):
        if not other_aff:
            continue
        # Sometimes affiliation strings are all uppercase i.e. GOOGLE INC.
        # Don't want to use word.istitle() for this reason
        other_capitalized_words = set(
            [word for word in re.split(r'\W', other_aff) if
             word and word[0].isupper()])
        matches = [word for word in other_capitalized_words if
                   word in aff_capitalized_words]
        match_count = len(matches)
        if match_count > highest_match_count:
            best_match_idx = i
            highest_match_count = match_count
    return best_match_idx


def _match_parsed_author(crossref_author, crossref_author_idx,
                         normalized_pl_authors):
    family = normalize(crossref_author.get('family') or '')
    given = normalize(crossref_author.get('given') or '')

    best_match_score = (0, -math.inf)
    best_match_idx = -1
    for pl_author_idx, pl_author_name in enumerate(normalized_pl_authors):
        name_match_score = 0

        if family and pl_author_name and family in pl_author_name:
            name_match_score += 2

        if given and pl_author_name and given in pl_author_name:
            name_match_score += 1

        index_difference = abs(crossref_author_idx - pl_author_idx)

        if name_match_score:
            match_score = (name_match_score, -index_difference)

            if match_score > best_match_score:
                best_match_score = match_score
                best_match_idx = pl_author_idx
    return best_match_idx


def _parsed_record_dict(parsed_record):
    parsed_dict = {
        'authors': [],
        'published_date': None,
        'genre': None,
        'abstract': None,
        'citations': []
    }

    pl_authors = json.loads(parsed_record.authors or '[]') or []

    for pl_author in pl_authors:
        author = {
            'raw': pl_author.get('name'),
            'affiliation': [],
            'is_corresponding': pl_author.get('is_corresponding')
        }
        pl_affiliations = pl_author.get('affiliations')

        if isinstance(pl_affiliations, list):
            for pl_affiliation in pl_affiliations:
                author['affiliation'].append({'name': pl_affiliation})

        if orcid := pl_author.get('orcid'):
            author['orcid'] = orcid

        parsed_dict['authors'].append(_normalize_author(author))

    parsed_dict['published_date'] = parsed_record.published_date
    parsed_dict['genre'] = parsed_record.genre
    parsed_dict['abstract'] = parsed_record.abstract
    parsed_dict['citations'] = json.loads(parsed_record.citations or '[]') or []

    return parsed_dict


def _normalize_author(author):
    # https://api.crossref.org/swagger-ui/index.html#model-Author
    author = deepcopy(author)

    for k in list(author.keys()):
        if k != k.lower():
            author[k.lower()] = author[k]
            del author[k]

    author.setdefault('raw', None)

    if 'affiliations' in author and 'affiliation' not in author:
        author['affiliation'] = author['affiliations']
        del author['affiliations']

    author.setdefault('affiliation', [])

    for idx, affiliation in enumerate(author['affiliation']):
        if isinstance(affiliation, str):
            affiliation = {'name': affiliation}
            author['affiliation'][idx] = affiliation

        for k in list(affiliation.keys()):
            if k != k.lower():
                affiliation[k.lower()] = affiliation[k]
                del affiliation[k]

        affiliation.setdefault('name', None)

    author.setdefault('sequence', None)
    author.setdefault('name', None)
    author.setdefault('family', None)
    author.setdefault('orcid', None)
    author.setdefault('suffix', None)
    author.setdefault('authenticated-orcid', None)
    author.setdefault('given', None)

    if author['orcid']:
        author['orcid'] = re.sub(r'.*((?:[0-9]{4}-){3}[0-9]{3}[0-9X]).*', r'\1',
                                 author['orcid'].upper())

    return author
