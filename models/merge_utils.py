import json
import re
from copy import deepcopy

import math
from enum import Enum

from app import logger
from const import MAX_AFFILIATIONS_PER_AUTHOR
from util import normalize

PARSED_RECORD_TYPES = {'crossref_parseland', 'parsed_pdf'}


def affiliations_probably_invalid(normalized_authors):
    return max(
        [len(author.get('affiliation', [])) for author in
         normalized_authors]) > MAX_AFFILIATIONS_PER_AUTHOR


def authors_no_affiliations(normalized_authors):
    for author in normalized_authors:
        author['affiliation'] = []
    return normalized_authors


def normalized_authors_has_affiliations(normalized_authors):
    return any(
        [bool(author.get('affiliation', [])) for author in normalized_authors])


def clone_record(record):
    from models import Record
    exclude_attrs = {'_sa_instance_state',
                     'insert_dict'}
    parent_record_d = {k: v for k, v in record.__dict__.items() if
                       k not in exclude_attrs}
    cloned = Record(**parent_record_d)
    return cloned


def merge_primary_with_parsed(primary_record, **parsed_records):
    if not primary_record or not primary_record.record_type in {'crossref_doi',
                                                                'datacite_doi'}:
        return primary_record
    pl_record, pdf_record = parsed_records['parseland_record'], parsed_records[
        'pdf_record']
    if pl_record is None and pdf_record is None:
        return primary_record

    logger.info(
        f"merging record {primary_record.id} with parsed records {pl_record.id if pl_record else None} (parseland), {pdf_record.id if pdf_record else None} (pdf)")

    cloned_parent_record = clone_record(primary_record)

    cloned_parent_record = merge_authors(cloned_parent_record,
                                         primary_record, **parsed_records)
    cloned_parent_record = merge_citations(cloned_parent_record,
                                           primary_record, **parsed_records)
    cloned_parent_record = merge_abstract(cloned_parent_record,
                                          primary_record, **parsed_records)
    return cloned_parent_record


def merge_abstract(cloned_parent_record, original_parent_record,
                   **parsed_records):
    pl_record, pdf_record = parsed_records.get(
        'parseland_record'), parsed_records.get('pdf_record')
    abstract_record = pl_record
    if (
            not pl_record or not pl_record.abstract) and pdf_record and pdf_record.abstract:
        abstract_record = pdf_record
    if original_parent_record.abstract:
        cloned_parent_record.abstract = original_parent_record.abstract
    elif abstract_record and abstract_record.abstract:
        cloned_parent_record.abstract = abstract_record.abstract
    return cloned_parent_record


def merge_citations(cloned_parent_record, original_parent_record,
                    **parsed_records):
    records_sorted = [parsed_records.get('parseland_record'),
                      parsed_records.get('pdf_record')]
    records_sorted = [record for record in records_sorted if record]
    citation_record = None
    for record in records_sorted:
        if record and record.has_citations:
            citation_record = record
    if len(original_parent_record.citations or '[]') > 3:
        cloned_parent_record.citations = original_parent_record.citations
    elif citation_record:
        cloned_parent_record.citations = citation_record.citations
    return cloned_parent_record


def merge_authors(cloned_parent_record, original_parent_record,
                  **parsed_records):
    pl_record, pdf_record, hal_record = (parsed_records.get('parseland_record'),
                                         parsed_records.get('pdf_record'),
                                         parsed_records.get('hal_record'))
    normalized_pl_dict, normalized_pdf_dict, normalized_hal_dict = (
    _normalized_record_dict(pl_record),
    _normalized_record_dict(pdf_record),
    _normalized_record_dict(hal_record))
    normalized_authors_dict = normalized_hal_dict
    if not normalized_authors_dict:
        normalized_authors_dict = normalized_pl_dict
    if (not normalized_authors_dict or not normalized_authors_has_affiliations(normalized_authors_dict.get('authors', []))) and (pdf_record and pdf_record.has_affiliations):
        normalized_authors_dict = normalized_pdf_dict
    normalized_authors = normalized_authors_dict.get('authors', [])
    parent_authors = original_parent_record.authors_json
    if not parent_authors and normalized_authors and not affiliations_probably_invalid(
            normalized_authors):
        cloned_parent_record.authors = json.dumps(normalized_authors)
    else:
        cloned_parent_record.authors = json.dumps(
            merge_affiliations(original_parent_record, normalized_authors))
    return cloned_parent_record


def merge_affiliations(original_record, normalized_authors):
    parent_authors = original_record.authors_json
    normalized_parsed_authors = [normalize(author.get('raw', '')) for author in
                                 normalized_authors]
    for parent_author_idx, parent_author in enumerate(parent_authors):
        best_match_idx = _match_parsed_author(
            parent_author,
            parent_author_idx,
            normalized_parsed_authors
        )

        if best_match_idx > -1:
            parsed_author = normalized_authors[best_match_idx]
            parent_author['is_corresponding'] = parsed_author.get(
                'is_corresponding', '')
            parent_author['affiliation'] = _reconcile_affiliations(
                parent_author,
                parsed_author,
                original_record.doi
            )
    return parent_authors


def _reconcile_affiliations(parent_author, pl_author, doi):
    if '/nejm' in doi.lower():
        return pl_author['affiliation']
    final_affs = []
    pl_affs = pl_author['affiliation'].copy()
    # We probably only want English affiliations from Parseland
    # Sometimes Crossref will have English version and Parseland will have version in another language
    # We probably don't want to keep version that is not in English
    pl_affs = [aff for aff in pl_affs if aff['name'].isascii()] if \
        parent_author['affiliation'] else pl_affs
    for aff in parent_author['affiliation']:
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


def _match_parsed_author(parent_author, parent_author_idx,
                         normalized_authors):
    family = normalize(parent_author.get('family') or '')
    given = normalize(parent_author.get('given') or '')

    best_match_score = (0, -math.inf)
    best_match_idx = -1
    for pl_author_idx, pl_author_name in enumerate(normalized_authors):
        name_match_score = 0

        if family and pl_author_name and family in pl_author_name:
            name_match_score += 2

        if given and pl_author_name and given in pl_author_name:
            name_match_score += 1

        index_difference = abs(parent_author_idx - pl_author_idx)

        if name_match_score:
            match_score = (name_match_score, -index_difference)

            if match_score > best_match_score:
                best_match_score = match_score
                best_match_idx = pl_author_idx
    return best_match_idx


def _normalized_record_dict(parsed_record):
    normalized_dict = {
        'authors': [],
        'published_date': None,
        'genre': None,
        'abstract': None,
        'citations': []
    }
    if not parsed_record:
        return normalized_dict

    parsed_authors = parsed_record.authors_json

    for parsed_author in parsed_authors:
        author = {
            'raw': parsed_author.get('name') or parsed_author.get('raw'),
            'affiliation': [],
            'is_corresponding': parsed_author.get('is_corresponding')
        }
        parsed_affiliations = parsed_author.get(
            'affiliations') or parsed_author.get('affiliation')

        if isinstance(parsed_affiliations, list):
            for parsed_affiliation in parsed_affiliations:
                if isinstance(parsed_affiliation, dict) and 'name' in parsed_affiliation:
                    parsed_affiliation = parsed_affiliation['name']
                author['affiliation'].append({'name': parsed_affiliation})

        if orcid := parsed_author.get('orcid'):
            author['orcid'] = orcid

        normalized_dict['authors'].append(_normalize_author(author))

    normalized_dict['published_date'] = parsed_record.published_date
    normalized_dict['genre'] = parsed_record.genre
    normalized_dict['abstract'] = parsed_record.abstract
    normalized_dict['citations'] = parsed_record.citations_json

    return normalized_dict


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
