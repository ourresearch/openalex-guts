import json
import re
from copy import deepcopy

import math

from app import logger
from util import normalize


def merge_crossref_with_parseland(crossref_record, parseland_record):
    from models import Record

    if not crossref_record:
        return None

    if not (
        crossref_record and crossref_record.record_type == 'crossref_doi'
        and parseland_record and parseland_record.record_type == 'crossref_parseland'
    ):
        return crossref_record

    logger.info(f"merging record {crossref_record.id} with parseland record {parseland_record.id}")
    cloned_crossref_record = Record(
        id=crossref_record.id,
        updated=crossref_record.updated,

        # ids
        record_type=crossref_record.record_type,
        doi=crossref_record.doi,
        pmid=crossref_record.pmid,
        pmh_id=crossref_record.pmh_id,
        pmcid=crossref_record.pmcid,
        arxiv_id=crossref_record.arxiv_id,

        # metadata
        title=crossref_record.title,
        published_date=crossref_record.published_date,
        genre=crossref_record.genre,
        abstract=crossref_record.abstract,
        mesh=crossref_record.mesh,
        publisher=crossref_record.publisher,
        normalized_book_publisher=crossref_record.normalized_book_publisher,
        normalized_conference=crossref_record.normalized_conference,
        institution_host=crossref_record.institution_host,
        is_retracted=crossref_record.is_retracted,
        volume=crossref_record.volume,
        issue=crossref_record.issue,
        first_page=crossref_record.first_page,
        last_page=crossref_record.last_page,

        # related tables
        citations=crossref_record.citations,
        authors=crossref_record.authors,

        # source links
        repository_id=crossref_record.repository_id,
        # the journal_id in record is not the openalex journal ID
        journal_issns=crossref_record.journal_issns,
        journal_issn_l=crossref_record.journal_issn_l,
        venue_name=crossref_record.venue_name,

        journals=crossref_record.journals,

        # record data
        record_webpage_url=crossref_record.record_webpage_url,
        record_webpage_archive_url=crossref_record.record_webpage_archive_url,
        record_structured_url=crossref_record.record_structured_url,
        record_structured_archive_url=crossref_record.record_structured_archive_url,

        # oa and urls
        work_pdf_url=crossref_record.work_pdf_url,
        work_pdf_archive_url=crossref_record.work_pdf_archive_url,
        is_work_pdf_url_free_to_read=crossref_record.is_work_pdf_url_free_to_read,
        is_oa=crossref_record.is_oa,
        oa_date=crossref_record.oa_date,
        open_license=crossref_record.open_license,
        open_version=crossref_record.open_version,

        normalized_title=crossref_record.normalized_title,
        funders=crossref_record.funders,

        # relationship to works is set in Work
        work_id=crossref_record.work_id
    )

    parseland_dict = _parseland_record_dict(parseland_record)
    pl_authors = parseland_dict.get('authors', [])
    normalized_pl_authors = [normalize(author.get('raw', '')) for author in pl_authors]

    crossref_authors = json.loads(cloned_crossref_record.authors or '[]')
    for crossref_author_idx, crossref_author in enumerate(crossref_authors):
        best_match_idx = _match_pl_author(
            crossref_author,
            crossref_author_idx,
            normalized_pl_authors
        )

        if best_match_idx > -1:
            pl_author = pl_authors[best_match_idx]
            crossref_author['is_corresponding'] = pl_author.get('is_corresponding', '')
            crossref_author['affiliation'] = _reconcile_affiliations(
                crossref_author,
                pl_author,
                crossref_record.doi
            )

    cloned_crossref_record.abstract = crossref_record.abstract or parseland_dict.get('abstract')
    cloned_crossref_record.authors = json.dumps(crossref_authors)

    return cloned_crossref_record


def _reconcile_affiliations(crossref_author, pl_author, doi):
    if '/nejm' in doi.lower():
        return pl_author['affiliation']
    final_affs = []
    pl_affs = pl_author['affiliation'].copy()
    # We probably only want English affiliations from Parseland
    # Sometimes Crossref will have English version and Parseland will have version in another language
    # We probably don't want to keep version that is not in English
    pl_affs = [aff for aff in pl_affs if aff['name'].isascii()] if crossref_author['affiliation'] else pl_affs
    for aff in crossref_author['affiliation']:
        # Assume crossref affiliation is better version initially
        if all((aff.get('department'), aff.get('id'), not pl_affs, not aff['name'])):
            final_affs.append(aff)
            continue
        best_aff_version = aff['name']
        pl_aff_idx = _match_affiliation(aff['name'], [aff['name'] for aff in pl_affs])
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


def _match_pl_author(crossref_author, crossref_author_idx, normalized_pl_authors):
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


def _parseland_record_dict(parseland_record):
    parseland_dict = {
        'authors': [],
        'published_date': None,
        'genre': None,
        'abstract': None
    }

    pl_authors = json.loads(parseland_record.authors or '[]') or []

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

        parseland_dict['authors'].append(_normalize_author(author))

    parseland_dict['published_date'] = parseland_record.published_date
    parseland_dict['genre'] = parseland_record.genre
    parseland_dict['abstract'] = parseland_record.abstract

    return parseland_dict


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
