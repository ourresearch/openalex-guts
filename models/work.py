import datetime
import hashlib
import json
import os
import re
from collections import defaultdict
from enum import IntEnum
from functools import cache
from time import sleep
from time import time
from typing import List

import requests
from cached_property import cached_property
from sqlalchemy import event, orm, text
from sqlalchemy.orm import selectinload
from sqlalchemy.orm.attributes import get_history
from sqlalchemy.types import ARRAY

import models
from app import WORKS_INDEX_PREFIX
from app import COUNTRIES_ENDPOINT_PREFIX
from app import COUNTRIES
from app import MAX_MAG_ID
from app import db
from app import get_apiurl_from_openalex_url
from app import get_db_cursor
from app import logger
from models.concept import is_valid_concept_id
from models.topic import is_valid_topic_id
from models.work_sdg import get_and_save_sdgs
from util import clean_doi, entity_md5
from util import clean_html
from util import detect_language_from_abstract_and_title
from util import elapsed
from util import f_generate_inverted_index
from util import normalize_orcid
from util import normalize_simple
from util import struct_changed
from util import truncate_on_word_break
from util import work_has_null_author_ids

DELETED_WORK_ID = 4285719527


def elastic_index_suffix(publication_year):
    if not publication_year or not isinstance(publication_year, int):
        return "invalid-data"

    if publication_year < 1960:
        return "1959-or-less"
    elif publication_year > 1959 and publication_year < 1970:
        return "1960s"
    elif publication_year > 1969 and publication_year < 1980:
        return "1970s"
    elif publication_year > 1979 and publication_year < 1990:
        return "1980s"
    elif publication_year > 1989 and publication_year < 1995:
        return "1990-to-1994"
    elif publication_year > 1994 and publication_year < 2000:
        return "1995-to-1999"
    elif publication_year > 2025:
        return "invalid-data"
    else:
        return str(publication_year)


# truncate mid.work
# insert into mid.work (select * from legacy.mag_main_papers)
# update mid.work set original_title=replace(original_title, '\\\\/', '/');

def as_work_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/W{id}"

def call_sagemaker_bulk_lookup_new_work_concepts(rows):
    insert_dicts = []
    data_list = []
    for row in rows:
        has_abstract = True if row["indexed_abstract"] else False
        data_list += [{
            "title": row["paper_title"].lower() if row["paper_title"] else None,
            "doc_type": row["doc_type"],
            "journal": row["journal_title"].lower() if row["journal_title"] else None,
            "abstract": row["indexed_abstract"],
            "inverted_abstract": has_abstract,
            "paper_id": row["paper_id"]
        }]

    class ConceptLookupResponse:
        pass

    api_key = os.getenv("SAGEMAKER_API_KEY")
    headers = {"X-API-Key": api_key}
    # api_url = "https://4rwjth9jek.execute-api.us-east-1.amazonaws.com/api/" #for version without abstracts
    api_url = "https://l7a8sw8o2a.execute-api.us-east-1.amazonaws.com/api/" #for version with abstracts
    r = requests.post(api_url, json=json.dumps(data_list), headers=headers)
    if r.status_code != 200:
        logger.error(f"error in call_sagemaker_bulk_lookup_new_work_concepts: status code {r} reason {r.reason}")
        return []

    api_json = r.json()
    for row, api_dict in zip(rows, api_json):
        if api_dict["tags"] != []:
            for i, concept_name in enumerate(api_dict["tags"]):
                insert_dicts += [{"WorkConcept": {"paper_id": row["paper_id"],
                                                       "field_of_study": api_dict["tag_ids"][i],
                                                       "score": api_dict["scores"][i],
                                                       "algorithm_version": 3,
                                                       "uses_newest_algorithm": True,
                                                       "updated_date": datetime.datetime.utcnow().isoformat()}}]
    response = ConceptLookupResponse()
    response.insert_dicts = insert_dicts
    response.delete_dict = {"WorkConcept": [row["paper_id"] for row in rows]}
    return [response]


@cache
def pubmed_json():
    return models.source.Source.query.options(
        selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
        selectinload(models.Source.publisher_entity).raiseload('*'),
        selectinload(models.Source.institution).raiseload('*'),
        orm.Load(models.Source).raiseload('*')
    ).get(4306525036).to_dict(return_level='minimum')


@cache
def arxiv_json():
    return models.source.Source.query.options(
        selectinload(models.Source.publisher_entity).selectinload(models.Publisher.self_and_ancestors).raiseload('*'),
        selectinload(models.Source.publisher_entity).raiseload('*'),
        selectinload(models.Source.institution).raiseload('*'),
        orm.Load(models.Source).raiseload('*')
    ).get(4306400194).to_dict(return_level='minimum')


@cache
def location_url_overrides():
    return {
        r'^http(s)?://arxiv\.org/': arxiv_json()
    }


def override_location_sources(locations):
    if not locations:
        return []

    for loc in locations:
        for url_pattern, source_json in location_url_overrides().items():
            pdf_url = loc.get('pdf_url') or ''
            landing_page_url = loc.get('landing_page_url') or ''

            if re.search(url_pattern, pdf_url) or re.search(url_pattern, landing_page_url):
                loc['source'] = source_json

                if not re.search(url_pattern, pdf_url):
                    loc['pdf_url'] = None

                if not re.search(url_pattern, landing_page_url):
                    loc['landing_page_url'] = None

    return locations

class OAStatusEnum(IntEnum):
    # we prioritize publisher-hosted versions
    # see https://docs.openalex.org/api-entities/works/work-object#any_repository_has_fulltext
    closed = 0
    unknown = 1
    green = 2
    bronze = 3
    hybrid = 4
    gold = 5

def oa_status_from_location(loc):
    if not loc.get('is_oa'):
        return 'closed'
    source = loc.get('source')
    if source is not None:
        if source['is_in_doaj']:
            return 'gold'
        if source['type'] == 'repository':
            return 'green'
        if loc.get('license') and loc['license'] not in ['unknown', 'unspecified-oa', 'implied-oa']:
            return 'hybrid'
        else:
            return 'bronze'
    else:
        # if we don't know anything about the source, we'll assume that it's bronze (we might want to change this)
        return 'bronze'
    

class Work(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work"

    paper_id = db.Column(db.BigInteger, primary_key=True)
    doi = db.Column(db.Text)
    doc_type = db.Column(db.Text)
    paper_title = db.Column(db.Text)
    original_title = db.Column(db.Text)
    year = db.Column(db.Numeric)
    publication_date = db.Column(db.DateTime)
    online_date = db.Column(db.DateTime)
    publisher = db.Column(db.Text)
    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"))
    volume = db.Column(db.Text)
    issue = db.Column(db.Text)
    first_page = db.Column(db.Text)
    last_page = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)
    full_updated_date = db.Column(db.DateTime)
    concepts_input_hash = db.Column(db.Text)
    topics_input_hash = db.Column(db.Text)
    previous_years = db.Column(ARRAY(db.Numeric))

    doi_lower = db.Column(db.Text)
    doc_sub_types = db.Column(db.Text)
    original_venue = db.Column(db.Text)
    genre = db.Column(db.Text)
    is_paratext = db.Column(db.Boolean)
    oa_status = db.Column(db.Text)
    best_url = db.Column(db.Text)
    best_free_url = db.Column(db.Text)
    best_free_version = db.Column(db.Text)

    unpaywall_normalize_title = db.Column(db.Text)

    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)
    json_entity_hash = db.Column(db.Text)
    arxiv_id = db.Column(db.Text)

    def __init__(self, **kwargs):
        super(Work, self).__init__(**kwargs)

    @property
    def id(self):
        return self.paper_id

    @property
    def cited_by_api_url(self):
        return f"https://api.openalex.org/works?filter=cites:{self.openalex_id_short}"

    @property
    def openalex_id(self):
        return as_work_openalex_id(self.paper_id)

    @property
    def openalex_id_short(self):
        from models import short_openalex_id
        return short_openalex_id(self.openalex_id)

    @property
    def openalex_api_url(self):
        return get_apiurl_from_openalex_url(self.openalex_id)

    def update_institutions(self, affiliation_retry_attempts=30):
        if not self.affiliations:
            return

        record_author_dict_list = []
        records_with_affiliations = [record for record in self.records_sorted if record.has_affiliations]
        if not records_with_affiliations:
            records_with_affiliations = [record for record in self.records_sorted if record.authors]

        if records_with_affiliations:
            record_author_dict_list = records_with_affiliations[0].authors_json

        all_affiliations = sorted(
            self.affiliations,
            key=lambda a: (a.author_sequence_number, a.affiliation_sequence_number)
        )

        self.affiliations = []

        author_sequence_nos = sorted(set([a.author_sequence_number for a in all_affiliations]))

        update_original_affiliations = False
        if len(record_author_dict_list) == len(author_sequence_nos):
            update_original_affiliations = True

        for author_idx, author_sequence_no in enumerate(author_sequence_nos):
            author_affiliations = sorted(
                [a for a in all_affiliations if a.author_sequence_number == author_sequence_no],
                key=lambda a: a.affiliation_sequence_number
            )

            original_affiliations = []
            if update_original_affiliations:
                original_affiliations = [
                    aff.get('name')
                    for aff in record_author_dict_list[author_idx].get('affiliation', [])
                    if aff.get('name')
                ]
                is_corresponding_author = record_author_dict_list[author_idx].get('is_corresponding', False)
            if not original_affiliations:
                original_affiliations = [a.original_affiliation for a in author_affiliations if a.original_affiliation]
                is_corresponding_author = author_affiliations[0].is_corresponding_author

            old_institution_ids = set([a.affiliation_id for a in author_affiliations if a.affiliation_id])

            new_institution_id_lists = models.Institution.get_institution_ids_from_strings(
                original_affiliations, retry_attempts=affiliation_retry_attempts
            )
            new_institution_ids = set()
            for new_institution_id_list in new_institution_id_lists:
                new_institution_ids.update([i for i in new_institution_id_list if i])

            strings_need_update = not all([aff1 == aff2 for aff1, aff2 in zip([author_aff.original_affiliation for author_aff in author_affiliations], original_affiliations)])

            if (
                old_institution_ids == new_institution_ids
                and is_corresponding_author == author_affiliations[0].is_corresponding_author
                and not strings_need_update
            ):
                self.affiliations.extend(author_affiliations)
                continue

            id_lookup = dict(zip(original_affiliations, new_institution_id_lists))

            affiliation_sequence_no = 1
            seen_ids = set()

            for original_affiliation in original_affiliations or [None]:
                affiliation_ids = id_lookup.get(original_affiliation, [None])
                for affiliation_id in affiliation_ids:
                    if affiliation_id and affiliation_id in seen_ids:
                        logger.info(f'seen id {affiliation_id}, continue')
                        continue

                    self.affiliations.append(
                        models.Affiliation(
                            author_id=author_affiliations[0].author_id,
                            affiliation_id=affiliation_id,
                            author_sequence_number=author_affiliations[0].author_sequence_number,
                            affiliation_sequence_number=affiliation_sequence_no,
                            original_author=author_affiliations[0].original_author,
                            original_affiliation=original_affiliation,
                            original_orcid=author_affiliations[0].original_orcid,
                            match_institution_name=models.Institution.matching_institution_name(original_affiliation),
                            is_corresponding_author=is_corresponding_author,
                            updated_date=datetime.datetime.utcnow().isoformat()
                        )
                    )

                    if affiliation_id:
                        seen_ids.add(affiliation_id)

                    affiliation_sequence_no += 1

    def update_orcid(self):
        if not self.affiliations:
            return

        affiliation_lookup = {}
        for aff in self.affiliations:
            if aff.author_sequence_number not in affiliation_lookup:
                affiliation_lookup[aff.author_sequence_number] = []
            affiliation_lookup[aff.author_sequence_number].append(aff)

        records_with_affiliations = [record for record in self.records_sorted if record.has_affiliations]
        if not records_with_affiliations:
            records_with_affiliations = [record for record in self.records_sorted if record.authors]

        if records_with_affiliations:
            try:
                record_author_dict_list = json.loads(records_with_affiliations[0].authors)
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON authors for {self.id}: {e}")
                return

            for author_idx, author_dict in enumerate(record_author_dict_list):
                orcid = author_dict.get('orcid')
                if orcid:
                    affiliations = affiliation_lookup.get(author_idx + 1, [])
                    for affiliation in affiliations:
                        # skip if name does not match
                        family_name = author_dict.get('family')
                        original_author = affiliation.original_author
                        if family_name and original_author and family_name.lower() not in original_author.lower():
                            if not affiliation.original_orcid:
                                logger.info(
                                    f"Skip updating author_id {affiliation.author_id}, work_id {self.id} because "
                                    f"family name not in original_author")
                                continue

                        if not affiliation.original_orcid:
                            logger.info(
                                f"Updating original_orcid for author_id {affiliation.author_id}, work_id {self.id} "
                                f"to {orcid}")
                            affiliation.original_orcid = orcid

    def concept_api_input_data(self):
        abstract = self.abstract.abstract if self.abstract else None

        return {
            "title": self.work_title.lower() if self.work_title else None,
            "doc_type": self.doc_type,
            "journal": self.journal.display_name.lower() if self.journal else None,
            "abstract": abstract,
            "inverted_abstract": False,
            "paper_id": self.paper_id
        }
    
    def topic_api_input_data(self):
        abstract = self.abstract.abstract if self.abstract else None

        return {
            "title": self.work_title if self.work_title else "",
            "abstract_inverted_index": abstract,
            "journal_display_name": self.journal.display_name.lower() if self.journal else "",
            "referenced_works": self.references_list,
            "inverted": False,
        }

    def get_concepts_input_hash(self):
        return hashlib.md5(
            json.dumps(self.concept_api_input_data(), sort_keys=True).encode('utf-8')
        ).hexdigest()
    
    def get_topics_input_hash(self):
        return hashlib.md5(
            json.dumps(self.topic_api_input_data(), sort_keys=True).encode('utf-8')
        ).hexdigest()
    
    def add_work_topics(self):
        current_topics_input_hash = self.get_topics_input_hash()
        if self.topics_input_hash == '-1':
            logger.info('skipping topic classification because it should not have topics for now. Set input hash to current.')
            self.topics_input_hash = current_topics_input_hash
            return
        elif self.topics_input_hash == current_topics_input_hash:
            logger.info('skipping topic classification because inputs are unchanged')
            return

        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        data = self.topic_api_input_data()
        api_key = os.getenv("SAGEMAKER_API_KEY")

        headers = {"X-API-Key": api_key}
        api_url = "https://5gl84dua69.execute-api.us-east-1.amazonaws.com/api/"

        number_tries = 0
        keep_calling = True
        topic_ids = None
        topic_scores = None
        response_json = None
        # r = None

        while keep_calling:
            r = requests.post(api_url, json=json.dumps([data], sort_keys=True), headers=headers)

            if r.status_code == 200:
                try:
                    response_json = r.json()
                    resp_data = response_json[0]
                    topic_ids = [i['topic_id'] for i in resp_data]
                    topic_scores = [i['topic_score'] for i in resp_data]
                    keep_calling = False
                except Exception as e:
                    logger.error(f"error {e} in add_work_topics with {self.id}, response {r}, called with {api_url} data: {data}")
                    topic_ids = None
                    topic_scores = None
                    keep_calling = False

            elif r.status_code == 500:
                logger.error(f"Error on try #{number_tries}, now trying again: Error back from API endpoint: {r} {r.status_code}")
                sleep(0.5)
                number_tries += 1
                if number_tries > 60:
                    keep_calling = False

            else:
                logger.error(f"Error, not retrying: Error back from API endpoint: {r} {r.status_code} {r.text} for input {data}")
                topic_ids = None
                topic_scores = None
                keep_calling = False

        if r.status_code == 200:
            self.topics = []
            self.topics_for_related_works = []

            if topic_ids and topic_scores:
                for i, (topic_id, topic_score) in enumerate(zip(topic_ids, 
                                                                topic_scores)):
                    if topic_id and is_valid_topic_id(topic_id):
                        new_work_topic = models.WorkTopic(
                            topic_id=topic_id,
                            score=topic_score,
                            algorithm_version=1,
                            updated_date=datetime.datetime.utcnow().isoformat()
                        )

                        self.topics.append(new_work_topic)

                        # self.topics_for_related_works.append(topic_id)
            self.topics_input_hash = current_topics_input_hash

    def add_work_concepts(self):
        current_concepts_input_hash = self.get_concepts_input_hash()

        if self.concepts_input_hash == current_concepts_input_hash:
            logger.info('skipping concept tagging because inputs are unchanged')
            return

        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        data = self.concept_api_input_data()
        api_key = os.getenv("SAGEMAKER_API_KEY")

        headers = {"X-API-Key": api_key}
        api_url = "https://l7a8sw8o2a.execute-api.us-east-1.amazonaws.com/api/" #for vesion with abstracts

        number_tries = 0
        keep_calling = True
        concept_names = None
        response_json = None
        r = None

        while keep_calling:
            r = requests.post(api_url, json=json.dumps([data], sort_keys=True), headers=headers)

            if r.status_code == 200:
                try:
                    response_json = r.json()
                    concept_names = response_json[0]["tags"]
                    keep_calling = False
                except Exception as e:
                    logger.error(f"error {e} in add_work_concepts with {self.id}, response {r}, called with {api_url} data: {data} headers: {headers}")
                    concept_names = None
                    keep_calling = False

            elif r.status_code == 500:
                logger.error(f"Error on try #{number_tries}, now trying again: Error back from API endpoint: {r} {r.status_code}")
                sleep(1)
                number_tries += 1
                if number_tries > 60:
                    keep_calling = False

            else:
                logger.error(f"Error, not retrying: Error back from API endpoint: {r} {r.status_code} {r.text} for input {data}")
                concept_names = None
                keep_calling = False

        if r.status_code == 200:
            self.concepts = []
            self.concepts_for_related_works = []

            if concept_names:
                for i, concept_name in enumerate(concept_names):
                    score = response_json[0]["scores"][i]
                    field_of_study = response_json[0]["tag_ids"][i]

                    if field_of_study and is_valid_concept_id(field_of_study):
                        new_work_concept = models.WorkConcept(
                            field_of_study=field_of_study,
                            score=score,
                            algorithm_version=3,
                            uses_newest_algorithm=True,
                            updated_date=datetime.datetime.utcnow().isoformat()
                        )

                        self.concepts.append(new_work_concept)

                        if score > 0.3:
                            self.concepts_for_related_works.append(field_of_study)

            self.concepts_input_hash = current_concepts_input_hash

    def add_everything(self, skip_concepts_and_related_works=False):
        self.delete_dict = defaultdict(list)
        self.insert_dicts = []

        if self.merge_into_id:
            # don't add relation table entries for merged works
            logger.info(f"not updating W{self.paper_id} because it was merged into W{self.merge_into_id}")
            return

        if not self.records_sorted:
            # not associated with a record, update institutions only
            logger.info(f"No associated records for {self.paper_id}, skipping most updates")

            update_institutions = False
            # fix for many works assigned to 4362561690
            for aff in self.affiliations:
                if aff.affiliation_id == 4362561690:
                    update_institutions = True
                    break
            if update_institutions:
                start_time = time()
                self.update_institutions()
                logger.info(f'update_institutions took {elapsed(start_time, 2)} seconds')
            return

        start_time = time()
        self.set_fields_from_all_records()
        logger.info(f'set_fields_from_all_records took {elapsed(start_time, 2)} seconds')

        start_time = time()
        self.add_abstract()  # must be before work_concepts
        logger.info(f'add_abstract took {elapsed(start_time, 2)} seconds')

        start_time = time()
        self.add_mesh()
        logger.info(f'add_mesh took {elapsed(start_time, 2)} seconds')

        start_time = time()
        self.add_ids()
        logger.info(f'add_ids took {elapsed(start_time, 2)} seconds')

        start_time = time()
        self.add_locations()
        logger.info(f'add_locations took {elapsed(start_time, 2)} seconds')

        start_time = time()
        self.add_references()  # must be before affiliations
        logger.info(f'add_references took {elapsed(start_time, 2)} seconds')
        if not skip_concepts_and_related_works:
            start_time = time()
            self.add_work_concepts()
            logger.info(f'add_work_concepts took {elapsed(start_time, 2)} seconds')

            # After initial burst, need to move this here becauase topics is slow
            start_time = time()
            self.add_work_topics()
            logger.info(f'add_work_topics took {elapsed(start_time, 2)} seconds')

            start_time = time()
            self.add_related_works()  # must be after work_concepts
            logger.info(f'add_related_works took {elapsed(start_time, 2)} seconds')

        start_time = time()
        self.add_funders()
        logger.info(f'add_funders took {elapsed(start_time, 2)} seconds')

        start_time = time()
        self.add_sdgs()
        logger.info(f'add_sdgs took {elapsed(start_time, 2)} seconds')

        # for now, only add/update affiliations if they aren't there
        start_time = time()
        if not self.affiliations:
            logger.info("adding affiliations because work didn't have any yet")
            self.add_affiliations()
            logger.info(f'add_affiliations took {elapsed(start_time, 2)} seconds')
        else:
            logger.info("not adding affiliations because work already has some set, but updating institutions")
            self.update_institutions()
            logger.info(f'update_institutions took {elapsed(start_time, 2)} seconds')
            logger.info("updating orcid")
            self.update_orcid()
            logger.info(f'update_orcid took {elapsed(start_time, 2)} seconds')

    def add_funders(self):
        self.full_updated_date = datetime.datetime.utcnow().isoformat()
        for record in self.records_merged:
            if record.record_type != "crossref_doi":
                continue

            new_funders = []
            record_funders = json.loads(record.funders) if record.funders else []
            if not record_funders:
                return

            json_funders_by_doi = {}
            for f in record_funders:
                if "DOI" in f:
                    f["DOI"] = f["DOI"].strip().lower()
                    if f["DOI"]:
                        json_funders_by_doi[f["DOI"]] = f

            funders = db.session.query(models.Funder).filter(
                models.Funder.doi.in_(list(json_funders_by_doi.keys()))
            ).options(orm.Load(models.Funder).raiseload('*')).all()

            seen_funders = set()
            for f in funders:
                if f.funder_id not in seen_funders:
                    seen_funders.add(f.funder_id)
                    new_funders.append(models.WorkFunder(
                        paper_id=self.paper_id,
                        funder_id=f.funder_id,
                        award=json_funders_by_doi.get(f.doi).get("award", [])
                    ))

            def work_funder_dict(funder):
                return {
                    'f': funder.funder_id,
                    'a': sorted(funder.award or [])
                }

            if struct_changed(
                [work_funder_dict(wf) for wf in sorted(self.funders, key=lambda fun: fun.funder_id)],
                [work_funder_dict(wf) for wf in sorted(new_funders, key=lambda fun: fun.funder_id)]
            ):
                self.funders = new_funders

    def add_sdgs(self):
        if not self.sdg:
            get_and_save_sdgs(self)

    def add_related_works(self):
        if not hasattr(self, "concepts_for_related_works"):
            if self.concepts:
                self.concepts_for_related_works = [c.field_of_study for c in self.concepts if c.score > .3]
            else:
                return

        if not self.concepts_for_related_works:
            return

        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        matching_papers_sql = """
        with fos as (
            select
            field_of_study_id,
            num_papers
            from mid.concept_for_api_mv
            join mid.num_papers_by_concept_mv
            on concept_for_api_mv.field_of_study_id = num_papers_by_concept_mv.field_of_study
            where concept_for_api_mv.field_of_study_id in %s
        )
        select
            paper_id as related_paper_id,
            avg(score) as average_related_score,
            sum(score) as total_score
        from
            (select field_of_study_id from fos order by num_papers limit 5) rare_fos
            cross join lateral (
                select paper_id, field_of_study, score
                from mid.work_concept wc
                where wc.field_of_study = rare_fos.field_of_study_id
                and wc.score > .3 order by score desc limit 1000
            ) papers_by_fos
        group by paper_id
        order by total_score desc
        limit 10;
        """

        with get_db_cursor() as cur:
            cur.execute(matching_papers_sql, (tuple(self.concepts_for_related_works), ))
            rows = cur.fetchall()

            self.related_works = [
                models.WorkRelatedWork(
                    paper_id=self.paper_id,
                    recommended_paper_id=row["related_paper_id"],
                    score=row["average_related_score"] * row["average_related_score"],
                    updated=datetime.datetime.utcnow().isoformat()
                )
                for row in rows
            ]

    def add_abstract(self):
        self.abstract_indexed_abstract = None
        self.full_updated_date = datetime.datetime.utcnow().isoformat()
        for record in self.records_merged:
            if record.abstract:
                indexed_abstract = f_generate_inverted_index(record.abstract)
                if len(indexed_abstract) >= 60000:
                    # truncate the abstract if too long
                    indexed_abstract = f_generate_inverted_index(record.abstract[0:30000])
                insert_dict = {
                    "paper_id": self.paper_id,
                    "indexed_abstract": indexed_abstract
                }
                self.abstract_indexed_abstract = indexed_abstract
                if not self.abstract or (self.abstract_indexed_abstract != self.abstract.indexed_abstract):
                    self.abstract = models.Abstract(**insert_dict)
                return

    def add_mesh(self):
        new_mesh = []
        self.full_updated_date = datetime.datetime.utcnow().isoformat()
        for record in self.records_merged:
            if record.mesh:
                mesh_dict_list = json.loads(record.mesh)
                mesh_objects = [models.Mesh(**mesh_dict) for mesh_dict in mesh_dict_list]
                for mesh_object in mesh_objects:
                    if mesh_object.qualifier_ui is None:
                        mesh_object.qualifier_ui = ""  # can't be null for primary key
                new_mesh = mesh_objects

                def mesh_properties(m):
                    return (
                        m.get("is_major_topic"),
                        m.get("descriptor_ui"),
                        m.get("descriptor_name"),
                        m.get("qualifier_ui"),
                        m.get("qualifier_name"),
                    )
                old_mesh_dicts = sorted([m.to_dict() for m in self.mesh], key=mesh_properties)
                new_mesh_dicts = sorted([m.to_dict() for m in new_mesh], key=mesh_properties)

                if struct_changed(old_mesh_dicts, new_mesh_dicts):
                    self.mesh = new_mesh

                return

    def add_ids(self):
        arxiv_repository_id = "ca8f8d56758a80a4f86"
        already_found_pmid = False
        for record in self.records_merged:
            # pmid
            if record.pmid and (already_found_pmid is False):
                self.full_updated_date = datetime.datetime.utcnow().isoformat()
                # self.insert_dicts += [{"WorkExtraIds": {"paper_id": self.paper_id, "attribute_type": 2, "attribute_value": record.pmid}}]
                if record.pmid not in [extra.attribute_value for extra in self.extra_ids if extra.attribute_type==2]:
                    self.extra_ids += [models.WorkExtraIds(paper_id=self.paper_id, attribute_type=2, attribute_value=record.pmid)]
                already_found_pmid = True
            # arxiv_id
            if (record.arxiv_id) and (record.repository_id is arxiv_repository_id or (not self.arxiv_id)):
                self.full_updated_date = datetime.datetime.utcnow().isoformat()
                self.arxiv_id = record.arxiv_id

    def guess_version(self):
        # some last-minute rules to try to guess the location's version
        if self.type_crossref == 'posted-content':
            return 'submittedVersion'
        return None

    def add_locations(self):
        from models.location import get_repository_institution_from_source_url
        new_locations = []
        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        records_with_unpaywall = [
            record for record in self.records_sorted
            if hasattr(record, "unpaywall") and record.unpaywall
        ]
        logger.info(f'{len(records_with_unpaywall)} - records_with_unpaywall')

        if not records_with_unpaywall:
            return

        record_to_use = records_with_unpaywall[0]

        for unpaywall_oa_location in record_to_use.unpaywall.oa_locations:
            insert_dict = {
                "paper_id": self.id,
                "endpoint_id": unpaywall_oa_location["endpoint_id"],
                "evidence": unpaywall_oa_location["evidence"],
                "host_type": unpaywall_oa_location["host_type"],
                "is_best": unpaywall_oa_location["is_best"],
                "oa_date": unpaywall_oa_location["oa_date"],
                "pmh_id": unpaywall_oa_location["pmh_id"],
                "repository_institution": unpaywall_oa_location["repository_institution"],
                "updated": unpaywall_oa_location["updated"],
                "source_url": unpaywall_oa_location["url"],
                "url": unpaywall_oa_location["url"],
                "url_for_landing_page": unpaywall_oa_location["url_for_landing_page"],
                "url_for_pdf": unpaywall_oa_location["url_for_pdf"],
                "version": unpaywall_oa_location["version"],
                "license": unpaywall_oa_location["license"],
            }

            if get_repository_institution_from_source_url(unpaywall_oa_location["url"]):
                insert_dict["repository_institution"] = get_repository_institution_from_source_url(unpaywall_oa_location["url"])

            if insert_dict["evidence"] == "oa repository (via pmcid lookup)":
                insert_dict["endpoint_id"] = "daaf77eacc58eec31bb"

            if insert_dict["endpoint_id"] == "ca8f8d56758a80a4f86":
                # special case for arXiv
                insert_dict["doi"] = insert_dict["pmh_id"].replace(
                    'oai:arXiv.org:',
                    '10.48550/arxiv.'
                )
            
            if not insert_dict["version"]:
                insert_dict["version"] = self.guess_version()

            new_locations += [models.Location(**insert_dict)]

            def location_sort_key(loc):
                return (
                    loc.endpoint_id or '',
                    loc.source_url or '',
                    loc.url_for_landing_page or '',
                    loc.url_for_pdf or ''
                )

            old_locs = [location_sort_key(loc) for loc in sorted(self.locations, key=location_sort_key)]
            new_locs = [location_sort_key(loc) for loc in sorted(new_locations, key=location_sort_key)]

            if struct_changed(old_locs, new_locs):
                self.locations = new_locations
                logger.info(f'Updated {len(new_locs)} locations')

    def add_references(self):
        from models import WorkExtraIds
        citation_dois = []
        citation_pmids = []
        citation_paper_ids = []

        self.citation_paper_ids = []

        reference_source_num = 0
        for record in self.records_merged:
            if record.citations:
                new_references_unmatched = []
                self.full_updated_date = datetime.datetime.utcnow().isoformat()

                try:
                    citation_dict_list = json.loads(record.citations)
                    for citation_dict in citation_dict_list:
                        reference_source_num += 1
                        if "doi" in citation_dict:
                            my_clean_doi = clean_doi(citation_dict["doi"], return_none_if_error=True)
                            if my_clean_doi:
                                citation_dois += [my_clean_doi]
                        elif "pmid" in citation_dict:
                            my_clean_pmid = citation_dict["pmid"]
                            if my_clean_pmid:
                                citation_pmids += [my_clean_pmid]
                        else:
                            new_references_unmatched += [
                                models.CitationUnmatched(
                                    reference_sequence_number=reference_source_num,
                                    raw_json=json.dumps(citation_dict)
                                )
                            ]

                    if struct_changed(
                        [ref.raw_json for ref in sorted(self.references_unmatched, key=lambda um: um.reference_sequence_number)],
                        [json.loads(ref.raw_json or '') for ref in sorted(new_references_unmatched, key=lambda um: um.reference_sequence_number)]
                    ):
                        self.references_unmatched = new_references_unmatched


                except Exception as e:
                    logger.exception(f"error json parsing citations, but continuing on other papers {self.paper_id} {e}")

        if citation_dois:
            works = db.session.query(Work).options(orm.Load(Work).raiseload('*')).filter(Work.doi_lower.in_(citation_dois)).all()
            for my_work in works:
                my_work.full_updated_date = datetime.datetime.utcnow().isoformat()
            citation_paper_ids += [work.merge_into_id or work.paper_id for work in works if work.paper_id]
        if citation_pmids:
            work_ids = db.session.query(WorkExtraIds).options(orm.Load(WorkExtraIds).selectinload(models.WorkExtraIds.work).raiseload('*')).filter(WorkExtraIds.attribute_type==2, WorkExtraIds.attribute_value.in_(citation_pmids)).all()
            for my_work_id in work_ids:
                if my_work_id.work:
                    my_work_id.work.full_updated_date = datetime.datetime.utcnow().isoformat()
            citation_paper_ids += [
                work_id.work.merge_into_id or work_id.work.paper_id
                for work_id in work_ids if work_id and work_id.work
            ]

        citation_paper_ids = list(set(citation_paper_ids))
        if citation_paper_ids:
            new_references = [models.Citation(paper_reference_id=reference_id) for reference_id in citation_paper_ids]
            if struct_changed([c.paper_reference_id for c in self.references], citation_paper_ids):
                self.references = new_references
        if citation_paper_ids:
            self.citation_paper_ids = citation_paper_ids  # used for matching authors right now

    def add_affiliations(self, affiliation_retry_attempts=30):
        self.affiliations = []
        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        records_with_affiliations = [record for record in self.records_sorted if record.has_affiliations]
        if not records_with_affiliations:
            records_with_affiliations = [record for record in self.records_sorted if record.authors]
        if not records_with_affiliations:
            logger.info("no affiliation data found in any of the records")
            return

        record = records_with_affiliations[0]

        for author_sequence_order, author_dict in enumerate(record.authors_json):
            original_name = author_dict["raw"]
            if author_dict["family"]:
                original_name = "{} {}".format(author_dict["given"], author_dict["family"])
            if not author_dict["affiliation"]:
                author_dict["affiliation"] = [defaultdict(str)]

            raw_author_string = original_name if original_name else None
            original_orcid = normalize_orcid(author_dict["orcid"]) if author_dict["orcid"] else None

            seen_institution_ids = set()

            affiliation_sequence_order = 1
            for affiliation_dict in author_dict["affiliation"]:
                raw_affiliation_string = affiliation_dict["name"] if affiliation_dict["name"] else None
                raw_affiliation_string = clean_html(raw_affiliation_string)
                my_institutions = []

                if raw_affiliation_string:
                    institution_id_matches = models.Institution.get_institution_ids_from_strings(
                        [raw_affiliation_string], retry_attempts=affiliation_retry_attempts
                    )
                    for institution_id_match in [m for m in institution_id_matches[0] if m]:
                        my_institution = models.Institution.query.options(
                            orm.Load(models.Institution).raiseload('*')
                        ).get(institution_id_match)

                        if (
                            my_institution and my_institution.affiliation_id
                            and my_institution.affiliation_id in seen_institution_ids
                        ):
                            continue
                        my_institutions.append(my_institution)
                        seen_institution_ids.add(my_institution.affiliation_id)

                # comment this out for now because it is too slow for some reason, but later comment it back in
                # if my_institution:
                #     my_institution.full_updated_date = datetime.datetime.utcnow().isoformat()  # citations and fields

                my_institutions = my_institutions or [None]

                # TODO: set last known affiliation from AND v3
                # if any(my_institutions) and my_author:
                #     author_last_known_affiliation_date = None
                #
                #     if my_author.last_known_affiliation_id_date:
                #         author_last_known_affiliation_date = my_author.last_known_affiliation_id_date
                #         if isinstance(author_last_known_affiliation_date, datetime.datetime):
                #             author_last_known_affiliation_date = author_last_known_affiliation_date.isoformat()[0:10]
                #     if (
                #         not my_author.last_known_affiliation_id
                #         or not author_last_known_affiliation_date
                #         or (self.publication_date and self.publication_date >= author_last_known_affiliation_date)
                #     ):
                #         my_author.last_known_affiliation_id = [m for m in my_institutions if m][0].affiliation_id
                #         my_author.last_known_affiliation_id_date = self.publication_date

                if raw_author_string or raw_affiliation_string:
                    for my_institution in my_institutions:
                        my_affiliation = models.Affiliation(
                            author_sequence_number=author_sequence_order+1,
                            affiliation_sequence_number=affiliation_sequence_order,
                            original_author=raw_author_string,
                            original_affiliation=raw_affiliation_string[:2500] if raw_affiliation_string else None,
                            original_orcid=original_orcid,
                            match_institution_name=models.Institution.matching_institution_name(raw_affiliation_string),
                            is_corresponding_author=author_dict.get('is_corresponding'),
                            updated_date=datetime.datetime.utcnow().isoformat()
                        )
                        my_affiliation.institution = my_institution
                        self.affiliations.append(my_affiliation)
                        affiliation_sequence_order += 1

        return

    def update_oa_status_if_better(self, new_oa_status):
        # update oa_status, only if it's better than the oa_status we already have
        try:
            new_oa_status_enum = OAStatusEnum[new_oa_status.lower()]
            old_oa_status_enum = OAStatusEnum[self.oa_status.lower()] if self.oa_status else OAStatusEnum['closed']
            if old_oa_status_enum.name in ['green', 'bronze'] and new_oa_status_enum.name in ['green', 'bronze']:
                # I'm not comfortable making any change in this case. Leave it alone
                return self.oa_status
            elif new_oa_status_enum > old_oa_status_enum:
                return new_oa_status
        except (AttributeError, KeyError):
            # probably an invalid new_oa_status
            logger.info(f"invalid new_oa_status for {self.paper_id}. old: {self.oa_status} new: {new_oa_status}")
        # if we didn't update above, return the existing oa_status
        return self.oa_status

    def set_fields_from_record(self, record):
        from util import normalize_doi

        if not self.created_date:
            self.created_date = datetime.datetime.utcnow().isoformat()
        self.original_title = record.title[:60000] if record.title else self.original_title
        self.paper_title = normalize_simple(self.original_title, remove_articles=False, remove_spaces=False)
        self.unpaywall_normalize_title = record.normalized_title[:60000] if record.normalized_title else None
        self.updated_date = datetime.datetime.utcnow().isoformat()
        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        self.original_venue = record.venue_name
        if record.journal:
            self.journal_id = record.journal.journal_id
            self.original_venue = record.journal.display_name  # overwrite record.venue_name if have a normalized name
            self.publisher = record.journal.publisher

            # don't include line below, it makes sqlalchemy errors, handle another way
            # self.journal.full_updated_date = datetime.datetime.utcnow().isoformat() # because its citation count has changed

        self.doi = normalize_doi(record.doi, return_none_if_error=True)
        self.doi_lower = self.doi
        self.publication_date = record.published_date.isoformat()[0:10] if record.published_date else None
        self.year = int(record.published_date.isoformat()[0:4]) if record.published_date else None

        self.volume = record.volume
        self.issue = record.issue
        self.first_page = record.first_page
        self.last_page = record.last_page
        self.doc_sub_types = "Retracted" if record.is_retracted else None
        self.genre = record.normalized_work_type
        self.doc_type = record.normalized_doc_type
        self.best_url = record.record_webpage_url

        if hasattr(record, "unpaywall") and record.unpaywall:
            self.is_paratext = record.unpaywall.is_paratext
            self.oa_status = self.update_oa_status_if_better(record.unpaywall.oa_status)  # this isn't guaranteed to be accurate, since it may be changed in to_dict()
            self.best_free_url = record.unpaywall.best_oa_location_url
            self.best_free_version = record.unpaywall.best_oa_location_version

    def set_fields_from_override_record(self, override):
        from util import normalize_doi

        self.updated_date = datetime.datetime.utcnow().isoformat()
        if override.title:
            self.original_title = override.title
            self.paper_title = normalize_simple(self.original_title, remove_articles=False, remove_spaces=False)

        if override.venue_name:
            self.original_venue = override.venue_name
        if override.journal:
            self.journal_id = override.journal.journal_id
            self.original_venue = override.journal.display_name  # overwrite override.venue_name if have a normalized name
            self.publisher = override.journal.publisher

            # don't include line below, it makes sqlalchemy errors, handle another way
            # self.journal.full_updated_date = datetime.datetime.utcnow().isoformat() # because its citation count has changed

        if override.doi:
            self.doi = normalize_doi(override.doi, return_none_if_error=True)
            self.doi_lower = self.doi
        if override.published_date:
            self.publication_date = override.published_date.isoformat()[0:10]
            self.year = int(override.published_date.isoformat()[0:4])

        if override.is_retracted:
            self.doc_sub_types = "Retracted"

        # mapping of recordthresher_record fields to Work (self) fields:
        other_allowed_overrides = {
            "volume": "volume",
            "issue": "issue",
            "first_page": "first_page",
            "last_page": "last_page",
            "normalized_work_type": "genre",
            "normalized_doc_type": "doc_type",
            "record_webpage_url": "best_url",
        }
        for rt_field, work_field in other_allowed_overrides.items():
            override_val = getattr(override, rt_field, None)
            if override_val:
                setattr(self, work_field, override_val)

    @cached_property
    def looks_like_paratext(self):
        if self.is_paratext:
            return True

        paratext_exprs = [
            r'^Author Guidelines$',
            r'^Author Index$'
            r'^Back Cover',
            r'^Back Matter',
            r'^Contents$',
            r'^Contents:',
            r'^Cover Image',
            r'^Cover Picture',
            r'^Editorial Board',
            r'Editor Report$',
            r'^Front Cover',
            r'^Frontispiece',
            r'^Graphical Contents List$',
            r'^Index$',
            r'^Inside Back Cover',
            r'^Inside Cover',
            r'^Inside Front Cover',
            r'^Issue Information',
            r'^List of contents',
            r'^List of Tables$',
            r'^List of Figures$',
            r'^List of Plates$',
            r'^Masthead',
            r'^Pages de début$',
            r'^Title page',
            r"^Editor's Preface",
        ]

        for expr in paratext_exprs:
            if self.work_title and re.search(expr, self.work_title, re.IGNORECASE):
                return True

        return False

    @cached_property
    def guess_type_from_title(self):
        erratum_exprs = [
            r'^erratum',
        ]
        for expr in erratum_exprs:
            if self.work_title and re.search(expr, self.work_title, re.IGNORECASE):
                return "erratum"

        letter_exprs = [
            r'^letter:',
            r'^letter to',
            r'^letter$',
            r'^\[letter to',
        ]
        for expr in letter_exprs:
            if self.work_title and re.search(expr, self.work_title, re.IGNORECASE):
                return "letter"

        editorial_exprs = [
            r'^editorial:',
            r'^editorial$',
            r'^editorial comment',
            r'^guest editorial',
            r'^editorial note',
            r'^editorial -'
        ]
        for expr in editorial_exprs:
            if self.work_title and re.search(expr, self.work_title, re.IGNORECASE):
                return "editorial"

        return None

    @property
    def records_sorted(self):
        if not self.records_merged:
            return []

        return sorted([
            r for r in self.records_merged if r.is_primary_record()],
            key=lambda x: x.score, reverse=True
        ) or []

    @property
    def records_merged(self):
        return [r.with_parsed_data for r in self.records or [] if r.with_parsed_data]

    def set_fields_from_all_records(self):
        self.updated_date = datetime.datetime.utcnow().isoformat()
        self.full_updated_date = datetime.datetime.utcnow().isoformat()
        self.finished = datetime.datetime.utcnow().isoformat()

        # go through them with oldest first, and least reliable record type to most reliable, overwriting
        if not self.records_sorted:
            return
        records = [r for r in self.records_sorted]
        records.reverse()

        logger.info(f"my records: {records}")

        for record in records:
            if record.record_type == "pmh_record":
                self.set_fields_from_record(record)

        for record in records:
            if record.record_type == "pubmed_record":
                self.set_fields_from_record(record)

        for record in records:
            if record.record_type == "datacite_doi":
                self.set_fields_from_record(record)

        for record in records:
            if record.record_type == "crossref_doi":
                self.set_fields_from_record(record)

        for record in records:
            if record.record_type == "override":
                self.set_fields_from_override_record(record)

        # self.delete_dict["Work"] += [self.paper_id]
        insert_dict = {}
        work_insert_fieldnames = Work.__table__.columns.keys()
        for key in work_insert_fieldnames:
            # insert_dict[key] = getattr(self, key)
            setattr(self, key, getattr(self, key))
        # self.insert_dicts += [{"Work": insert_dict}]

    @cached_property
    def is_retracted(self):
        if self.doc_sub_types != None:
            return True
        elif self.retraction_watch and self.retraction_watch.is_retracted:
            return True
        return False

    @property
    def affiliations_sorted(self):
        return sorted(self.affiliations, key=lambda x: x.author_sequence_number)

    @cached_property
    def mesh_sorted(self):
        # sort so major topics at the top and the rest is alphabetical
        return sorted(self.mesh, key=lambda x: (not x.is_major_topic, x.descriptor_name), reverse=False)

    @property
    def affiliations_list(self):
        affiliations = [affiliation for affiliation in self.affiliations_sorted]
        if not affiliations:
            return []

        # it seems like sometimes there are 0s and sometimes 1st, so figure out the minimum
        first_author_sequence_number = min([affil.author_sequence_number for affil in affiliations])
        last_author_sequence_number = max([affil.author_sequence_number for affil in affiliations])
        affiliation_dict = defaultdict(list)
        for affil in affiliations:
            affil.author_position = "middle"
            if affil.author_sequence_number == first_author_sequence_number:
                affil.author_position = "first"
            elif affil.author_sequence_number == last_author_sequence_number:
                affil.author_position = "last"
            affiliation_dict[affil.author_sequence_number] += [affil.to_dict("minimum")]
        response = []
        for seq, affil_list in affiliation_dict.items():
            institution_list = [a["institution"] for a in affil_list if a["institution"].get("id") is not None]
            if institution_list == [{}]:
                institution_list = []
            if len(affiliation_dict) == 1:
                # override - single author is always corresponding
                is_corresponding = True
            else:
                is_corresponding = affil_list[0].get('is_corresponding_author', False)

            raw_affiliation_strings = sorted(list(set([
                                a.get("raw_affiliation_string") for a in affil_list
                                if a.get("raw_affiliation_string")
                             ])))
            raw_affiliation_string = '; '.join(sorted(list(set([
                                 a.get("raw_affiliation_string") for a in affil_list
                                 if a.get("raw_affiliation_string")
                             ]))))
            # add countries
            if institution_list:
                countries_list = [a["institution"]["country_code"] for a in affil_list if a["institution"].get("country_code") is not None]
                countries = sorted(list(set(countries_list)))
            elif not institution_list and raw_affiliation_string:
                countries = self.get_countries_from_raw_affiliation(raw_affiliation_string)
            else:
                countries = []

            response_dict = {"author_position": affil_list[0]["author_position"],
                             "author": affil_list[0]["author"],
                             "institutions": institution_list,
                             "countries": countries,
                             "country_ids": [f"{COUNTRIES_ENDPOINT_PREFIX}/{c}" for c in countries],
                             "is_corresponding": is_corresponding,
                             "raw_author_name": affil_list[0]["raw_author_name"],
                             "raw_affiliation_strings": raw_affiliation_strings,
                             "raw_affiliation_string": raw_affiliation_string
                     }
            response.append(response_dict)
        return response

    @cached_property
    def institutions_distinct(self):
        # return set of unique institutions for the authorships in this work
        institution_ids = []
        for affil in self.affiliations_list:
            for inst in affil['institutions']:
                if inst.get('id'):
                    institution_ids.append(inst['id'])
        return set(institution_ids)
        

    @classmethod
    def author_match_names_from_record_json(cls, record_author_json):
        author_match_names = []
        if not record_author_json:
            return []
        author_dict_list = json.loads(record_author_json)

        for author_sequence_order, author_dict in enumerate(author_dict_list):
            original_name = author_dict["raw"]
            if author_dict["family"]:
                original_name = "{} {}".format(author_dict["given"], author_dict["family"])

            raw_author_string = original_name if original_name else None
            author_match_strings = models.Author.matching_author_strings(raw_author_string)
            if author_match_strings:
                author_match_names.extend(author_match_strings)
        return author_match_names

    def matches_authors_in_record(self, record_author_json):
        # returns True if either of them are missing authors, or if the authors match
        # returns False if both have authors but neither the first nor last author in the Work is in the author string

        if not record_author_json:
            logger.info("no record_author_json, so not trying to match")
            return True
        if record_author_json == '[]':
            logger.info("no record_author_json, so not trying to match")
            return True
        if not self.affiliations:
            logger.info("no self.affiliations, so not trying to match")
            return True
        logger.info(f"trying to match existing work {self.id} {self.doi_lower} with record authors")

        for original_name in [self.first_author_original_name, self.last_author_original_name]:
            logger.info(f"original_name: {original_name}")
            if original_name:
                author_match_strings = models.Author.matching_author_strings(original_name)
                logger.info(f"author_match_strings: {author_match_strings}")
                for author_match_string in author_match_strings:
                    logger.info(f"author_match_string: {author_match_string}")
                    match_names_from_record_json = Work.author_match_names_from_record_json(record_author_json)
                    logger.info(f"match_names_from_record_json: {match_names_from_record_json}")
                    if author_match_string and (author_match_string in match_names_from_record_json):
                        logger.info("author match!")
                        return True

        logger.info("author no match")
        return False

    @cached_property
    def first_author_original_name(self):
        if not self.affiliations:
            return None
        return self.affiliations_sorted[0].original_author

    @cached_property
    def last_author_original_name(self):
        if not self.affiliations:
            return None
        return self.affiliations_sorted[-1].original_author

    @property
    def concepts_sorted(self):
        return sorted(self.concepts, key=lambda x: x.score, reverse=True)
       
    # @property
    # def topics_fields_subfields_domains(self):
    #     return [topic.to_dict("minimum") for topic in self.topics_sorted] if self.topics_sorted else []

    @property
    def topics_sorted(self):
        return sorted(self.topics, key=lambda x: x.score, reverse=True)

    # @property
    # def primary_topic(self):
    #     return self.topics_sorted[:1] if self.topics_sorted else []

    # @property
    # def subfields_sorted(self):
    #     self.subfields = [dict(t) for t in {tuple(d.items()) for d in 
    #                                         [{'id':x['subfield']['id'], 'display_name': x['subfield']['display_name']} 
    #                                          for x in self.topics_fields_subfields_domains]}] if self.topics_sorted else []
    #     return sorted(self.subfields, key=lambda x: x['id'], reverse=False)
    
    # @property
    # def fields_sorted(self):
    #     self.fields = [dict(t) for t in {tuple(d.items()) for d in 
    #                                      [{'id':x['field']['id'], 'display_name': x['field']['display_name']} 
    #                                       for x in self.topics_fields_subfields_domains]}] if self.topics_sorted else []
    #     return sorted(self.fields, key=lambda x: x['id'], reverse=False)
    
    # @property
    # def domains_sorted(self):
    #     self.domains = [dict(t) for t in {tuple(d.items()) for d in 
    #                                        [{'id':x['domain']['id'], 'display_name': x['domain']['display_name']} 
    #                                         for x in self.topics_fields_subfields_domains]}] if self.topics_sorted else []
    #     return sorted(self.domains, key=lambda x: x['id'], reverse=False)
    
    @property
    def locations_sorted(self):
        return sorted(self.locations, key=lambda x: x.score, reverse=True)

    @property
    def mag_publisher(self):
        return self.publisher

    @property
    def work_title(self):
        return self.original_title

    @property
    def work_id(self):
        return self.paper_id

    @property
    def doi_url(self):
        if not self.doi:
            return None
        return "https://doi.org/{}".format(self.doi.lower())

    @property
    def is_closed_springer(self):
        publisher_str = (self.journal and self.journal.publisher) or self.publisher
        if publisher_str and 'springer' in publisher_str.lower():
            return not self.is_oa
        return False

    @cached_property
    def is_oa(self):
        return True if self.oa_locations else False

    @cached_property
    def type_crossref(self):
        # legacy type used < 2023-08
        # (but don't get rid of it, it's used to derive the new type (display_genre))
        if self.looks_like_paratext:
            return "other"
        if self.genre:
            return self.genre
        if self.doc_type:
            lookup_mag_to_crossref_type = {
                "Journal": "journal-article",
                "Thesis": "dissertation",
                "Conference": "proceedings-article",
                "Repository": "posted-content",
                "Book": "book",
                "BookChapter": "book-chapter",
                "Dataset": "dataset",
            }
            if mag_type := lookup_mag_to_crossref_type.get(self.doc_type):
                return mag_type
        if self.journal and self.journal.type and 'book' in self.journal.type:
            return 'book-chapter'
        return 'journal-article'

    @cached_property
    def display_genre(self):
        # this is what goes into the `Work.type` attribute
        if self.looks_like_paratext:
            return "paratext"
        # infer "erratum", "editorial", "letter" types:
        try:
            if self.guess_type_from_title:
                # todo: do another pass at this. improve precision and recall.
                return self.guess_type_from_title
        except AttributeError:
            pass
        lookup_crossref_to_openalex_type = {
            "journal-article": "article",
            "proceedings-article": "article",
            "posted-content": "article",
            "book-part": "book-chapter",
            "journal-issue": "paratext",
            "journal": "paratext",
            "journal-volume": "paratext",
            "report-series": "paratext",
            "proceedings": "paratext",
            "proceedings-series": "paratext",
            "book-series": "paratext",
            "component": "paratext",
            "monograph": "book",
            "reference-book": "book",
            "book-set": "book",
            "edited-book": "book",
        }
        # return mapping from lookup if it's in there, otherwise pass-through
        return lookup_crossref_to_openalex_type.get(self.type_crossref, self.type_crossref)

    @cached_property
    def language(self):
        abstract_words = []
        if self.abstract and self.abstract.indexed_abstract:
            json_abstract = json.loads(self.abstract.indexed_abstract)
            abstract_words = list(json_abstract.get('InvertedIndex', {}).keys())
        return detect_language_from_abstract_and_title(abstract_words, self.original_title)

    @cached_property
    def references_list(self):

        reference_paper_ids = [as_work_openalex_id(reference.paper_reference_id) for reference in self.references]
        return reference_paper_ids

        # objs = db.session.query(Work).options(
        #      selectinload(Work.journal),
        #      selectinload(Work.extra_ids),
        #      selectinload(Work.affiliations).selectinload(models.Affiliation.author).selectinload(models.Author.orcids),
        #      selectinload(Work.affiliations).selectinload(models.Affiliation.institution).selectinload(models.Institution.ror),
        #      orm.Load(Work).raiseload('*')).filter(Work.paper_id.in_(reference_paper_ids)).all()
        # response = [obj.to_dict("minimum") for obj in objs]
        # return response

    @property
    def apc_paid(self):
        """Display OpenAPC if it exists, then fall back to DOAJ."""
        first_doaj_apc = (
            self.journal.apc_prices_with_0[0] if self.journal and self.journal.apc_prices_with_0 else None
        )
        doaj_apc_in_usd = self.journal.apc_usd_with_0 if self.journal else None

        if self.openapc:
            return {
                "value": self.openapc.apc_in_euro,
                "currency": "EUR",
                "value_usd": self.openapc.apc_in_usd,
                "provenance": "openapc",
            }
        elif first_doaj_apc:
            return {
                "value": first_doaj_apc.get("price", None),
                "currency": first_doaj_apc.get("currency", None),
                "value_usd": doaj_apc_in_usd,
                "provenance": "doaj",
            }

    @property
    def apc_list(self):
        """Display first DOAJ APC."""
        first_doaj_apc = (
            self.journal.apc_prices_with_0[0] if self.journal and self.journal.apc_prices_with_0 else None
        )
        doaj_apc_in_usd = self.journal.apc_usd_with_0 if self.journal else None
        if first_doaj_apc:
            return {
                "value": first_doaj_apc.get("price", None),
                "currency": first_doaj_apc.get("currency", None),
                "value_usd": doaj_apc_in_usd,
                "provenance": "doaj",
            }

    @property
    def sustainable_development_goals(self):
        formatted_sdgs = []
        threshold = 0.4
        if self.sdg and self.sdg.predictions:
            for sdg_prediction in self.sdg.predictions:
                score = sdg_prediction.get("prediction")
                sdg = sdg_prediction.get("sdg")
                if score and sdg and score > threshold:
                    # validate sdg fields exist
                    if sdg.get("id") and sdg.get("name"):

                        # override some names
                        if sdg.get("name") == "Peace, Justice and strong institutions":
                            sdg["name"] = "Peace, justice, and strong institutions"
                        elif sdg.get("name") == "Quality Education":
                            sdg["name"] = "Quality education"
                        elif sdg.get("name") == "Life in Land":
                            sdg["name"] = "Life on land"

                        formatted_sdgs.append(
                            {
                                "id": sdg.get("id"),
                                "display_name": sdg.get("name"),
                                "score": round(score, 2),
                            }
                        )
        return formatted_sdgs
    
    @property
    def all_work_keywords(self):
        formatted_keywords = []
        if self.work_keywords and self.work_keywords.keywords:
            for keyword in self.work_keywords.keywords:
                score = keyword.get("score")
                keyword = keyword.get("keyword")
                formatted_keywords.append(
                    {
                        "keyword": keyword.lower() if keyword else None,
                        "score": score
                    }
                )
        return formatted_keywords

    @property
    def cited_by_percentile_year(self):
        if not self.year:
            return None

        year = max(self.year, 1920)
        citation_count = self.counts.citation_count if self.counts else 0

        base_query = models.CitationPercentilesByYear.query.filter_by(year=year)

        exact_row = base_query.filter_by(citation_count=citation_count).first()
        higher_row = base_query.filter(models.CitationPercentilesByYear.citation_count > citation_count) \
            .order_by(models.CitationPercentilesByYear.citation_count.asc()) \
            .first()

        # sometimes cited_by_count is higher than the highest row in the table. In that case return highest row in table.
        if higher_row is None:
            higher_row = base_query.order_by(models.CitationPercentilesByYear.citation_count.desc()) \
                .first()
            if higher_row and higher_row.citation_count > citation_count:
                higher_row = None

        if exact_row:
            return self.format_percentiles(exact_row.percentile, higher_row.percentile if higher_row else exact_row.percentile)

        # try closest lower row
        lower_row = base_query.filter(models.CitationPercentilesByYear.citation_count < citation_count) \
            .order_by(models.CitationPercentilesByYear.citation_count.desc()) \
            .first()

        if not lower_row or not higher_row:
            logger.info(f"no percentiles for {self.paper_id} {self.year} {citation_count}")
            return None

        return self.format_percentiles(lower_row.percentile, higher_row.percentile)

    @staticmethod
    def format_percentiles(min_perc, max_perc):
        min_percentile = int(round(min_perc * 100))
        max_percentile = int(round(max_perc * 100))

        # override for max value
        if min_percentile == 100:
            min_percentile = 99
            max_percentile = 100

        # override when min and max are the same
        if min_percentile == max_percentile:
            min_percentile = min_percentile - 1

        return {
            "min": min_percentile,
            "max": max_percentile
        }

    @property
    def indexed_in(self):
        sources = []
        for record in self.records_sorted:
            if record.record_type == "crossref_doi":
                sources.append("crossref")
            if record.record_type == "datacite_doi":
                sources.append("datacite")
            if record.record_type == "pubmed_record":
                sources.append("pubmed")
            if record.record_type == "pmh_record" and record.pmh_id:
                pmh_id_lower = record.pmh_id.lower()
                if "oai:arxiv.org" in pmh_id_lower:
                    sources.append("arxiv")
                if "oai:doaj.org/" in pmh_id_lower:
                    sources.append("doaj")
        return sorted(list(set(sources)))

    def store(self):
        if not self.full_updated_date:
            return []

        index_suffix = elastic_index_suffix(self.year)

        if self.merge_into_id is not None:
            bulk_actions = self.handle_merge(index_suffix)
        else:
            bulk_actions = self.handle_indexing(index_suffix)
        return bulk_actions

    def handle_merge(self, index_suffix):
        bulk_actions = []
        entity_hash = entity_md5(self.merge_into_id)

        if entity_hash != self.json_entity_hash:
            logger.info(f"merging {self.openalex_id} into {self.merge_into_id}")
            index_record = {
                "_op_type": "index",
                "_index": "merge-works",
                "_id": self.openalex_id,
                "_source": {
                    "id": self.openalex_id,
                    "merge_into_id": as_work_openalex_id(self.merge_into_id),
                }
            }
            delete_record = {
                "_op_type": "delete",
                "_index": f"{WORKS_INDEX_PREFIX}-{index_suffix}",
                "_id": self.openalex_id,
            }
            bulk_actions.append(index_record)
            bulk_actions.append(delete_record)
        else:
            logger.info(f"already merged into {self.merge_into_id}, not saving again")
        self.json_entity_hash = entity_hash
        return bulk_actions

    def handle_indexing(self, index_suffix):
        bulk_actions = []

        my_dict = self.to_dict("full")
        my_dict['updated'] = my_dict.get('updated_date')
        my_dict['@timestamp'] = datetime.datetime.utcnow().isoformat()
        my_dict['@version'] = 1
        my_dict['authors_count'] = len(self.affiliations_list)
        my_dict['concepts_count'] = len(self.concepts_sorted)
        my_dict['topics_count'] = len(self.topics_sorted)
        my_dict['abstract_inverted_index'] = self.abstract.indexed_abstract if self.abstract else None

        if self.abstract and self.abstract.abstract:
            my_dict['abstract'] = self.abstract.abstract

        if self.record_fulltext:
            my_dict['fulltext'] = self.record_fulltext
            my_dict['has_fulltext'] = True
            my_dict['fulltext_origin'] = 'pdf'
        elif self.fulltext and self.fulltext.fulltext:
            my_dict['fulltext'] = self.fulltext.fulltext
            my_dict['has_fulltext'] = True
            my_dict['fulltext_origin'] = 'ngrams'
        else:
            my_dict['has_fulltext'] = False

        if len(my_dict.get('authorships', [])) > 100:
            my_dict['authorships_full'] = my_dict.get('authorships', [])
            my_dict['authorships'] = my_dict.get('authorships', [])[0:100]
            my_dict['authorships_truncated'] = True

        if self.is_closed_springer:
            my_dict.pop('abstract', None)
            my_dict["abstract_inverted_index"] = None

        entity_hash = entity_md5(my_dict)

        if work_has_null_author_ids(my_dict):
            logger.info('not saving work because some authors have null IDs')
            # log this to db
            sq = """
            INSERT INTO logs.store_fail_null_authors
            (work_id, failed_at)
            VALUES(:work_id, :now);
            """
            db.session.execute(text(sq), {"work_id": self.paper_id, "now": datetime.datetime.utcnow().isoformat()})
        elif entity_hash != self.json_entity_hash:
            logger.info(f"dictionary for {self.openalex_id} new or changed, so save again")
            index_record = {
                "_op_type": "index",
                "_index": f"{WORKS_INDEX_PREFIX}-{index_suffix}",
                "_id": self.openalex_id,
                "_source": my_dict
            }
            bulk_actions.append(index_record)

            # check if year has changed, delete old records to prevent duplicate
            if self.previous_years:
                for old_year in self.previous_years:
                    try:
                        old_year = int(old_year)
                    except ValueError:
                        logger.warning(f"year for {self.openalex_id} changed but not a valid year {old_year}")
                        continue
                    logger.info(f"year for {self.openalex_id} changed from {old_year} to {self.year}")
                    old_index_suffix = elastic_index_suffix(old_year)
                    if old_index_suffix != index_suffix:
                        logger.info(f"delete {self.openalex_id} from old index {old_index_suffix}")
                        delete_record = {
                            "_op_type": "delete",
                            "_index": f"{WORKS_INDEX_PREFIX}-{old_index_suffix}",
                            "_id": self.openalex_id,
                        }
                        bulk_actions.append(delete_record)
                    else:
                        logger.info(f"year for {self.openalex_id} changed but still in same index {index_suffix}")
                self.previous_years = None
        else:
            logger.info(f"dictionary not changed, don't save again {self.openalex_id}")

        self.json_entity_hash = entity_hash
        self.updated_date = my_dict.get('updated_date')
        return bulk_actions

    @cached_property
    def display_counts_by_year(self):
        response_dict = {}
        for count_row in self.counts_by_year:
            response_dict[count_row.year] = {"year": count_row.year, "cited_by_count": 0}
        for count_row in self.counts_by_year:
            if count_row.type == "citation_count":
                response_dict[count_row.year]["cited_by_count"] = count_row.n

        my_dicts = [counts for counts in response_dict.values() if counts["year"] and counts["year"] >= 2012]
        response = sorted(my_dicts, key=lambda x: x["year"], reverse=True)
        return response

    def host_source_matching_location(self):
        journal_id_match = None
        publisher_host_type_match = None
        legacy_mag_match = None

        for loc in self.locations_sorted:
            if loc.journal and self.journal_id == loc.journal.journal_id and not journal_id_match:
                journal_id_match = loc
            elif loc.host_type == "publisher" and not publisher_host_type_match:
                publisher_host_type_match = loc
            elif not self.records_merged and not legacy_mag_match:
                legacy_mag_match = loc

        return journal_id_match or publisher_host_type_match or legacy_mag_match

    @cached_property
    def dict_locations(self):
        from models.location import is_accepted, is_published
        locations = []
        seen_urls = set()

        # make sure to add doi location first
        for r in self.records_sorted:
            if r.record_type == 'crossref_doi' or r.record_type == 'datacite_doi':
                doi_url = f'https://doi.org/{r.doi}'

                doi_location = {
                    'source': r.journal and r.journal.to_dict(return_level='minimum'),
                    'pdf_url': r.work_pdf_url,
                    'landing_page_url': r.record_webpage_url,
                    'is_oa': r.is_oa,
                    'version': r.open_version,
                    'license': r.open_license,
                    'doi': doi_url,
                }

                # bare minimum: include the DOI as the landing page URL
                if not doi_location['landing_page_url']:
                    doi_location['landing_page_url'] = doi_url

                if not doi_location['version']:
                    doi_location['version'] = self.guess_version()
                
                doi_location['is_accepted'] = is_accepted(doi_location['version'])
                doi_location['is_published'] = is_published(doi_location['version'])

                locations.append(doi_location)

                if doi_location['pdf_url']:
                    seen_urls.add(doi_location['pdf_url'])

                if doi_location['landing_page_url']:
                    seen_urls.add(doi_location['landing_page_url'])

                break

        # then primary-source repositories
        for r in self.records_sorted:
            if not (r.work_pdf_url or r.record_webpage_url):
                continue

            if r.work_pdf_url in seen_urls or r.record_webpage_url in seen_urls:
                continue

            if r.record_type == 'pmh_record':
                if r.doi:
                    doi_url = f'https://doi.org/{r.doi}'
                else:
                    doi_url = None

                pmh_location = {
                    'source': r.journal and r.journal.to_dict(return_level='minimum'),
                    'pdf_url': r.work_pdf_url,
                    'landing_page_url': r.record_webpage_url,
                    'is_oa': r.is_oa,
                    'version': r.open_version,
                    'license': r.open_license,
                    'doi': doi_url,
                }

                # special case for arXiv
                if r.repository_id == "ca8f8d56758a80a4f86" and r.arxiv_id:
                    pmh_location['doi'] = f"https://doi.org/10.48550/{r.arxiv_id.replace(':', '.')}"

                if not pmh_location['version']:
                    pmh_location['version'] = self.guess_version()

                pmh_location['is_accepted'] = is_accepted(pmh_location['version'])
                pmh_location['is_published'] = is_published(pmh_location['version'])

                if pmh_location['pdf_url']:
                    seen_urls.add(pmh_location['pdf_url'])

                if pmh_location['landing_page_url']:
                    seen_urls.add(pmh_location['landing_page_url'])

                locations.append(pmh_location)

        # then OA unpaywall locations
        for other_location in self.locations_sorted:
            if (
                other_location.is_from_unpaywall()  # take anything from unpaywall
                #  if we didn't already get it from the Recordthresher records
                and other_location.url_for_landing_page not in seen_urls
                and other_location.url_for_pdf not in seen_urls
            ):
                logger.info(f'Appending Unpaywall location to work - {self.paper_id}')
                other_location_dict = other_location.to_locations_dict()

                if other_location_dict['pdf_url']:
                    seen_urls.add(other_location_dict['pdf_url'])

                if other_location_dict['landing_page_url']:
                    seen_urls.add(other_location_dict['landing_page_url'])

                locations.append(other_location_dict)

        # then mag locations
        for other_location in self.locations_sorted:
            if not locations and not other_location.is_from_unpaywall() and other_location.has_any_url():
                # take anything with a URL at this point
                other_location_dict = other_location.to_locations_dict()

                if other_location_dict['pdf_url']:
                    seen_urls.add(other_location_dict['pdf_url'])

                if other_location_dict['landing_page_url']:
                    seen_urls.add(other_location_dict['landing_page_url'])

                if not self.records_merged and self.journal:
                    # mag location, assume it came from the work's mag journal
                    other_location_dict['source'] = self.journal.to_dict(return_level='minimum')

                locations.append(other_location_dict)

        # pubmed
        for r in self.records_sorted:
            if r.record_type == 'pubmed_record' and r.pmid:
                if r.doi:
                    doi_url = f'https://doi.org/{r.doi}'
                else:
                    doi_url = None
                pubmed_location = {
                    'source': pubmed_json(),
                    'pdf_url': None,
                    'landing_page_url': f'https://pubmed.ncbi.nlm.nih.gov/{r.pmid}',
                    'is_oa': False,
                    'version': self.guess_version(),
                    'license': None,
                    'doi': doi_url,
                }
                pubmed_location['is_accepted'] = is_accepted(pubmed_location['version'])
                pubmed_location['is_published'] = is_published(pubmed_location['version'])

                locations.append(pubmed_location)
                break

        # then datacite metadata
        for r in self.records_sorted:
            if r.record_type == 'datacite_doi':
                datacite_source = models.Source.query.get(4393179698)
                datacite_location = {
                    'source': datacite_source.to_dict(return_level='minimum'),
                    'pdf_url': None,
                    'landing_page_url': f'https://api.datacite.org/dois/{r.doi}',
                    'is_oa': None,
                    'version': None,
                    'license': None,
                    'doi': r.doi,
                }

                locations.append(datacite_location)
                break

        # last chance, make a location if there is a DOI but no locations yet
        if self.doi_url and not locations:
            lastchance_location = {
                'source': None,
                'pdf_url': None,
                'landing_page_url': self.doi_url,
                'is_oa': False,
                'version': self.guess_version(),
                'license': None,
                'doi': self.doi_url,
            }
            lastchance_location['is_accepted'] = is_accepted(lastchance_location['version'])
            lastchance_location['is_published'] = is_published(lastchance_location['version'])

            locations.append(lastchance_location)

        # Sources created manually using only the original_venue property from works that otherwise don't have Sources
        # ! Note that this does name matching of sources, which is problematic. I'm too nervous to change it now because I don't know how many works it will affect, so I'm just hard-coding manual exceptions.
        source_match_exceptions = ['Zoonoses']
        if locations and locations[0]['source'] is None and self.safety_journals:
            source_match = self.safety_journals[0].to_dict(return_level='minimum')
            if source_match and source_match['display_name'] not in source_match_exceptions:
                locations[0]['source'] = source_match

        locations = override_location_sources(locations)
        for loc in locations:
            loc['is_oa'] = loc['is_oa'] or False

        if locations and (not locations[0]["version"]):
            locations[0]["version"] = self.guess_version()
            locations[0]['is_accepted'] = is_accepted(locations[0]['version'])
            locations[0]['is_published'] = is_published(locations[0]['version'])

        return locations

    @cached_property
    def oa_locations(self):
        return [loc for loc in self.dict_locations if loc.get("is_oa")]

    def locations_count(self):
        return len(self.dict_locations)

    @cached_property
    def oa_url(self):
        if self.oa_locations:
            return self.oa_locations[0].get('pdf_url') or self.oa_locations[0].get('landing_page_url')
        return None

    @staticmethod
    def get_countries_from_raw_affiliation(raw_affiliation):
        countries_in_string = []
        # Hopeful first match
        _ = [countries_in_string.append(x) for x, y in COUNTRIES.items() if
             max([1 if re.search(fr"\b{i}\b", raw_affiliation) else 0 for i in y]) > 0]
        if not countries_in_string:
            # Replace '.' to see if match can be found
            _ = [countries_in_string.append(x) for x, y in COUNTRIES.items() if
                 max([1 if re.search(fr"\b{i}\b", raw_affiliation.replace(".", "")) else 0 for i in y]) > 0]
            if not countries_in_string:
                # Remove word boundary requirement
                _ = [countries_in_string.append(x) for x, y in COUNTRIES.items() if
                     max([1 if re.search(fr"{i}", raw_affiliation) else 0 for i in y]) > 0]
                if not countries_in_string:
                    # Lowercase all text to catch weird capitalizations
                    _ = [countries_in_string.append(x) for x, y in COUNTRIES.items() if
                         max([1 if re.search(fr"\b{i.lower()}\b", raw_affiliation.lower()) else 0 for i in y]) > 0]

        final_countries = sorted(list(set(countries_in_string)))

        # If we match to Georgia countries GE or GS, remove US match that came from short state string
        if ("GE" in final_countries or "GS" in final_countries) and "US" in final_countries:
            final_countries.remove("US")

        return final_countries

    @cached_property
    def countries_distinct_count(self):
        countries = []
        for affil in self.affiliations_list:
            if affil.get("countries"):
                countries += affil.get("countries")
        return len(set(countries))

    @cached_property
    def record_fulltext(self):
        # currently this fulltext comes from parsed PDFs
        for record in self.records_merged:
            if record.record_type == "crossref_doi" and record.fulltext and record.fulltext.truncated_fulltext:
                clean_fulltext = re.sub(r'<[^>]+>', '', record.fulltext.truncated_fulltext)
                clean_fulltext = ' '.join(clean_fulltext.split())
                clean_fulltext = '\n'.join([line.strip() for line in clean_fulltext.splitlines() if line.strip()])
                return clean_fulltext

    def to_dict(self, return_level="full"):
        truncated_title = truncate_on_word_break(self.work_title, 500)
        
        corresponding_author_ids: List[str] = []
        corresponding_institution_ids: List[str] = []
        for affil in self.affiliations_list:
            if affil.get('is_corresponding', False) is True:
                author = affil.get('author', None)
                if author and author.get('id'):
                    corresponding_author_ids.append(author.get("id"))
                institutions = affil.get('institutions', []) or []
                for institution in institutions:
                    if institution.get("id"):
                        corresponding_institution_ids.append(institution.get("id"))

        is_oa = self.is_oa
        oa_status = self.oa_status or "closed"
        # if is_oa and oa_status are inconsistent, we need to fix
        if is_oa is False and oa_status != 'closed':
            # on inspection, a lot of these seem to be open, so let's mark them OA
            is_oa = True
        elif is_oa is True and oa_status == 'closed':
            for loc in self.oa_locations:
                this_loc_oa_status = oa_status_from_location(loc)
                oa_status = self.update_oa_status_if_better(this_loc_oa_status)

        response = {
            "id": self.openalex_id,
            "doi": self.doi_url,
            "doi_registration_agency": self.doi_ra.agency if self.doi_ra else None,
            "display_name": truncated_title,
            "title": truncated_title,
            "publication_year": self.year,
            "publication_date": self.publication_date,
            "language": self.language,
            "language_id": f"https://openalex.org/languages/{self.language}",
            "ids": {
                "openalex": self.openalex_id,
                "doi": self.doi_url,
                "pmid": None, #filled in below (extra_ids)
                "mag": self.paper_id if self.paper_id < MAX_MAG_ID else None,
                "arxiv_id": self.arxiv_id,
            },
            "primary_location": self.dict_locations[0] if self.dict_locations else None,
            "best_oa_location": self.oa_locations[0] if self.oa_locations else None,
            "type": self.display_genre,
            "type_crossref": self.type_crossref,
            "type_id": f"https://openalex.org/work-types/{self.display_genre}",
            "indexed_in": self.indexed_in,
            "open_access": {
                "is_oa": is_oa,
                "oa_status": oa_status,
                "oa_url": self.oa_url,
                "any_repository_has_fulltext": any(
                    [
                        loc.get("source") is None or (loc.get("source") or {}).get("type") == "repository"
                        for loc in self.oa_locations
                    ]
                )
            },
            "authorships": self.affiliations_list,
            "countries_distinct_count": self.countries_distinct_count,
            "institutions_distinct_count": len(self.institutions_distinct),
            "corresponding_author_ids": corresponding_author_ids,
            "corresponding_institution_ids": corresponding_institution_ids,
        }

        if self.extra_ids:
            for extra_id in self.extra_ids:
                response["ids"][extra_id.id_type] = extra_id.url

        updated_date = self.updated_date # backup in case full_updated_date is null waiting for update
        if self.full_updated_date:
            if isinstance(self.full_updated_date, datetime.datetime):
                updated_date = self.full_updated_date.isoformat()
            else:
                updated_date = self.full_updated_date
        if return_level in ("full", "store"):
            grant_dicts = []
            for f in self.funders:
                fd = f.funder.to_dict(return_level="minimum")
                if f.award:
                    awards = set(f.award)
                    for award in awards:
                        grant_dicts.append({
                            "funder": fd.get("id"),
                            "funder_display_name": fd.get("display_name"),
                            "award_id": award
                        })
                else:
                    grant_dicts.append({
                        "funder": fd.get("id"),
                        "funder_display_name": fd.get("display_name"),
                        "award_id": None
                    })

            response.update({
                # "doc_type": self.doc_type,
                "cited_by_count": self.counts.citation_count if self.counts else 0,
                "summary_stats": {
                    "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                    "2yr_cited_by_count": int(self.citation_count_2year.count or 0) if self.citation_count_2year else 0
                },
                "biblio": {
                    "volume": self.volume,
                    "issue": self.issue,
                    "first_page": self.first_page,
                    "last_page": self.last_page
                },
                "is_retracted": self.is_retracted,
                "is_paratext": self.display_genre == 'paratext' or self.looks_like_paratext,
                "concepts": [concept.to_dict("minimum") for concept in self.concepts_sorted],
                "topics": [topic.to_dict("minimum") for topic in self.topics_sorted][:3] if self.topics_sorted else [],
                "primary_topic": [topic.to_dict("minimum") for topic in self.topics_sorted[:1]][0] if self.topics_sorted else None,
                "mesh": [mesh.to_dict("minimum") for mesh in self.mesh_sorted],
                "locations_count": self.locations_count(),
                "locations": self.dict_locations,
                "referenced_works": self.references_list,
                "referenced_works_count": len(self.references_list),
                "sustainable_development_goals": self.sustainable_development_goals,
                "keywords": self.all_work_keywords,
                "grants": grant_dicts,
                "apc_list": self.apc_list,
                "apc_paid": self.apc_paid,
                "cited_by_percentile_year": self.cited_by_percentile_year,
                "related_works": [as_work_openalex_id(related.recommended_paper_id) for related in self.related_works]
            })
            
            if return_level == "full":
                response["abstract_inverted_index"] = self.abstract.to_dict("minimum") if self.abstract else None
                if self.is_closed_springer:
                    response["abstract_inverted_index"] = None

            response["counts_by_year"] = self.display_counts_by_year
            response["cited_by_api_url"] = self.cited_by_api_url
            response["updated_date"] = datetime.datetime.utcnow().isoformat()
            response["created_date"] = self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]

        # only include non-null IDs
        for id_type in list(response["ids"].keys()):
            if response["ids"][id_type] == None:
                del response["ids"][id_type]

        return response

    def __repr__(self):
        return "<Work ( {} ) {} {} '{}...'>".format(self.openalex_api_url, self.id, self.doi, self.original_title[0:20] if self.original_title else None)


def on_year_change(mapper, connection, target):
    hist = get_history(target, 'year')
    if hist.has_changes():
        old_year = hist.deleted[0] if hist.deleted else None
        new_year = hist.added[0] if hist.added else None

        if old_year != new_year and old_year is not None:
            if target.previous_years is None:
                logger.info(f"year for {target.paper_id} changed from {old_year} to {new_year} (setting previous_years)")
                target.previous_years = [old_year]
            else:
                if old_year not in target.previous_years:
                    logger.info(f"year for {target.paper_id} changed from {old_year} to {new_year} (adding to previous_years)")
                    target.previous_years.append(old_year)


event.listen(Work, 'before_update', on_year_change)


class WorkFulltext(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_fulltext"

    work_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    doi = db.Column(db.Text)
    fulltext = db.Column(db.Text)


Work.fulltext = db.relationship(WorkFulltext, lazy='selectin', viewonly=True, uselist=False)
