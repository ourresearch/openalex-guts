from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
import datetime
from collections import defaultdict
import requests
import os
import json

from app import db
from app import MAX_MAG_ID
from app import get_apiurl_from_openalex_url
from util import f_generate_inverted_index
from util import jsonify_fast_no_sort_raw
from util import normalize_simple
from util import clean_doi
from util import normalize_orcid
from util import normalize_title_like_sql
from util import clean_html
from util import dictionary_nested_diff
from util import jsonify_fast_no_sort_raw
from app import get_db_cursor
import models

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
            "inverted_abstract": has_abstract
        }]

    class ConceptLookupResponse:
        pass

    api_key = os.getenv("SAGEMAKER_API_KEY")
    headers = {"X-API-Key": api_key}
    # api_url = "https://4rwjth9jek.execute-api.us-east-1.amazonaws.com/api/" #for version without abstracts
    api_url = "https://cm1yuwajpa.execute-api.us-east-1.amazonaws.com/api/" #for vesion with abstracts
    r = requests.post(api_url, json=json.dumps(data_list), headers=headers)
    if r.status_code != 200:
        print(f"error in call_sagemaker_bulk_lookup_new_work_concepts: status code {r} reason {r.reason}")
        return []

    api_json = r.json()
    for row, api_dict in zip(rows, api_json):
        if api_dict["tags"] != []:
            for i, concept_name in enumerate(api_dict["tags"]):
                insert_dicts += [{"WorkConceptFull": {"paper_id": row["paper_id"],
                                                       "field_of_study": api_dict["tag_ids"][i],
                                                       "score": api_dict["scores"][i],
                                                       "algorithm_version": 2,
                                                       "uses_newest_algorithm": True,
                                                       "updated_date": datetime.datetime.utcnow().isoformat()}}]
    response = ConceptLookupResponse()
    response.insert_dicts = insert_dicts
    response.delete_dict = {"WorkConceptFull": [row["paper_id"] for row in rows]}
    return [response]


class Work(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work"

    # id = db.Column(db.BigInteger)
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


    def update_institutions(self):
        institution_names = [affil.original_affiliation for affil in self.affiliations if affil.original_affiliation]
        institution_ids = models.Institution.get_institution_ids_from_strings(institution_names)

        if institution_ids:
            lookup = dict(zip(institution_names, institution_ids))
            # print(lookup)
            for affil in self.affiliations:
                if affil.original_affiliation:
                    new_affiliation = lookup.get(affil.original_affiliation, None)
                    if new_affiliation != affil.affiliation_id:
                        affil.affiliation_id = new_affiliation
                        affil.updated_date = datetime.datetime.utcnow().isoformat()
                        self.full_updated_date = datetime.datetime.utcnow().isoformat()


    def add_work_concepts(self):
        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        # keep all the version 1s for now but remove all the version 2s
        self.concepts_full = [concept for concept in self.concepts_full if concept.algorithm_version == 1]

        abstract_dict = None
        # for when abstract was added and not included in table yet
        if hasattr(self, "abstract_indexed_abstract"):
            abstract_dict = self.abstract_indexed_abstract
        elif self.abstract:
            abstract_dict = self.abstract.indexed_abstract

        api_key = os.getenv("SAGEMAKER_API_KEY")
        has_abstract = True if abstract_dict else False
        data = {
            "title": self.work_title.lower() if self.work_title else None,
            "doc_type": self.doc_type,
            "journal": self.journal.display_name.lower() if self.journal else None,
            "abstract": abstract_dict,
            "inverted_abstract": has_abstract
        }
        headers = {"X-API-Key": api_key}
        api_url = "https://t9a5o7qfy2.execute-api.us-east-1.amazonaws.com/api/" #for vesion with abstracts

        number_tries = 0
        keep_calling = True
        while keep_calling:
            r = requests.post(api_url, json=json.dumps([data]), headers=headers)

            if r.status_code == 200:
                try:
                    response_json = r.json()
                    concept_names = response_json[0]["tags"]
                    keep_calling = False
                except Exception as e:
                    print(f"error {e} in add_work_concepts with {self.id}, response {r}, called with {api_url} data: {data} headers: {headers}")
                    concept_names = None
                    keep_calling = False

            elif r.status_code == 500:
                print(f"Error on try #{number_tries}, now trying again: Error back from API endpoint: {r} {r.status_code}")
                number_tries += 1

            else:
                print(f"Error, not retrying: Error back from API endpoint: {r} {r.status_code} {r.text} for input {data}")
                concept_names = None
                keep_calling = False


        self.concepts_for_related_works = []
        fields_of_study = []
        if concept_names:
            for i, concept_name in enumerate(concept_names):
                score = response_json[0]["scores"][i]
                field_of_study = response_json[0]["tag_ids"][i]
                if field_of_study:
                    fields_of_study += [field_of_study]
                new_work_concept = models.WorkConceptFull(field_of_study=field_of_study,
                                                       score=score,
                                                       algorithm_version=2,
                                                       uses_newest_algorithm=True,
                                                       updated_date=datetime.datetime.utcnow().isoformat())
                self.concepts_full += [new_work_concept]
                if score > 0.3:
                    self.concepts_for_related_works.append(field_of_study)
        else:
            pass


    def add_everything(self):
        self.delete_dict = defaultdict(list)
        self.insert_dicts = []

        if not self.records_sorted:
            # not associated with a record, so leave it for now
            # merged-away works have their records' work ids updated,
            # so also do nothing for merged-away works
            print(f"No associated records for {self.paper_id}, so skipping")
            return

        self.set_fields_from_all_records()
        self.add_abstract()  # must be before work_concepts
        self.add_work_concepts()

        self.add_related_works()  # must be after work_concepts

        self.add_mesh()
        self.add_ids()
        self.add_locations()
        self.add_references() # must be before affiliations

        # for now, only add/update affiliations if they aren't there
        if not self.affiliations:
            print("adding affiliations because work didn't have any yet")
            self.add_affiliations()
        else:
            print("not adding affiliations because work already has some set, but updating institutions")
            self.update_institutions()


    def add_related_works(self):
        if self.concepts:
            self.concepts_for_related_works = [concept.field_of_study for concept in self.concepts]
        else:
            if not hasattr(self, "concepts_for_related_works"):
                return
            if not self.concepts_for_related_works:
                return

        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        # if not hasattr(self, "insert_dicts"):
        #     self.insert_dicts = []

        matching_papers_sql = """
            with matches as (
            select
                            paper_id as related_paper_id,
                            avg(score) as average_related_score,
                            count(distinct field_of_study) as n
                        from mid.work_concept_for_api_view
                        where field_of_study in %s
                        group by paper_id
                        limit 100000
            )
            select matches.*, n*average_related_score
            from matches
            -- where n >= 3
            order by n desc, average_related_score desc
            limit 10
        """

        with get_db_cursor() as cur:
            # print(cur.mogrify(matching_papers_sql, (tuple(self.concepts_for_related_works), )))
            cur.execute(matching_papers_sql, (tuple(self.concepts_for_related_works), ))
            rows = cur.fetchall()
            # print(rows)

            # for row in rows:
            #     score = row["average_related_score"] * row["average_related_score"]
            #
            #     self.insert_dicts += [{"WorkRelatedWork": {"paper_id": self.paper_id,
            #                                                "recommended_paper_id": row["related_paper_id"],
            #                                                "score": score,
            #                                                "updated": datetime.datetime.utcnow().isoformat()
            #                                                }}]
            self.related_works = [models.WorkRelatedWork(paper_id = self.paper_id,
                                                       recommended_paper_id = row["related_paper_id"],
                                                       score = row["average_related_score"] * row["average_related_score"],
                                                       updated = datetime.datetime.utcnow().isoformat())
                                  for row in rows]


    def add_abstract(self):
        self.abstract_indexed_abstract = None
        self.full_updated_date = datetime.datetime.utcnow().isoformat()
        for record in self.records:
            if record.abstract:
                indexed_abstract = f_generate_inverted_index(record.abstract)
                if len(indexed_abstract) >= 60000:
                    # truncate the abstract if too long
                    indexed_abstract = f_generate_inverted_index(record.abstract[0:30000])
                insert_dict = {"paper_id": self.paper_id, "indexed_abstract": indexed_abstract}
                # self.insert_dicts += [{"Abstract": insert_dict}]
                self.abstract = models.Abstract(**insert_dict)
                self.abstract_indexed_abstract = indexed_abstract
                return

    def add_mesh(self):
        self.mesh = []
        self.full_updated_date = datetime.datetime.utcnow().isoformat()
        for record in self.records:
            if record.mesh:
                mesh_dict_list = json.loads(record.mesh)
                mesh_objects = [models.Mesh(**mesh_dict) for mesh_dict in mesh_dict_list]
                for mesh_object in mesh_objects:
                    if mesh_object.qualifier_ui == None:
                        mesh_object.qualifier_ui = ""  # can't be null for primary key
                self.mesh = mesh_objects
                # for mesh_dict in mesh_dict_list:
                #     mesh_dict["paper_id"] = self.paper_id
                #     self.insert_dicts += [{"Mesh": mesh_dict}]
                return

    def add_ids(self):
        for record in self.records:
            # just pmid for now
            if record.pmid:
                self.full_updated_date = datetime.datetime.utcnow().isoformat()
                # self.insert_dicts += [{"WorkExtraIds": {"paper_id": self.paper_id, "attribute_type": 2, "attribute_value": record.pmid}}]
                if record.pmid not in [extra.attribute_value for extra in self.extra_ids if extra.attribute_type==2]:
                    self.extra_ids += [models.WorkExtraIds(paper_id=self.paper_id, attribute_type=2, attribute_value=record.pmid)]
                return

    def add_locations(self):
        from models.location import get_repository_institution_from_source_url
        self.locations = []
        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        records_with_unpaywall = [record for record in self.records_sorted if hasattr(record, "unpaywall") and record.unpaywall]
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
                "license": unpaywall_oa_location["license"]}
            if get_repository_institution_from_source_url(unpaywall_oa_location["url"]):
                insert_dict["repository_institution"] = get_repository_institution_from_source_url(unpaywall_oa_location["url"])
            # self.insert_dicts += [{"Location": insert_dict}]
            self.locations += [models.Location(**insert_dict)]

    def add_references(self):
        from models import WorkExtraIds
        citation_dois = []
        citation_pmids = []
        citation_paper_ids = []

        self.citation_paper_ids = []

        reference_source_num = 0
        for record in self.records:
            if record.citations:
                self.references = []
                self.references_unmatched = []
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
                            self.references_unmatched += [models.CitationUnmatched(reference_sequence_number=reference_source_num, raw_json=json.dumps(citation_dict))]

                except Exception as e:
                    print(f"error json parsing citations, but continuing on other papers {self.paper_id} {e}")

        if citation_dois:
            works = db.session.query(Work).options(orm.Load(Work).raiseload('*')).filter(Work.doi_lower.in_(citation_dois)).all()
            for my_work in works:
                my_work.full_updated_date = datetime.datetime.utcnow().isoformat()
            citation_paper_ids += [work.paper_id for work in works if work.paper_id]
        if citation_pmids:
            work_ids = db.session.query(WorkExtraIds).options(orm.Load(WorkExtraIds).selectinload(models.WorkExtraIds.work).raiseload('*')).filter(WorkExtraIds.attribute_type==2, WorkExtraIds.attribute_value.in_(citation_pmids)).all()
            for my_work_id in work_ids:
                if my_work_id.work:
                    my_work_id.work.full_updated_date = datetime.datetime.utcnow().isoformat()
            citation_paper_ids += [work_id.paper_id for work_id in work_ids if work_id and work_id.paper_id]

        citation_paper_ids = list(set(citation_paper_ids))
        self.references = [models.Citation(paper_reference_id=reference_id) for reference_id in citation_paper_ids]
        if citation_paper_ids:
            self.citation_paper_ids = citation_paper_ids  # used for matching authors right now


    def add_affiliations(self):
        self.affiliations = []
        self.full_updated_date = datetime.datetime.utcnow().isoformat()

        records_with_affiliations = [record for record in self.records_sorted if record.authors]
        if not records_with_affiliations:
            print("no affiliation data found in any of the records")
            return

        record = records_with_affiliations[0]
        author_dict_list = json.loads(record.authors)

        for author_sequence_order, author_dict in enumerate(author_dict_list):
            my_author = None
            original_name = author_dict["raw"]
            if author_dict["family"]:
                original_name = "{} {}".format(author_dict["given"], author_dict["family"])
            if not author_dict["affiliation"]:
                author_dict["affiliation"] = [defaultdict(str)]

            raw_author_string = original_name if original_name else None
            original_orcid = normalize_orcid(author_dict["orcid"]) if author_dict["orcid"] else None
            if raw_author_string:
                my_author = models.Author.try_to_match(raw_author_string, original_orcid, self.citation_paper_ids)

            author_match_name = models.Author.matching_author_string(raw_author_string)
            # print(f"author_match_name: {author_match_name}")

            if raw_author_string and not my_author:
                my_author = models.Author(display_name=raw_author_string,
                    match_name=author_match_name,
                    created_date=datetime.datetime.utcnow().isoformat(),
                    full_updated_date=datetime.datetime.utcnow().isoformat(),
                    updated_date=datetime.datetime.utcnow().isoformat())
                if original_orcid:
                    my_author_orcid = models.AuthorOrcid(orcid=original_orcid)
                    my_author.orcids = [my_author_orcid]

            if my_author:
                my_author.full_updated_date = datetime.datetime.utcnow().isoformat()  # citations and fields

            for affiliation_sequence_order, affiliation_dict in enumerate(author_dict["affiliation"]):
                raw_affiliation_string = affiliation_dict["name"] if affiliation_dict["name"] else None
                raw_affiliation_string = clean_html(raw_affiliation_string)
                my_institution = None
                if raw_affiliation_string:
                    institution_id_matches = models.Institution.get_institution_ids_from_strings([raw_affiliation_string])
                    if institution_id_matches and institution_id_matches[0]:
                        my_institution = models.Institution.query.options(orm.Load(models.Institution).raiseload('*')).get(institution_id_matches[0])

                # comment this out for now because it is too slow for some reason, but later comment it back in
                # if my_institution:
                #     my_institution.full_updated_date = datetime.datetime.utcnow().isoformat()  # citations and fields

                if my_institution and my_author:
                    my_author.last_known_affiliation_id = my_institution.affiliation_id
                if raw_author_string or raw_affiliation_string:
                    my_affiliation = models.Affiliation(
                        author_sequence_number=author_sequence_order+1,
                        affiliation_sequence_number=affiliation_sequence_order+1,
                        original_author=raw_author_string,
                        original_affiliation=raw_affiliation_string[:2500] if raw_affiliation_string else None,
                        original_orcid=original_orcid,
                        match_author=author_match_name,
                        match_institution_name=models.Institution.matching_institution_name(raw_affiliation_string),
                        updated_date=datetime.datetime.utcnow().isoformat())
                    my_affiliation.author = my_author
                    my_affiliation.institution = my_institution
                    self.affiliations += [my_affiliation]

        return

    def set_fields_from_record(self, record):
        from util import normalize_doi

        if not self.created_date:
            self.created_date = datetime.datetime.utcnow().isoformat()
        self.original_title = record.title[:60000] if record.title else None
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
        self.publication_date = record.published_date.isoformat()[0:10]
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
            self.oa_status = record.unpaywall.oa_status
            self.best_free_url = record.unpaywall.best_oa_location_url
            self.best_free_version = record.unpaywall.best_oa_location_version


    @cached_property
    def records_sorted(self):
        if not self.records:
            return []
        return sorted(self.records, key=lambda x: x.score, reverse=True)

    def set_fields_from_all_records(self):
        self.updated_date = datetime.datetime.utcnow().isoformat()
        self.full_updated_date = datetime.datetime.utcnow().isoformat()
        self.finished = datetime.datetime.utcnow().isoformat()

        # go through them with oldest first, and least reliable record type to most reliable, overwriting
        if not self.records_sorted:
            return
        records = self.records_sorted
        records.reverse()

        print(f"my records: {records}")

        for record in records:
            if record.record_type == "pmh_record":
                self.set_fields_from_record(record)
        for record in records:
            if record.record_type == "pubmed_record":
                self.set_fields_from_record(record)

        for record in records:
            if record.record_type == "crossref_doi":
                self.set_fields_from_record(record)

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
        return False

    @cached_property
    def affiliations_sorted(self):
        return sorted(self.affiliations, key=lambda x: x.author_sequence_number)

    @cached_property
    def mesh_sorted(self):
        # sort so major topics at the top and the rest is alphabetical
        return sorted(self.mesh, key=lambda x: (not x.is_major_topic, x.descriptor_name), reverse=False)

    @cached_property
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
            institution_list = [a["institution"] for a in affil_list]
            # institution_list = [a["institution"] for a in affil_list if a["institution"]["id"] != None]
            if institution_list == [{}]:
                institution_list = []
            response_dict = {"author_position": affil_list[0]["author_position"],
                             "author": affil_list[0]["author"],
                             "institutions": institution_list,
                             "raw_affiliation_string": affil_list[0]["raw_affiliation_string"]
                     }
            response.append(response_dict)
        return response

    @classmethod
    def author_match_names_from_record_json(cls, record_author_json):
        author_match_names = []
        if not record_author_json:
            return []
        author_dict_list = json.loads(record_author_json)

        for author_sequence_order, author_dict in enumerate(author_dict_list):
            my_author = None
            original_name = author_dict["raw"]
            if author_dict["family"]:
                original_name = "{} {}".format(author_dict["given"], author_dict["family"])

            raw_author_string = original_name if original_name else None
            author_match_name = models.Author.matching_author_string(raw_author_string)
            # print(f"author_match_name: {author_match_name}")
            if author_match_name:
                author_match_names += [author_match_name]
        print(f"author_match_names: {author_match_names}")
        return author_match_names


    def matches_authors_in_record(self, record_author_json):
        # returns True if either of them are missing authors, or if the authors match
        # returns False if both have authors but neither the first nor last author in the Work is in the author string

        if not record_author_json:
            print("no record_author_json, so not trying to match")
            return True
        if record_author_json == '[]':
            print("no record_author_json, so not trying to match")
            return True
        if not self.affiliations:
            print("no self.affiliations, so not trying to match")
            return True
        print(f"trying to match existing work {self.id} {self.doi_lower} with record authors")

        for original_name in [self.first_author_original_name, self.last_author_original_name]:
            print(f"original_name: {original_name}")
            if original_name:
                author_match_name = models.Author.matching_author_string(original_name)
                print(f"author_match_name: {author_match_name}")
                if author_match_name and (author_match_name in Work.author_match_names_from_record_json(record_author_json)):
                    print("author match!")
                    return True

        print("author no match")
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

    @cached_property
    def is_oa(self):
        if self.best_free_url:
            return True
        if self.oa_status and self.oa_status != "closed":
            return True
        return False

    @cached_property
    def display_genre(self):
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
            return lookup_mag_to_crossref_type[self.doc_type]
        return None

    @cached_property
    def references_list(self):
        import models

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


    def store(self):
        self.insert_dicts = []

        if not self.merge_into_id and not self.full_updated_date:
            print(f"Not storing anything for {self.openalex_id} yet because no full_updated_date or merge_into_id, so is a stub waiting for add_everything")
            return

        VERSION_STRING = "new: updated if changed"
        my_full_dict = self.to_dict("full")

        if self.stored and (self.stored.merge_into_id == self.merge_into_id):
            if self.stored.json_save:
                stored_full_dict = json.loads(self.stored.json_save_with_abstract)

                # freeze publication date/year until elastic partitioning is fixed
                if stored_pub_year := stored_full_dict.get('publication_year'):
                    my_full_dict['publication_year'] = stored_pub_year
                if stored_pub_date := stored_full_dict.get('publication_date'):
                    my_full_dict['publication_date'] = stored_pub_date

                # check merged here for everything but concept
                diff = dictionary_nested_diff(stored_full_dict, my_full_dict, ["updated_date"])
                if not diff:
                    print(f"dictionary not changed, don't save again {self.openalex_id}")
                    return
                print(f"dictionary for {self.openalex_id} new or changed, so save again. Diff: {diff}")

        now = datetime.datetime.utcnow().isoformat()
        self.full_updated_date = now
        my_full_dict["updated_date"] = now

        json_save = None
        json_save_with_abstract = None
        self.abstract_inverted_index = None
        if not self.merge_into_id:
            my_store_dict = self.to_dict("store")
            my_store_dict['publication_year'] = my_full_dict.get('publication_year')
            my_store_dict['publication_date'] = my_full_dict.get('publication_date')
            my_store_dict['updated_date'] = my_full_dict.get('updated_date')
            json_save = jsonify_fast_no_sort_raw(my_store_dict)
            self.abstract_inverted_index = self.abstract.indexed_abstract if self.abstract else None
            json_save_with_abstract = jsonify_fast_no_sort_raw(my_full_dict)


        #if json_save and len(json_save) > 65000:
        #    print("Error: json_save_escaped too long for paper_id {}, skipping".format(self.openalex_id))
        #    json_save = None
        #if json_save_with_abstract and len(json_save_with_abstract) > 65000:
        #    print("Error: json_save_escaped too long for paper_id {}, skipping".format(self.openalex_id))
        #    json_save_with_abstract = json_save

        self.insert_dicts = [{"JsonWorks": {"id": self.paper_id,
                                            "updated": now,
                                            "changed": now,
                                            "json_save": json_save,
                                            "version": VERSION_STRING,
                                            "abstract_inverted_index": self.abstract_inverted_index, # comment out if going fast
                                            "json_save_with_abstract": json_save_with_abstract, # comment out if going fast
                                            "merge_into_id": self.merge_into_id
                                            }}]

        # print("processing work! {}".format(self.id))



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

    @property
    def host_venue_details_dict(self):
        # should match the extra stuff put out in locations.to_dict()
        matching_location = None
        url = None
        for location in self.locations_sorted:
            if "doi.org/" in location.source_url and not matching_location:
                matching_location = location
            elif not matching_location:
                if location.host_type == "publisher":
                    matching_location = location
        if self.locations_sorted and (not matching_location):
            matching_location = self.locations_sorted[0]

        if self.best_url:
            url = self.best_url
        elif matching_location:
            url = matching_location.source_url

        type = None
        if matching_location and matching_location.host_type != None:
            type = matching_location.host_type
        elif self.journal and self.journal.issn_l:
            type = "publisher"
        elif url and "doi.org/" in url:
            type = "publisher"

        version = matching_location.version if matching_location else None
        license = matching_location.display_license if matching_location else None

        is_oa = None
        if matching_location and matching_location.is_oa != None:
            is_oa = matching_location.is_oa
        elif self.is_oa == False:
            is_oa = False
        elif self.oa_status == "gold":
            is_oa = True
            version = "publishedVersion"

        response = {
            "type": type,
            "url": url,
            "is_oa": is_oa,
            "version": version,
            "license": license
        }
        return response

    def to_dict(self, return_level="full"):
        from models import Venue

        response = {
            "id": self.openalex_id,
            "doi": self.doi_url,
            "display_name": self.work_title,
            "title": self.work_title,
            "publication_year": self.year,
            "publication_date": self.publication_date,
            "ids": {
                "openalex": self.openalex_id,
                "doi": self.doi_url,
                "pmid": None, #filled in below
                "mag": self.paper_id if self.paper_id < MAX_MAG_ID else None
            },
            "host_venue": self.journal.to_dict("minimum") if self.journal else Venue().to_dict_null_minimum(),
            "type": self.display_genre,
            "open_access": {
                "is_oa": self.is_oa,
                "oa_status": self.oa_status,
                "oa_url": self.best_free_url,
            },
            "authorships": self.affiliations_list,
        }
        response["host_venue"].update(self.host_venue_details_dict)
        response["host_venue"]["display_name"] = response["host_venue"]["display_name"] if response["host_venue"]["display_name"] else self.original_venue
        response["host_venue"]["publisher"] = response["host_venue"]["publisher"] if response["host_venue"]["publisher"] else self.publisher
        if self.extra_ids:
            for extra_id in self.extra_ids:
                response["ids"][extra_id.id_type] = extra_id.url

        updated_date = self.updated_date # backup in case full_updated_date is null waiting for update
        if self.full_updated_date:
            if isinstance(self.full_updated_date, datetime.datetime):
                updated_date = self.full_updated_date.isoformat()[0:10]
            else:
                updated_date = self.full_updated_date[0:10]
        if return_level in ("full", "store"):
            response.update({
                # "doc_type": self.doc_type,
                "cited_by_count": self.counts.citation_count if self.counts else 0,
                "biblio": {
                    "volume": self.volume,
                    "issue": self.issue,
                    "first_page": self.first_page,
                    "last_page": self.last_page
                },
                "is_retracted": self.is_retracted,
                "is_paratext": self.is_paratext,
                "concepts": [concept.to_dict("minimum") for concept in self.concepts_sorted],
                "mesh": [mesh.to_dict("minimum") for mesh in self.mesh_sorted],
                "alternate_host_venues": [location.to_dict("minimum") for location in self.locations_sorted if location.include_in_alternative],
                "referenced_works": self.references_list,
                "related_works": [as_work_openalex_id(related.recommended_paper_id) for related in self.related_works]
                })
            if return_level == "full":
                response["abstract_inverted_index"] = self.abstract.to_dict("minimum") if self.abstract else None
            response["counts_by_year"] = self.display_counts_by_year
            response["cited_by_api_url"] = self.cited_by_api_url
            response["updated_date"] = updated_date
            response["created_date"] = self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]

        # only include non-null IDs
        for id_type in list(response["ids"].keys()):
            if response["ids"][id_type] == None:
                del response["ids"][id_type]

        return response


    def __repr__(self):
        return "<Work ( {} ) {} {} '{}...'>".format(self.openalex_api_url, self.id, self.doi, self.original_title[0:20] if self.original_title else None)




