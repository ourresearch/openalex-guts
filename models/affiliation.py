from app import db

# alter table affiliation add column match_name varchar(65000)
# update affiliation set normalized_institution_name=f_matching_string(original_affiliation) where original_affiliation is not null

# alter table crossref_main_author add column normalized_author text
# update crossref_main_author set normalized_author=f_normalize_author(given || ' ' || family) where family is not null and given is not null

# alter table pubmed_main_author add column normalized_author text
# update pubmed_main_author set normalized_author=f_normalize_author(coalesce(given, '') || ' ' || coalesce(initials, '') || ' ' || family) where family is not null

# truncate mid.affiliation
# insert into mid.affiliation (select * from legacy.mag_main_paper_author_affiliations)
# update mid.affiliation set original_author=replace(original_author, '\t', '') where original_author ~ '\t';

class Affiliation(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "affiliation"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    author_sequence_number = db.Column(db.Numeric, primary_key=True)
    original_author = db.Column(db.Text)
    original_affiliation = db.Column(db.Text, primary_key=True)
    original_orcid = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)

    def update(self):

        # update mid.affiliation set affiliation_id=lookup.affiliation_id, updated_date=sysdate
        # from mid.affiliation affil
        # join mid.affiliation_institution_lookup_view lookup on affil.original_affiliation=lookup.original_affiliation
        # where affil.original_affiliation is not null and affil.affiliation_id is null and affil.paper_id > 4200000000


        # with update_table as (select affil.original_affiliation, inst.affiliation_id
        # from mid.affiliation affil
        # join mid.institution inst on affil.original_affiliation ilike '%' || inst.display_name || '%'
        # where affil.original_affiliation is not null and affil.affiliation_id is null and affil.paper_id > 4200000000
        # and inst.affiliation_id not in (select affiliation_id from mid.institutions_with_names_bad_for_ilookup)
        # group by affil.original_affiliation, inst.affiliation_id
        # )
        # update mid.affiliation set affiliation_id=t2.affiliation_id, updated_date=sysdate
        # -- select count(*)
        # from mid.affiliation t1
        # join update_table t2 on t1.original_affiliation=t2.original_affiliation
        # where t1.original_affiliation is not null and t1.affiliation_id is null and t1.paper_id > 4200000000

        # make sure to make the updated_date of works and be the max of its updated date and this updated date
        # update mid.work set updated_date=t2.updated_date
        # from mid.work t1
        # join mid.affiliation t2 on t1.paper_id=t2.paper_id
        # where t1.updated_date < t2.updated_date and t2.updated_date is not null

        # update mid.affiliation set match_author = f_matching_author_string(original_author) where original_author is not null and match_author is null;  -- running now 3830.2
        # update mid.affiliation set match_institution_name = f_matching_string(original_affiliation) where original_affiliation is not null and match_institution_name is null; -- took 1500

        # match orcids to what we've got
        # update mid.affiliation set author_id=t2.author_id, updated_date=sysdate
        # from mid.affiliation t1
        # join mid.author_orcid t2 on t1.original_orcid=t2.orcid
        # where t1.original_orcid is not null and t1.author_id is null

        # match author_id with the ones who cite themselves
        # with matching_authors as (
        # select orig_affil.paper_id, orig_affil.author_sequence_number,  affil.author_id, auth_cited.match_name, affil.match_author, citation.paper_reference_id
        # from mid.affiliation orig_affil
        # join mid.citation citation on orig_affil.paper_id=citation.paper_id
        # join mid.affiliation affil on affil.paper_id=citation.paper_reference_id
        # join mid.author auth_cited on affil.author_id=auth_cited.author_id
        # where orig_affil.author_id is null
        # and auth_cited.match_name = orig_affil.match_author
        # )
        # update mid.affiliation set author_id=t2.author_id, updated_date=sysdate
        # from mid.affiliation t1
        # join matching_authors t2 on (t1.paper_id=t2.paper_id) and (t1.author_sequence_number=t2.author_sequence_number) and (t1.match_author=t2.match_author)
        # and t1.author_id is null

        # make new authors of everyone else

        # select * from util.max_openalex_id
        # -- 4207094444
        #
        # with new_ids_for_authors as
        # (
        #     SELECT
        #     paper_id,
        #     author_sequence_number,
        #     1 + 4207094444 + row_number() over (partition by 1) AS new_author_id  YOU MUST UPDATE THIS NUMBER
        #     FROM mid.affiliation
        #     where author_id is null
        #     group by paper_id, author_sequence_number
        # )
        # update mid.affiliation set author_id=t2.new_author_id, updated_date=sysdate
        # -- select *
        # from mid.affiliation t1
        # join new_ids_for_authors t2 on t2.paper_id=t1.paper_id
        # where t2.author_sequence_number = t1.author_sequence_number

        # insert into mid.author (author_id, display_name, created_date, updated_date, match_name, last_known_affiliation_id)
        # select author_id, original_author, sysdate, sysdate, match_author, max(affiliation_id) as affiliation_id
        # from mid.affiliation
        # where author_id > 4207094444  YOU MUST UPDATE THIS NUMBER
        # group by author_id, original_author, match_author

        # add author orcid if not already there
        # insert into mid.author_orcid (author_id, orcid, updated, evidence)
        # (select author_id, original_orcid, sysdate, 'from recordthresher'
        # from mid.affiliation where (author_id is not null) and (original_orcid is not null)
        # and (author_id not in (select author_id from mid.author_orcid))
        # group by author_id, original_orcid
        # )


        # update the updated date of works
        # update mid.work set updated_date=t2.updated_date
        # from mid.work t1
        # join mid.affiliation t2 on t1.paper_id=t2.paper_id
        # where t1.updated_date < t2.updated_date and t2.updated_date is not null

        # figure out how I did the recommended papers thing

        #
        # create table temp_papers_can_recommend distkey(author_id) sortkey(author_id) as
        # (select work.paper_id, publication_date, citation_count, author_id
        # from
        # mid.work work
        # join mid.affiliation affil on affil.paper_id=work.paper_id
        # where work.publication_date > '2017-01-01' and work.citation_count >=1 and work.citation_count is not null
        # )
        #
        # delete from temp_papers_can_recommend where author_id not in
        # (select author_id from mid.affiliation where paper_id > 4200000000)
        #
        # insert into mid.related_work (paper_id, recommended_paper_id, score, updated)
        # with relevant_recommendations as
        # (
        # select base_auth.paper_id,
        #     rec_list.paper_id as recommended_paper_id,
        #     row_number() OVER (PARTITION BY base_auth.paper_id ORDER BY citation_count desc) AS row_number
        # from mid.affiliation base_auth
        # join mid.citation citation on base_auth.paper_id=citation.paper_id
        # join mid.affiliation affil on affil.paper_id=citation.paper_reference_id
        # join temp_papers_can_recommend rec_list on affil.author_id=rec_list.author_id
        # where base_auth.paper_id > 4200000000
        # and base_auth.paper_id not in (select paper_id from mid.related_work)
        # )
        # -- select count(*), count(distinct paper_id)
        # select paper_id, recommended_paper_id, 0.4 as score, sysdate as updated
        # from relevant_recommendations
        # where row_number <= 10

        #
        # refresh materialized view mid.citation_papers_mv;
        # update mid.work set reference_count=v.reference_count, citation_count=v.citation_count, estimated_citation=v.estimated_citation, updated_date=sysdate
        # from mid.work t1
        # join mid.citation_papers_mv v on t1.paper_id=v.paper_id
        # where (v.reference_count != t1.reference_count) or (v.citation_count != t1.citation_count) or (v.estimated_citation != t1.estimated_citation);
        # update mid.work set updated_date = created_date::timestamp where updated_date is null;
        #
        # refresh materialized view mid.citation_authors_mv;
        # update mid.author set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
        # from mid.author t1
        # join mid.citation_authors_mv v on t1.author_id=v.author_id
        # where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
        # update mid.author set updated_date = created_date::timestamp where updated_date is null;
        #
        # refresh materialized view mid.citation_journals_mv;
        # update mid.journal set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
        # from mid.journal t1
        # join mid.citation_journals_mv v on t1.journal_id=v.journal_id
        # where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
        # update mid.journal set updated_date = created_date::timestamp where updated_date is null;
        #
        #
        # refresh materialized view mid.citation_institutions_mv;
        # update mid.institution set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
        # from mid.institution t1
        # join mid.citation_institutions_mv v on t1.affiliation_id=v.affiliation_id
        # where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
        # update mid.institution set updated_date = created_date::timestamp where updated_date is null;
        #
        #
        # refresh materialized view mid.citation_concepts_mv;
        # update mid.concept set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
        # from mid.concept t1
        # join mid.citation_concepts_mv v on t1.field_of_study_id=v.field_of_study_id
        # where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
        # update mid.concept set updated_date = created_date::timestamp where updated_date is null;
        #
        #
        # refresh materialized view mid.citation_authors_by_year_mv;
        # refresh materialized view mid.citation_journals_by_year_mv;
        # refresh materialized view mid.citation_institutions_by_year_mv;
        # refresh materialized view mid.citation_concepts_by_year_mv;
        # refresh materialized view mid.citation_papers_by_year_mv;
        #
        # refresh materialized view mid.concept_for_api_mv;
        # refresh materialized view mid.work_concept_for_api_mv;
        # refresh materialized view mid.author_concept_for_api_mv;


        pass

    def to_dict(self, return_level="full"):
        response = {}

        # author_position set in works
        if hasattr(self, "author_position"):
            response["author_position"] = self.author_position

        # keep in this order so author_position is at the top
        response.update({"author": {}, "institution": {}})

        if self.original_author:
            response["author"] = {"id": self.author_id, "display_name": self.original_author, "orcid": None}
        if self.original_affiliation:
            response["institution"] = {"id": self.affiliation_id, "display_name": self.original_affiliation, "ror": None, "country_code": None, "type": None}

        # overwrite display name with better ones from these dicts if we have them
        if self.author:
            response["author"].update(self.author.to_dict(return_level="minimum"))
        if self.institution:
            response["institution"].update(self.institution.to_dict(return_level="minimum"))

        response["author_sequence_number"] = self.author_sequence_number
        response["raw_affiliation_string"] = self.original_affiliation

        return response



    def __repr__(self):
        return "<Affiliation ( {} ) {} {}>".format(self.paper_id, self.author_id, self.affiliation_id)
