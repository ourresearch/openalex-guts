from cached_property import cached_property
from sqlalchemy import text
import json

from app import db


# truncate mid.author
# insert into mid.author (select * from legacy.mag_main_authors)
# update mid.author set display_name=replace(display_name, '\t', '') where display_name ~ '\t';

class Author(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author"

    author_id = db.Column(db.BigInteger, primary_key=True)
    display_name = db.Column(db.Text)
    last_known_affiliation_id = db.Column(db.Numeric, db.ForeignKey("mid.institution.affiliation_id"))
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    created_date = db.Column(db.DateTime)
    updated_date = db.Column(db.DateTime)

    @property
    def last_known_institution_id(self):
        return self.last_known_affiliation_id

    @property
    def last_known_institution_api_url(self):
        if not self.last_known_affiliation_id:
            return None
        return f"http://localhost:5007/institution/id/{self.last_known_affiliation_id}"

    @property
    def orcid(self):
        if not self.orcids:
            return None
        return sorted(self.orcids, key=lambda x: x.orcid)[0].orcid

    @property
    def orcid_url(self):
        if not self.orcid:
            return None
        return "https://orcid.org/{}".format(self.orcid)

    @cached_property
    def papers(self):
        q = "select paper_id from mid.affiliation where author_id = :author_id;"
        rows = db.session.execute(text(q), {"author_id": self.author_id}).fetchall()
        paper_ids = [row[0] for row in rows]
        return paper_ids

    @cached_property
    def citations(self):
        q = """select citation.paper_id as cited_paper_id 
            from mid.affiliation affil
            join mid.citation citation on affil.paper_id=citation.paper_reference_id
            where author_id = :author_id;"""
        rows = db.session.execute(text(q), {"author_id": self.author_id}).fetchall()
        cited_paper_ids = [row[0] for row in rows]
        return cited_paper_ids

    @cached_property
    def all_institutions(self):
        q = """select distinct institution.affiliation_id
            from mid.affiliation affil
            join mid.institution institution on affil.affiliation_id=institution.affiliation_id
            where author_id = :author_id;"""
        rows = db.session.execute(text(q), {"author_id": self.author_id}).fetchall()
        response = list(set([row[0] for row in rows]))
        return response

    @cached_property
    def alternative_names(self):
        q = """
        select attribute_value
        from legacy.mag_main_author_extended_attributes
        WHERE author_id = :author_id
        """
        rows = db.session.execute(text(q), {"author_id": self.author_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def orcid_data_person(self):
        if not self.orcid:
            return None

        q = """
        select api_json
        from orcid_raw_from_s3
        WHERE api_json."orcid-identifier".path::text = :orcid
        """
        row = db.session.execute(text(q), {"orcid": self.orcid}).first()
        if row:
            my_data = json.loads(row[0])
            return my_data.get("person", None)
        return None

    @cached_property
    def orcid_data_works(self):
        if not self.orcid:
            return None

        q = """
        select api_json
        from orcid_raw_from_s3
        WHERE api_json."orcid-identifier".path::text = :orcid
        """
        row = db.session.execute(text(q), {"orcid": self.orcid}).first()
        if row:
            my_data = json.loads(row[0])
            return my_data.get("works", None)
        return None


    def to_dict(self, return_level="full"):
        response = {
                "id": self.author_id,
                "display_name": self.display_name,
                "orcid": self.orcid_url
                    }
        if return_level == "full":
            response.update({
                # "last_known_institution_id": self.last_known_institution_id,
                # "last_known_institution": self.last_known_institution.to_dict() if self.last_known_institution else None,
                "alternative_names": self.alternative_names,
                "external_ids": [],
                "works_count": self.paper_count,
                "cited_by_count": self.citation_count,
                "orcid_data_person": self.orcid_data_person,
                "updated_date": self.updated_date
                # "all_institutions": self.all_institutions if self.all_institutions else [], NO_CITATIONS_FOR_NOW
                # "works": self.papers, #NO_CITATIONS_FOR_NOW
                # "citations": self.citations, #NO_CITATIONS_FOR_NOW
            })
        return response

    def __repr__(self):
        return "<Author ( {} ) {}>".format(self.author_id, self.display_name)


