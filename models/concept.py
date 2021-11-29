from cached_property import cached_property
from sqlalchemy import text

from app import db


# truncate mid.concept
# insert into mid.concept (select * from legacy.mag_advanced_fields_of_study)

class Concept(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "concept"

    field_of_study_id = db.Column(db.BigInteger, primary_key=True)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    main_type = db.Column(db.Text)
    level = db.Column(db.Numeric)
    created_date = db.Column(db.DateTime)

    @cached_property
    def parents(self):
        q = """
        WITH RECURSIVE leaf (child_field_of_study_id, child_field, child_level, field_of_study_id, parent_field, parent_level) AS (
        SELECT  linking.child_field_of_study_id,
                child_fields.display_name as child_field,
                child_fields.level as child_level,
                linking.field_of_study_id,
                parent_fields.display_name as parent_field,
                parent_fields.level as parent_level
        FROM    legacy.mag_advanced_field_of_study_children linking
        JOIN    legacy.mag_advanced_fields_of_study child_fields on child_fields.field_of_study_id=linking.child_field_of_study_id
        JOIN    legacy.mag_advanced_fields_of_study parent_fields on parent_fields.field_of_study_id=linking.field_of_study_id
        WHERE   child_field_of_study_id = :concept_id
        UNION ALL 
        SELECT  linking2.child_field_of_study_id,
                child_fields2.display_name as child_field,
                child_fields2.level as child_level,
                linking2.field_of_study_id,
                parent_fields2.display_name as parent_field,
                parent_fields2.level as parent_level
        FROM    legacy.mag_advanced_field_of_study_children linking2
        JOIN    legacy.mag_advanced_fields_of_study child_fields2 on child_fields2.field_of_study_id=linking2.child_field_of_study_id
        JOIN    legacy.mag_advanced_fields_of_study parent_fields2 on parent_fields2.field_of_study_id=linking2.field_of_study_id
        
        INNER JOIN leaf l 
        On l.field_of_study_id = linking2.child_field_of_study_id
        )
        SELECT distinct field_of_study_id, parent_field, parent_level FROM leaf;
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        parents = [{"field_of_study_id": row["field_of_study_id"],
                    "display_name": row["parent_field"],
                    "level": row["parent_level"]} for row in rows]
        parents = sorted(parents, key=lambda x: (x["level"], x["display_name"]), reverse=True)
        return parents

    @cached_property
    def paper_count(self):
        q = """select count(distinct paper_id) 
            from mid.work_concept work_concept
            where field_of_study = :concept_id;"""
        row = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).first()
        paper_count = row[0]
        return paper_count

    @cached_property
    def citation_count(self):
        q = """select count(distinct citation.paper_id) 
            from mid.citation citation
            join mid.work_concept work_concept on work_concept.paper_id=citation.paper_reference_id             
            where field_of_study = :concept_id;"""
        row = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).first()
        citation_count = row[0]
        return citation_count

    def to_dict(self, return_level="full"):
        response = {
            "field_of_study_id": self.field_of_study_id,
            "display_name": self.display_name,
            "main_type": self.main_type,
            "level": self.level,
            "paper_count": self.paper_count,   # NO_CITATIONS_FOR_NOW
            "citation_count": self.citation_count,   # NO_CITATIONS_FOR_NOW
            "parent_concepts": self.parents, # NO_CITATIONS_FOR_NOW
            "created_date": self.created_date,
        }
        return response

    def __repr__(self):
        return "<Concept ( {} ) {}>".format(self.field_of_study_id, self.display_name)


