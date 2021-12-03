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
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    created_date = db.Column(db.DateTime)

    @cached_property
    def ancestors(self):
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
        ancestors = [{"id": row["field_of_study_id"],
                    "display_name": row["parent_field"],
                    "level": row["parent_level"]} for row in rows]
        ancestors = sorted(ancestors, key=lambda x: (x["level"], x["display_name"]), reverse=True)
        return ancestors

    @cached_property
    def extended_attributes(self):
        q = """
        select attribute_type, attribute_value
        from legacy.mag_advanced_field_of_study_extended_attributes
        WHERE field_of_study_id = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        extended_attributes = [{"attribute_type": row["attribute_type"],
                    "attribute_value": row["attribute_value"]} for row in rows]
        return extended_attributes

    @cached_property
    def wikipedia_url(self):
        for attr_dict in self.extended_attributes:
            if attr_dict["attribute_type"] == 2:
                wiki_url = attr_dict["attribute_value"]
                page_title = wiki_url.rsplit("/", 1)[-1]
                url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
                return url
        return None

    @cached_property
    def related_fields1(self):
        q = """
        select type1, field_of_study_id2, type2, 
        concept2.display_name as field_of_study_id2_display_name,
        concept2.level as field_of_study_id2_level,
        related.rank        
        from legacy.mag_advanced_related_field_of_study related
        join mid.concept concept2 on concept2.field_of_study_id = field_of_study_id2
        WHERE field_of_study_id1 = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        related_fields1 = [{"type1": row["type1"],
                    "id": row["field_of_study_id2"],
                    "display_name": row["field_of_study_id2_display_name"],
                    "level": row["field_of_study_id2_level"],
                    "type2": row["type2"],
                    "rank": row["rank"]
                            } for row in rows]
        related_fields1 = sorted(related_fields1, key=lambda x: (x["rank"]), reverse=True)
        return related_fields1

    @cached_property
    def related_fields2(self):
        q = """
        select type1, field_of_study_id1, type2, 
        concept1.display_name as field_of_study_id1_display_name,
        concept1.level as field_of_study_id1_level,        
        related.rank       
        from legacy.mag_advanced_related_field_of_study related
        join mid.concept concept1 on concept1.field_of_study_id = field_of_study_id1
        WHERE field_of_study_id2 = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        related_fields2 = [{"type1": row["type1"],
                    "id": row["field_of_study_id1"],
                    "display_name": row["field_of_study_id1_display_name"],
                    "level": row["field_of_study_id1_level"],
                    "type2": row["type2"],
                    "rank": row["rank"]
                            } for row in rows]
        related_fields2 = sorted(related_fields2, key=lambda x: (x["rank"]), reverse=True)
        return related_fields2

    def to_dict(self, return_level="full"):
        response = {
            "field_of_study_id": self.field_of_study_id,
            "display_name": self.display_name,
            "main_type": self.main_type,
            "level": self.level,
            "paper_count": self.paper_count,
            "citation_count": self.citation_count,
            "wikipedia_data_url": self.wikipedia_url,
            "ancestors": self.ancestors,
            "extended_attributes": self.extended_attributes,
            "related_fields1": self.related_fields1,
            "related_fields2": self.related_fields2,
            "created_date": self.created_date,
        }
        return response

    def __repr__(self):
        return "<Concept ( {} ) {}>".format(self.field_of_study_id, self.display_name)


