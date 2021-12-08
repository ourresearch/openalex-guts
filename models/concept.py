from cached_property import cached_property
from sqlalchemy import text
import requests
import urllib.parse

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
    updated_date = db.Column(db.DateTime)

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
    def umls_aui_url(self):
        for attr in self.extended_attributes:
            if attr["attribute_type"]==1:
                return attr["attribute_value"]
        return None

    @cached_property
    def wikipedia_url(self):
        for attr in self.extended_attributes:
            if attr["attribute_type"]==2:
                return attr["attribute_value"]
        encoded = urllib.parse.quote(self.display_name)
        return f"http://en.wikipedia.org/wiki/{encoded}"

    @cached_property
    def umls_cui_url(self):
        for attr in self.extended_attributes:
            if attr["attribute_type"]==3:
                return attr["attribute_value"]
        return None

    @cached_property
    def wikipedia_data_url(self):
        for attr_dict in self.extended_attributes:
            if attr_dict["attribute_type"] == 2:
                wiki_url = attr_dict["attribute_value"]
                page_title = wiki_url.rsplit("/", 1)[-1]
                url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
                return url
        return None

    @cached_property
    def related_concepts(self):
        q = """
        select type2, field_of_study_id2, 
        concept2.display_name as field_of_study_id2_display_name,
        concept2.level as field_of_study_id2_level,
        related.rank        
        from legacy.mag_advanced_related_field_of_study related
        join mid.concept concept2 on concept2.field_of_study_id = field_of_study_id2
        WHERE field_of_study_id1 = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        # not including type on purpose
        related_concepts1 = [{"id": row["field_of_study_id2"],
                            "display_name": row["field_of_study_id2_display_name"],
                            "level": row["field_of_study_id2_level"],
                            "score": row["rank"]
                            } for row in rows]

        q = """
        select type1, field_of_study_id1, 
        concept1.display_name as field_of_study_id1_display_name,
        concept1.level as field_of_study_id1_level,        
        related.rank       
        from legacy.mag_advanced_related_field_of_study related
        join mid.concept concept1 on concept1.field_of_study_id = field_of_study_id1
        WHERE field_of_study_id2 = :concept_id
        """
        rows = db.session.execute(text(q), {"concept_id": self.field_of_study_id}).fetchall()
        # not including type on purpose
        related_concepts2 = [{"id": row["field_of_study_id1"],
                            "display_name": row["field_of_study_id1_display_name"],
                            "level": row["field_of_study_id1_level"],
                            "score": row["rank"]
                            } for row in rows]

        related_concepts_all = related_concepts1 + related_concepts2

        related_concepts_dict = {}
        for row in related_concepts_all:
            related_concepts_dict[row["id"]] = row
        #do it this way to dedup
        related_concepts_all = sorted(related_concepts_dict.values(), key=lambda x: (x["score"]), reverse=True)
        # the ones with poor rank aren't good enough to include
        related_concepts_all = [field for field in related_concepts_all if field["score"] >= 0.75 and field["level"] <= self.level + 1]
        return related_concepts_all

    @cached_property
    def wikipedia_pageid(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["pageid"]
        except KeyError:
            return None

        return page_id

    @cached_property
    def image_url(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["original"]["source"]
        except KeyError:
            return None

        return page_id

    @cached_property
    def image_thumbnail_url(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["thumbnail"]["source"]
        except KeyError:
            return None

        return page_id

    @cached_property
    def description(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["terms"]["description"][0]
        except KeyError:
            return None

    @cached_property
    def wikipedia_title(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        # print(data)
        try:
            return data["query"]["pages"][0]["title"]
        except KeyError:
            return None

    @cached_property
    def wikidata_id(self):
        if not self.wikipedia_data:
            return None
        data = self.wikipedia_data
        try:
            page_id = data["query"]["pages"][0]["pageprops"]["wikibase_item"]
        except KeyError:
            return None

        return page_id

    @cached_property
    def wikipedia_data(self):
        if not self.wikipedia_url:
            return None
        wikipedia_page_name = self.wikipedia_url.rsplit("/", 1)[-1]
        url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageprops%7Cpageimages%7Cpageterms&piprop=original%7Cthumbnail&titles={wikipedia_page_name}&pithumbsize=100&redirects="
        # print(url)
        r = requests.get(url)
        # print(r.json())
        return r.json()

    # is whatever the wikipedia url redirects to
    @cached_property
    def wikipedia_url_canonical(self):
        if not self.wikipedia_title:
            return None
        encoded = urllib.parse.quote(self.wikipedia_title)
        return f"http://en.wikipedia.org/wiki/{encoded}"

    @cached_property
    def display_name_international(self):
        if not self.wikidata_data:
            return None
        data = self.wikidata_data
        try:
            response = data["entities"][self.wikidata_id]["labels"]
            return dict(sorted(response.items()))
        except KeyError:
            return None

    @cached_property
    def description_international(self):
        if not self.wikidata_data:
            return None
        data = self.wikidata_data
        try:
            response = data["entities"][self.wikidata_id]["descriptions"]
            return dict(sorted(response.items()))
        except KeyError:
            return None

    @cached_property
    def wikidata_data(self):
        if not self.wikidata_id:
            return None
        url = f"https://www.wikidata.org/wiki/Special:EntityData/{self.wikidata_id}.json"
        r = requests.get(url)
        # print(r.json())
        return r.json()

    def to_dict(self, return_level="full"):
        response = {
            "id": self.field_of_study_id,
            "display_name": self.display_name,
            "level": self.level,
        }
        if return_level == "full":
            response.update({
                "works_count": self.paper_count,
                "cited_by_count": self.citation_count,
                "wikipedia_url": self.wikipedia_url_canonical,
                "wikipedia_pageid": self.wikipedia_pageid,
                "umls_aui": self.umls_aui_url,
                "umls_cui": self.umls_cui_url,
                "wikidata_id": self.wikidata_id,
                "image_url": self.image_url,
                "image_thumbnail_url": self.image_thumbnail_url,
                "display_name_international": self.display_name_international,
                "description_international": self.description_international,
                # "description": self.description,
                "ancestors": self.ancestors,
                "related_concepts": self.related_concepts,
                "works_api_url": "https://api.openalex.org/works?filter=concept:2778407487",
                "updated_date": self.updated_date
            })
        return response

    def __repr__(self):
        return "<Concept ( {} ) {}>".format(self.field_of_study_id, self.display_name)


