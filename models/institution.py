from cached_property import cached_property
from sqlalchemy import text

from app import db

# alter table institution rename column normalized_name to mag_normalized_name
# alter table institution add column normalized_name varchar(65000)
# update institution set normalized_name=f_normalize_title(institution.mag_normalized_name)

# truncate mid.institution
# insert into mid.institution (select * from legacy.mag_main_affiliations)
# update mid.institution set display_name=replace(display_name, '\t', '') where display_name ~ '\t';

class Institution(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution"

    affiliation_id = db.Column(db.BigInteger, primary_key=True)
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    official_page = db.Column(db.Text)
    iso3166_code = db.Column(db.Text)
    created_date = db.Column(db.DateTime)
    ror_id = db.Column(db.Text)
    grid_id = db.Column(db.Text)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    wiki_page = db.Column(db.Text)
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)

    @property
    def institution_id(self):
        return self.affiliation_id

    @property
    def institution_display_name(self):
        if self.ror:
            return self.ror.name
        return self.display_name

    @property
    def ror_url(self):
        if self.ror_id:
            return "https://ror.org/{}".format(self.ror_id)
        return None

    @property
    def country_code(self):
        if not self.iso3166_code:
            return None
        return self.iso3166_code.lower()

    @cached_property
    def wikipedia_data_url(self):
        if self.wiki_page:
            page_title = self.wiki_page.rsplit("/", 1)[-1]
            url = f"https://en.wikipedia.org/w/api.php?action=query&format=json&formatversion=2&prop=pageimages|pageterms&piprop=original|thumbnail&titles={page_title}&pithumbsize=100"
            return url
        return None

    @cached_property
    def acroynyms(self):
        q = """
        select acronym
        from ins.ror_acronyms
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def aliases(self):
        q = """
        select alias
        from ins.ror_aliases
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def external_ids(self):
        q = """
        select external_id_type, external_id
        from ins.ror_external_ids
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [{"type": row[0], "id": row[1]} for row in rows]
        return response

    @cached_property
    def geonames(self):
        q = """
        select *
        from ins.ror_geonames
        join ins.ror_addresses on ror_addresses.geonames_city_id = ror_geonames.geonames_city_id
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [dict(row) for row in rows]
        return response

    @cached_property
    def labels(self):
        q = """
        select *
        from ins.ror_labels
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [dict(row) for row in rows]
        return response

    @cached_property
    def links(self):
        q = """
        select link
        from ins.ror_links
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def relationships(self):
        q = """
        select relationship_type, 
        ror_grid_equivalents.ror_id, 
        related_grid_id as grid_id, 
        name,
        country_code 
        from ins.ror_relationships
        join ins.ror_grid_equivalents on ror_grid_equivalents.grid_id = ror_relationships.related_grid_id        
        WHERE ror_relationships.ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [dict(row) for row in rows]
        return response

    @cached_property
    def types(self):
        q = """
        select type
        from ins.ror_types
        WHERE ror_id = :ror_id
        """
        rows = db.session.execute(text(q), {"ror_id": self.ror_id}).fetchall()
        response = [row[0] for row in rows]
        return response

    @cached_property
    def wikidata_url(self):
        for attr_dict in self.external_ids:
            if attr_dict["type"] == "Wikidata":
                wikidata_id = attr_dict["id"]
                url = f"https://www.wikidata.org/wiki/{wikidata_id}"
                return url
        return None

    def to_dict(self, return_level="full"):
        from models import Ror

        response = {
            "id": self.institution_id,
            "display_name": self.display_name,
            "ror": self.ror_url
        }
        if self.ror:
            response.update(self.ror.to_dict(return_level))
        else:
            response.update(Ror.to_dict_null())
        response["country_code"] = self.country_code

        if return_level == "full":
            response.update({
                "homepage_url": self.official_page,
                "wikipedia_url": self.wiki_page,
                "wikipedia_data_url": self.wikipedia_data_url,
                "wikidata_url": self.wikidata_url,
                "latitude": self.latitude,
                "longitude": self.longitude,
                "acroynyms": self.acroynyms,
                "aliases": self.aliases,
                "labels": self.labels,
                "links": self.links,
                "relationships": self.relationships,
                "types": self.types,
                "external_ids": self.external_ids,
                "geonames": self.geonames,
                "works_count": self.paper_count,
                "cited_by_count": self.citation_count,
            })
        return response


    @classmethod
    def to_dict_null(self):
        response = {
            "id": self.institution_id,
            "display_name": self.display_name,
            "ror": None,
            "country_code": self.country_code,
            "official_page": self.official_page,
            "wikipedia_page": self.wiki_page,
            "works_count": None,
            "cited_by_count": None,
        }
        return response

    def __repr__(self):
        return "<Institution ( {} ) {}>".format(self.affiliation_id, self.display_name)


