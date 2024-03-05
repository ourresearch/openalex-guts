import datetime

from sqlalchemy.dialects.postgresql import JSONB

from app import db
from bulk_actions import create_bulk_actions
import models
from app import SUBFIELDS_INDEX


class Subfield(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "subfield"

    subfield_id = db.Column(db.Integer, db.ForeignKey("mid.topic.subfield_id"), primary_key=True)
    display_name = db.Column(db.Text)
    display_name_alternatives = db.Column(JSONB)
    description = db.Column(db.Text)
    wikidata_url = db.Column(db.Text)
    wikipedia_url = db.Column(db.Text)
    json_entity_hash = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @property
    def id(self):
        return self.subfield_id

    @property
    def openalex_id(self):
        return f"https://openalex.org/subfields/{self.id}"

    def field(self):
        field_query = (
            db.session.query(models.Field)
            .join(models.Topic, models.Field.field_id == models.Topic.field_id)
            .filter(models.Topic.subfield_id == self.subfield_id)
            .first()
        )
        return field_query.to_dict(return_level="minimum")

    def domain(self):
        domain_query = (
            db.session.query(models.Domain)
            .join(models.Topic, models.Domain.domain_id == models.Topic.domain_id)
            .filter(models.Topic.subfield_id == self.subfield_id)
            .first()
        )
        return domain_query.to_dict(return_level="minimum")

    def topics_list(self):
        topics_query = (
            db.session.query(models.Topic)
            .filter(models.Topic.subfield_id == self.subfield_id)
            .all()
        )
        topics_list = [{"id": topic.openalex_id, "display_name": topic.display_name} for topic in topics_query]
        topics_list_sorted = sorted(topics_list, key=lambda x: x['display_name'].lower())
        return topics_list_sorted

    def siblings(self):
        field_id = self.field().get('id').split("/")[-1]
        siblings_query = (
            db.session.query(models.Subfield)
            .join(models.Topic, models.Subfield.subfield_id == models.Topic.subfield_id)
            .filter(models.Topic.field_id == field_id)
            .all()
        )
        subfields_list = [
            {'id': subfield.openalex_id, 'display_name': subfield.display_name}
            for subfield in siblings_query
            if subfield.subfield_id != self.subfield_id
        ]
        subfields_list_sorted = sorted(subfields_list, key=lambda x: x['display_name'].lower())
        return subfields_list_sorted

    def store(self):
        bulk_actions, new_entity_hash = create_bulk_actions(self, SUBFIELDS_INDEX)
        self.json_entity_hash = new_entity_hash
        return bulk_actions

    def to_dict(self, return_level="full"):
        response = {
            'id': self.openalex_id,
            'display_name': self.display_name
        }
        if return_level == "full":
            response.update({
                "description": self.description,
                "display_name_alternatives": self.display_name_alternatives,
                "ids": {
                    "wikidata": self.wikidata_url,
                    "wikipedia": self.wikipedia_url.replace(" ", "_") if self.wikipedia_url else None
                },
                "field": self.field(),
                "domain": self.domain(),
                "topics": self.topics_list(),
                "siblings": self.siblings(),
                "works_count": int(self.counts.paper_count or 0) if self.counts else 0,
                "cited_by_count": int(self.counts.citation_count or 0) if self.counts else 0,
                "works_api_url": f"https://api.openalex.org/works?filter=topics.subfield.id:{self.subfield_id}",
                "updated_date": datetime.datetime.utcnow().isoformat(),
                "created_date": self.created_date.isoformat()[0:10] if isinstance(self.created_date, datetime.datetime) else self.created_date[0:10]
            })
        return response

    def __repr__(self):
        return "<Subfield ( {} ) {}>".format(self.subfield_id, self.display_name)
