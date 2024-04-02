import datetime
import json

from cached_property import cached_property
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import ARRAY

from app import COUNTRIES_ENDPOINT_PREFIX
from app import MAX_MAG_ID
from app import db
from app import get_apiurl_from_openalex_url
from app import logger
from util import entity_md5
from util import truncate_on_word_break


class SourceLanguageOverride(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "journal_language_override"

    source_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True
    )
    language = db.Column(db.Text)
    updated_date = db.Column(db.DateTime)
    note = db.Column(db.Text)

    @property
    def id(self):
        return self.source_id

    def __repr__(self):
        return "<SourceLanguageOverride {} {}>".format(self.id, self.language)
