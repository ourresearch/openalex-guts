from sqlalchemy.dialects.postgresql import JSONB

from app import db


def as_publisher_openalex_id(id):
    from app import API_HOST
    return f"{API_HOST}/P{id}"


class Publisher(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "publisher"

    publisher_id = db.Column(db.BigInteger, primary_key=True)
    display_name = db.Column(db.Text)
    alternate_titles = db.Column(JSONB)
    country_code = db.Column(db.Text)
    parent_publisher = db.Column(db.BigInteger, db.ForeignKey('mid.publisher.publisher_id'))
    ror_id = db.Column(db.Text)
    wikidata_id = db.Column(db.Text)
    country_name = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)
    merge_into_date = db.Column(db.DateTime)

    @property
    def openalex_id(self):
        return as_publisher_openalex_id(self.publisher_id)
