from app import db
from sqlalchemy.dialects.postgresql import JSONB


class RecordTrackEventHash(db.Model):
    """This table just stores the hash, so we don't have to store a lot of duplicates of the payload"""

    __table_args__ = {"schema": "logs"}
    __tablename__ = "recordtrack_event_hash"

    recordtrack_record_id = db.Column(
        db.BigInteger,
        db.ForeignKey("logs.recordtrack_record.id"),
        primary_key=True,
    )
    event_timestamp = db.Column(db.DateTime, primary_key=True)
    query_type = db.Column(db.Text, primary_key=True)
    payload_hash = db.Column(
        db.Text, db.ForeignKey("logs.recordtrack_event.payload_hash")
    )
    recordtrack_event_id = db.Column(
        db.BigInteger, db.ForeignKey("logs.recordtrack_event.id")
    )
