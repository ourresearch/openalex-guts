from app import db
from sqlalchemy.dialects.postgresql import JSONB


class RecordTrackEvent(db.Model):
    __table_args__ = {"schema": "logs"}
    __tablename__ = "recordtrack_event"

    id = db.Column(db.BigInteger, primary_key=True)
    recordtrack_record_id = db.Column(
        db.BigInteger, db.ForeignKey("logs.recordtrack_record.id")
    )
    event_timestamp = db.Column(db.DateTime)
    payload = db.Column(JSONB)
    query_type = db.Column(db.Text)
    query = db.Column(db.Text)
    note = db.Column(db.Text)
    payload_hash = db.Column(db.Text)
