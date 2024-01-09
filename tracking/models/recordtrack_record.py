import json
from datetime import datetime
from app import db
from util import text_md5


def existing_hash(hash: str):
    from . import RecordTrackEvent

    results = db.session.query(RecordTrackEvent).filter_by(payload_hash=hash).all()
    return results


class RecordTrack(db.Model):
    __table_args__ = {"schema": "logs"}
    __tablename__ = "recordtrack_record"

    id = db.Column(db.BigInteger, primary_key=True)
    doi = db.Column(db.Text)
    work_id = db.Column(db.BigInteger)
    arxiv_id = db.Column(db.Text)
    pmid = db.Column(db.BigInteger)
    pmh_id = db.Column(db.Text)
    created_at = db.Column(db.DateTime)
    last_tracked_at = db.Column(db.DateTime)
    work_id_found = db.Column(db.DateTime)
    api_found = db.Column(db.DateTime)
    note = db.Column(db.Text)
    active = db.Column(db.Boolean)

    def track(self):
        from . import RecordTrackEvent, RecordTrackEventHash

        # ins.recordthresher_record table
        if self.doi:
            from ..trackdb import (
                query_recordthresher_record_by_doi,
                work_id_from_recordthresher_result,
            )

            event_hash = RecordTrackEventHash(recordtrack_record_id=self.id)
            results = query_recordthresher_record_by_doi(self.doi)
            now = datetime.utcnow().isoformat()
            payload = [dict(x) for x in results]
            payload_str = json.dumps(payload, default=str, sort_keys=True)
            payload_hash = text_md5(payload_str)
            event_hash.query_type = "openalex-db_recordthresher_record"
            event_hash.event_timestamp = now
            event_hash.payload_hash = payload_hash
            db.session.add(event_hash)
            if not existing_hash(payload_hash):
                event = RecordTrackEvent(recordtrack_record_id=self.id)
                event.payload = json.loads(payload_str)
                event.query_type = "openalex-db_recordthresher_record"
                event.payload_hash = text_md5(payload_str)
                event.event_timestamp = now
                db.session.add(event)
            self.last_tracked_at = now
            if not self.work_id:
                work_id = work_id_from_recordthresher_result(results)
                if work_id:
                    self.work_id = work_id
                    self.work_id_found = now
            db.session.commit()

        # mid.work table
        if self.work_id:
            from ..trackdb import query_work_by_work_id

            event_hash = RecordTrackEventHash(recordtrack_record_id=self.id)
            result = query_work_by_work_id(self.work_id)
            now = datetime.utcnow().isoformat()
            payload = dict(result)
            payload_str = json.dumps(payload, default=str, sort_keys=True)
            payload_hash = text_md5(payload_str)
            event_hash.query_type = "openalex-db_work"
            event_hash.event_timestamp = now
            event_hash.payload_hash = payload_hash
            db.session.add(event_hash)
            if not existing_hash(payload_hash):
                event = RecordTrackEvent(recordtrack_record_id=self.id)
                event.payload = json.loads(payload_str)
                event.query_type = "openalex-db_work"
                event.payload_hash = text_md5(payload_str)
                event.event_timestamp = now
                db.session.add(event)
            self.last_tracked_at = now
            db.session.commit()

        # OpenAlex API
        if self.work_id:
            from ..trackapi import query_api_by_work_id

            event_hash = RecordTrackEventHash(recordtrack_record_id=self.id)
            result = query_api_by_work_id(self.work_id)
            now = datetime.utcnow().isoformat()
            if not self.api_found and result.get("id"):
                self.api_found = now
            payload_str = json.dumps(result, sort_keys=True)
            payload_hash = text_md5(payload_str)
            event_hash.query_type = "api"
            event_hash.event_timestamp = now
            event_hash.payload_hash = payload_hash
            db.session.add(event_hash)
            if not existing_hash(payload_hash):
                event = RecordTrackEvent(recordtrack_record_id=self.id)
                event.payload = result
                event.payload_hash = text_md5(payload_str)
                event.query_type = "api"
                event.event_timestamp = now
                db.session.add(event)
            self.last_tracked_at = now
            db.session.commit()

        # Fast queue (redis)
        if self.work_id:
            from redis import Redis
            from app import REDIS_QUEUE_URL
            from models import REDIS_WORK_QUEUE

            event_hash = RecordTrackEventHash(recordtrack_record_id=self.id)
            _redis = Redis.from_url(REDIS_QUEUE_URL)
            redis_rank = _redis.zrank(REDIS_WORK_QUEUE, self.work_id)
            redis_total = _redis.zcard(REDIS_WORK_QUEUE)
            payload = {
                "redis_rank": redis_rank,
                "redis_total": redis_total,
            }
            now = datetime.utcnow().isoformat()
            payload_str = json.dumps(payload, sort_keys=True)
            payload_hash = text_md5(payload_str)
            event_hash.query_type = "redis_work_queue"
            event_hash.event_timestamp = now
            event_hash.payload_hash = payload_hash
            db.session.add(event_hash)
            if not existing_hash(payload_hash):
                event = RecordTrackEvent(recordtrack_record_id=self.id)
                event.payload = payload
                event.payload_hash = text_md5(payload_str)
                event.query_type = "redis_work_queue"
                event.event_timestamp = now
                db.session.add(event)
            self.last_tracked_at = now
            db.session.commit()
