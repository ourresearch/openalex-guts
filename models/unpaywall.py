import json

from cached_property import cached_property

from app import db


class Unpaywall(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "unpaywall_recordthresher_fields_mv"

    recordthresher_id = db.Column(db.Text, db.ForeignKey("ins.recordthresher_record.id"), primary_key=True)
    doi = db.Column(db.Text)
    updated = db.Column(db.DateTime)
    oa_status = db.Column(db.Text)
    is_paratext = db.Column(db.Boolean)
    best_oa_location_url = db.Column(db.Text)
    best_oa_location_version = db.Column(db.Text)
    best_oa_location_license = db.Column(db.Text)
    oa_locations_json = db.Column(db.Text)

    @cached_property
    def oa_locations(self):
        if not self.oa_locations_json:
            return []
        return [
            loc for loc in json.loads(self.oa_locations_json)
            if not loc.get('endpoint_id') == 'trmgzrn8eq4yx7ddvmzs'  # semantic scholar
        ]

    def __repr__(self):
        return "<Unpaywall ( {} ) '{}' {}>".format(self.recordthresher_id, self.doi, self.oa_status)
