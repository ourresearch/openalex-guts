from app import db

# copy ins.unpaywall_raw from
# 's3://unpaywall-daily-snapshots/unpaywall_snapshot_2021-11-01T083001.jsonl.gz'
# credentials 'CREDS HERE'
# COMPUPDATE ON
# GZIP
# JSON 'auto ignorecase'
# TRUNCATECOLUMNS
# TRIMBLANKS
# region 'us-west-2';

# create or replace view ins.oa_locations_for_unload_view as (
# select doi,
#     replace(replace(replace(replace(oa_locations,
#                  '[{', '{'), '}]', '}'), '},{', '}\n{'),
#                  '"url"',
#                  '"doi": "' || doi || '", "url"'
#                  )
#                  as oa_location_jsonlines
#     from ins.unpaywall_raw
#     where is_oa
#     and doi not like '%\\\\%'
#     and len(oa_locations) < 50000
# ) with no schema binding;
#
# #
# unload ('select oa_location_jsonlines from oa_locations_for_unload_view')
# to 's3://openalex-sandbox/unpaywall/oa_locations.jsonl.gz'
# credentials 'CREDS HERE'
# ALLOWOVERWRITE
# gzip;
#
#
# copy ins.unpaywall_oa_location_raw from 's3://openalex-sandbox/unpaywall/oa_locations.jsonl.gz'
# credentials 'CREDS HERE'
# COMPUPDATE ON
# GZIP
# JSON 'auto ignorecase'
# MAXERROR as 100;



class Unpaywall(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "unpaywall"

    doi = db.Column(db.Text, db.ForeignKey("mid.work.doi_lower"), primary_key=True)
    genre = db.Column(db.Text)
    journal_is_oa = db.Column(db.Text)
    oa_status = db.Column(db.Text)
    has_green = db.Column(db.Boolean)
    is_oa_bool = db.Column(db.Boolean)
    best_version = db.Column(db.Text)
    best_license = db.Column(db.Text)
    best_host_type = db.Column(db.Text)
    best_url = db.Column(db.Text)

    def to_dict(self, return_level="full"):
        keys = [col.name for col in self.__table__.columns if col.name not in ["doi"]]
        return {key: getattr(self, key) for key in keys}

    def __repr__(self):
        return "<Unpaywall ( {} ) '{}' {}>".format(self.issn_l, self.title, self.publisher)


