import datetime

import shortuuid
import random


from app import db
from util import normalize_title
from util import jsonify_fast_no_sort_raw


class WorkJson(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_json"

    paper_id = db.Column(db.BigInteger, primary_key=True)
    json_elastic = db.Column(db.Text)

    def to_dict(self, return_level="full"):
        return self.json_elastic

    def __repr__(self):
        return "<WorkJson ( {} ) >".format(self.paper_id)



