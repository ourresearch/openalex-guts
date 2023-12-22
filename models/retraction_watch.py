import json

from cached_property import cached_property

from app import db


class RetractionWatch(db.Model):
    __table_args__ = {'schema': 'ins'}
    __tablename__ = "retraction_watch"

    record_id = db.Column(db.BigInteger, primary_key=True)
    OriginalPaperDOILower = db.Column(db.Text, db.ForeignKey("mid.work.doi_lower"))
    RetractionNature = db.Column(db.Text)

    @cached_property
    def is_retracted(self):
        if self.RetractionNature.lower() == 'reinstatement':
            return False
        else:
            return True

    def __repr__(self):
        return "<RetractionWatch ( {} ) '{}' {}>".format(self.record_id, self.OriginalPaperDOILower, self.RetractionNature)
