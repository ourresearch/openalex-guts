from app import db


# truncate mid.journal
# insert into mid.journal (select * from legacy.mag_main_journals)
# update mid.journal set display_title=replace(display_title, '\\\\/', '/');
# update mid.journal set publisher=replace(publisher, '\t', '') where publisher ~ '\t';

class Journal(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "journal"

    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.journal_id"), primary_key=True)
    # rank integer,
    normalized_name = db.Column(db.Text)
    display_name = db.Column(db.Text)
    issn = db.Column(db.Text)
    publisher = db.Column(db.Text)
    webpage = db.Column(db.Text)
    # paper_count bigint,
    # paper_family_count bigint,
    # citation_count bigint,
    updated_date = db.Column(db.DateTime)
    created_date = db.Column(db.DateTime)

    @property
    def mag_journal(self):
        return self.display_name

    def to_dict(self, return_level="full"):
        if return_level=="full":
            return {c.name: getattr(self, c.name) for c in self.__table__.columns}
        keys = ["journal_id"]
        response = {key: getattr(self, key) for key in keys}
        if self.journalsdb:
            response.update(self.journalsdb.to_dict(return_level))
        return response

    def __repr__(self):
        return "<Journal ( {} ) {}>".format(self.id, self.doi, self.pmh_id, self.pmid)


