from app import db


class JsonWorks(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_works"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    abstract_inverted_index = db.Column(db.Text)
    json_save_with_abstract = db.Column(db.Text)
    authors_count = db.Column(db.Integer)
    concepts_count = db.Column(db.Integer)
    merge_into_id = db.Column(db.BigInteger)


class JsonAuthors(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_authors"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)


class JsonInstitutions(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_institutions"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)


class JsonVenues(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_venues"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)


class JsonFunders(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_funders"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)


class JsonSources(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_sources"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)


class JsonConcepts(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_concepts"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)


class JsonPublishers(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "json_publishers"
    id = db.Column(db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True)
    updated = db.Column(db.DateTime)
    changed = db.Column(db.DateTime)
    json_save = db.Column(db.Text)
    version = db.Column(db.Text)
    merge_into_id = db.Column(db.BigInteger)
