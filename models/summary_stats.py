from app import db


class AuthorImpactFactor(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author_impact_factor_mv"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    impact_factor = db.Column(db.Float)

    def __repr__(self):
        return "<AuthorImpactFactor ( {} ) {} >".format(self.author_id, self.impact_factor)


class SourceImpactFactor(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "source_impact_factor_mv"

    source_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    impact_factor = db.Column(db.Float)

    def __repr__(self):
        return "<SourceImpactFactor ( {} ) {} >".format(self.source_id, self.impact_factor)


class PublisherImpactFactor(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "publisher_impact_factor_mv"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True)
    impact_factor = db.Column(db.Float)

    def __repr__(self):
        return "<PublisherImpactFactor ( {} ) {} >".format(self.publisher_id, self.impact_factor)


class FunderImpactFactor(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "funder_impact_factor_mv"

    funder_id = db.Column(db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True)
    impact_factor = db.Column(db.Float)

    def __repr__(self):
        return "<FunderImpactFactor ( {} ) {} >".format(self.author_id, self.impact_factor)


class InstitutionImpactFactor(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution_impact_factor_mv"

    institution_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    impact_factor = db.Column(db.Float)

    def __repr__(self):
        return "<InstitutionImpactFactor ( {} ) {} >".format(self.funder_id, self.impact_factor)


class ConceptImpactFactor(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "concept_impact_factor_mv"

    concept_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    impact_factor = db.Column(db.Float)

    def __repr__(self):
        return "<ConceptImpactFactor ( {} ) {} >".format(self.author_id, self.impact_factor)


class AuthorHIndex(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author_h_index_mv"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    h_index = db.Column(db.Integer)

    def __repr__(self):
        return "<AuthorHIndex ( {} ) {} >".format(self.author_id, self.h_index)


class SourceHIndex(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "source_h_index_mv"

    source_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    h_index = db.Column(db.Integer)

    def __repr__(self):
        return "<SourceHIndex ( {} ) {} >".format(self.source_id, self.h_index)


class PublisherHIndex(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "publisher_h_index_mv"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True)
    h_index = db.Column(db.Integer)

    def __repr__(self):
        return "<PublisherHIndex ( {} ) {} >".format(self.publisher_id, self.h_index)


class FunderHIndex(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "funder_h_index_mv"

    funder_id = db.Column(db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True)
    h_index = db.Column(db.Integer)

    def __repr__(self):
        return "<FunderHIndex ( {} ) {} >".format(self.author_id, self.h_index)


class InstitutionHIndex(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution_h_index_mv"

    institution_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    h_index = db.Column(db.Integer)

    def __repr__(self):
        return "<InstitutionHIndex ( {} ) {} >".format(self.funder_id, self.h_index)


class ConceptHIndex(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "concept_h_index_mv"

    concept_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    h_index = db.Column(db.Integer)

    def __repr__(self):
        return "<ConceptHIndex ( {} ) {} >".format(self.author_id, self.h_index)


class AuthorI10Index(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author_i10_index_mv"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    i10_index = db.Column(db.Integer)

    def __repr__(self):
        return "<AuthorI10Index ( {} ) {} >".format(self.author_id, self.h_index)


class SourceI10Index(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "source_i10_index_mv"

    source_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    i10_index = db.Column(db.Integer)

    def __repr__(self):
        return "<SourceI10Index ( {} ) {} >".format(self.source_id, self.h_index)


class PublisherI10Index(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "publisher_i10_index_mv"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True)
    i10_index = db.Column(db.Integer)

    def __repr__(self):
        return "<PublisherI10Index ( {} ) {} >".format(self.publisher_id, self.h_index)


class FunderI10Index(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "funder_i10_index_mv"

    funder_id = db.Column(db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True)
    i10_index = db.Column(db.Integer)

    def __repr__(self):
        return "<FunderI10Index ( {} ) {} >".format(self.author_id, self.h_index)


class InstitutionI10Index(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "institution_i10_index_mv"

    institution_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    i10_index = db.Column(db.Integer)

    def __repr__(self):
        return "<InstitutionI10Index ( {} ) {} >".format(self.funder_id, self.h_index)


class ConceptI10Index(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "concept_i10_index_mv"

    concept_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    i10_index = db.Column(db.Integer)

    def __repr__(self):
        return "<ConceptI10Index ( {} ) {} >".format(self.author_id, self.h_index)
