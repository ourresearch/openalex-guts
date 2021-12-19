from app import db


class AuthorCountsByYear(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_authors_by_year_mv"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def to_dict(self):
        response = {
            "year": self.year,
            "works_count": self.paper_count,
            "cited_by_count": self.citation_count,
        }
        return response

    def __repr__(self):
        return "<AuthorCountsByYear ( {} ) {} {} >".format(self.author_id, self.paper_count, self.citation_count)


class ConceptCountsByYear(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_concepts_by_year_mv"

    field_of_study_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept.field_of_study_id"), primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def to_dict(self):
        response = {
            "year": self.year,
            "works_count": self.paper_count,
            "cited_by_count": self.citation_count,
        }
        return response

    def __repr__(self):
        return "<ConceptCountsByYear ( {} ) {} {} >".format(self.field_of_study_id, self.paper_count, self.citation_count)


class InstitutionCountsByYear(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_institutions_by_year_mv"

    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def to_dict(self):
        response = {
            "year": self.year,
            "works_count": self.paper_count,
            "cited_by_count": self.citation_count,
        }
        return response

    def __repr__(self):
        return "<InstitutionCountsByYear ( {} ) {} {} >".format(self.affiliation_id, self.paper_count, self.citation_count)


class VenueCountsByYear(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_journals_by_year_mv"

    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def to_dict(self):
        response = {
            "year": self.year,
            "works_count": self.paper_count,
            "cited_by_count": self.citation_count,
        }
        return response

    def __repr__(self):
        return "<VenueCountsByYear ( {} ) {} {} >".format(self.journal_id, self.paper_count, self.citation_count)


class WorkCountsByYear(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_papers_by_year_mv"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    citation_count = db.Column(db.Numeric)
    estimated_citation = db.Column(db.Numeric)

    def to_dict(self):
        response = {
            "year": self.year,
            "cited_by_count": self.citation_count,
        }
        return response

    def __repr__(self):
        return "<WorkCountsByYear ( {} ) {} {} >".format(self.paper_id, self.reference_count, self.citation_count)

