from app import db


class AuthorCounts(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_authors_view"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCounts ( {} ) {} {} >".format(self.author_id, self.paper_count, self.citation_count)


class AuthorCountsByYearPapers(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_authors_by_year_paper_count_view"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCountsByYearPapers ( {} ) {} {} >".format(self.author_id, self.year, self.n)


class AuthorCountsByYearCitations(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_authors_by_year_citation_count_view"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCountsByYearCitations ( {} ) {} {} >".format(self.author_id, self.year, self.n)


class VenueCounts(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_journals_mv"

    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<VenueCounts ( {} ) {} {} >".format(self.journal_id, self.paper_count, self.citation_count)


class VenueCountsByYearPapers(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_journals_by_year_paper_count_view"

    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<VenueCountsByYearPapers ( {} ) {} {} >".format(self.journal_id, self.year, self.n)


class VenueCountsByYearCitations(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_journals_by_year_citation_count_view"

    journal_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<VenueCountsByYearCitations ( {} ) {} {} >".format(self.journal_id, self.year, self.n)


class PublisherCounts(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_publishers_mv"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<VenueCounts ( {} ) {} {} >".format(self.publisher_id)


class PublisherCountsByYearPapers(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_publishers_by_year_paper_count_view"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<VenueCountsByYearPapers ( {} ) {} {} >".format(self.publisher_id, self.year, self.n)


class PublisherCountsByYearCitations(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_publishers_by_year_citation_count_view"

    publisher_id = db.Column(db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<VenueCountsByYearCitations ( {} ) {} {} >".format(self.publisher_id, self.year, self.n)


class InstitutionCounts(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_institutions_mv"

    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCounts ( {} ) {} {} >".format(self.affiliation_id, self.paper_count, self.citation_count)


class InstitutionCountsByYearPapers(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_institutions_by_year_paper_count_view"

    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCountsByYearPapers ( {} ) {} {} >".format(self.affiliation_id, self.year, self.n)


class InstitutionCountsByYearCitations(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_institutions_by_year_citation_count_view"

    affiliation_id = db.Column(db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCountsByYearCitations ( {} ) {} {} >".format(self.affiliation_id, self.year, self.n)


class ConceptCounts(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_concepts_mv"

    field_of_study_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<ConceptCounts ( {} ) {} {} >".format(self.field_of_study_id, self.paper_count, self.citation_count)


class ConceptCountsByYear(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_concepts_by_year_mv"

    field_of_study_id = db.Column(db.BigInteger, db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<ConceptCountsByYear ( {} ) {} {} >".format(self.field_of_study_id, self.year, self.n)


class WorkCounts(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_papers_mv"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<WorkCounts ( {} ) {} >".format(self.paper_id, self.citation_count)


class WorkCountsByYear(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "citation_papers_by_year_mv"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<WorkCountsByYear ( {} ) {} >".format(self.paper_id, self.year, self.n)

