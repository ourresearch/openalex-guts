from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from redis import Redis
from requests_cache import CachedSession, RedisCache
from sqlalchemy import PrimaryKeyConstraint

from app import db, ELASTIC_URL, REDIS_URL, WORKS_INDEX


class AuthorCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_authors_mv"

    author_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCounts ( {} ) {} {} >".format(
            self.author_id, self.paper_count, self.citation_count
        )


class AuthorCounts2Year(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_authors_2yr_mv"

    author_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCounts2Year ( {} ) {} {} >".format(
            self.author_id, self.paper_count, self.citation_count
        )


class AuthorCountsByYearPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_authors_by_year_paper_count_view"

    author_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCountsByYearPapers ( {} ) {} {} >".format(
            self.author_id, self.year, self.n
        )


class AuthorCountsByYearOAPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "authors_by_year_oa_works_count_view"

    author_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCountsByYearOAPapers ( {} ) {} {} >".format(
            self.author_id, self.year, self.n
        )


class AuthorCountsByYearCitations(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_authors_by_year_citation_count_view"

    author_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<AuthorCountsByYearCitations ( {} ) {} {} >".format(
            self.author_id, self.year, self.n
        )


class SourceCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_journals_mv"

    journal_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<SourceCounts ( {} ) {} {} >".format(
            self.journal_id, self.paper_count, self.citation_count
        )


class SourceCounts2Year(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_journals_2yr_mv"

    journal_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<SourceCounts2Year ( {} ) {} {} >".format(
            self.journal_id, self.paper_count, self.citation_count
        )


class SourceCountsByYearPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_journals_by_year_paper_count_view"

    journal_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<SourceCountsByYearPapers ( {} ) {} {} >".format(
            self.journal_id, self.year, self.n
        )


class SourceCountsByYearOAPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "journals_by_year_oa_works_count_view"

    journal_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<SourceCountsByYearOAPapers ( {} ) {} {} >".format(
            self.journal_id, self.year, self.n
        )


class SourceCountsByYearCitations(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_journals_by_year_citation_count_view"

    journal_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.journal.journal_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<SourceCountsByYearCitations ( {} ) {} {} >".format(
            self.journal_id, self.year, self.n
        )


class FunderCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_funders_mv"

    funder_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<FunderCounts ( {} ) {} {} >".format(
            self.funder_id, self.paper_count, self.citation_count
        )


class FunderCounts2Year(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_funders_2yr_mv"

    funder_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<FunderCounts2Year ( {} ) {} {} >".format(
            self.funder_id, self.paper_count, self.citation_count
        )


class FunderCountsByYearPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_funders_by_year_paper_count_view"

    funder_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<FunderCountsByYearPapers ( {} ) {} {} >".format(
            self.funder_id, self.year, self.n
        )


class FunderCountsByYearOAPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "funders_by_year_oa_works_count_view"

    funder_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<FunderCountsByYearOAPapers ( {} ) {} {} >".format(
            self.funder_id, self.year, self.n
        )


class FunderCountsByYearCitations(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_funders_by_year_citation_count_view"

    funder_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.funder.funder_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<FunderCountsByYearCitations ( {} ) {} {} >".format(
            self.funder_id, self.year, self.n
        )


class PublisherCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_publishers_mv"

    publisher_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<PublisherCounts ( {} ) {} {} >".format(self.publisher_id)


class PublisherCounts2Year(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_publishers_2yr_mv"

    publisher_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<PublisherCounts2Year ( {} ) {} {} >".format(self.publisher_id)


class PublisherCountsByYearPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_publishers_by_year_paper_count_mv"

    publisher_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<PublisherCountsByYearPapers ( {} ) {} {} >".format(
            self.publisher_id, self.year, self.n
        )


class PublisherCountsByYearOAPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "publishers_by_year_oa_works_count_mv"

    publisher_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<PublisherCountsByYearOAPapers ( {} ) {} {} >".format(
            self.publisher_id, self.year, self.n
        )


class PublisherCountsByYearCitations(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_publishers_by_year_citation_count_mv"

    publisher_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.publisher.publisher_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<PublisherCountsByYearCitations ( {} ) {} {} >".format(
            self.publisher_id, self.year, self.n
        )


class InstitutionCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_institutions_mv"

    affiliation_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCounts ( {} ) {} {} >".format(
            self.affiliation_id, self.paper_count, self.citation_count
        )


class InstitutionCounts2Year(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_institutions_2yr_mv"

    affiliation_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCounts2Year ( {} ) {} {} >".format(
            self.affiliation_id, self.paper_count, self.citation_count
        )


class InstitutionCountsByYearPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_institutions_by_year_paper_count_view"

    affiliation_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCountsByYearPapers ( {} ) {} {} >".format(
            self.affiliation_id, self.year, self.n
        )


class InstitutionCountsByYearOAPapers(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "institutions_by_year_oa_works_count_view"

    affiliation_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCountsByYearOAPapers ( {} ) {} {} >".format(
            self.affiliation_id, self.year, self.n
        )


class InstitutionCountsByYearCitations(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_institutions_by_year_citation_count_view"

    affiliation_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<InstitutionCountsByYearCitations ( {} ) {} {} >".format(
            self.affiliation_id, self.year, self.n
        )


class ConceptCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_concepts_mv"

    field_of_study_id = db.Column(
        db.BigInteger,
        db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"),
        primary_key=True,
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<ConceptCounts ( {} ) {} {} >".format(
            self.field_of_study_id, self.paper_count, self.citation_count
        )


class ConceptCounts2Year(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_concepts_2yr_mv"

    field_of_study_id = db.Column(
        db.BigInteger,
        db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"),
        primary_key=True,
    )
    paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<ConceptCounts2Year ( {} ) {} {} >".format(
            self.field_of_study_id, self.paper_count, self.citation_count
        )


class ConceptCountsByYear(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_concepts_by_year_mv"

    field_of_study_id = db.Column(
        db.BigInteger,
        db.ForeignKey("mid.concept_for_api_mv.field_of_study_id"),
        primary_key=True,
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<ConceptCountsByYear ( {} ) {} {} >".format(
            self.field_of_study_id, self.year, self.n
        )


class WorkCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_papers_mv"

    paper_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True
    )
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<WorkCounts ( {} ) {} >".format(self.paper_id, self.citation_count)


class WorkCountsByYear(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_papers_by_year_mv"

    paper_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True
    )
    type = db.Column(db.Text, primary_key=True)
    year = db.Column(db.Numeric, primary_key=True)
    n = db.Column(db.Numeric)

    def __repr__(self):
        return "<WorkCountsByYear ( {} ) {} >".format(self.paper_id, self.year, self.n)


class Work2YearCitationCount(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "paper_citations_2yr_mv"

    paper_id = db.Column(
        db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True
    )
    count = db.Column(db.Numeric)

    def __repr__(self):
        return "<Work2YearCitationCount ( {} ) {} >".format(self.paper_id, self.count)


class CitationPercentilesByYear(db.Model):
    __table_args__ = (PrimaryKeyConstraint("year", "citation_count"), {"schema": "mid"})
    __tablename__ = "citation_percentiles_by_year_mv"

    year = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)
    percentile = db.Column(db.Numeric)

    def __repr__(self):
        return "<CitationPercentilesByYear ( {} ) {} {} >".format(
            self.year, self.citation_count, self.percentile
        )


class TopicCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_topics_mv"

    topic_id = db.Column(
        db.Integer, db.ForeignKey("mid.topic.topic_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<TopicCounts ( {} ) {} {} >".format(
            self.topic_id, self.paper_count, self.citation_count
        )


class SubfieldCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_subfields_mv"

    subfield_id = db.Column(
        db.Integer, db.ForeignKey("mid.subfield.subfield_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<SubfieldCounts ( {} ) {} {} >".format(
            self.subfield_id, self.paper_count, self.citation_count
        )


class FieldCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_fields_mv"

    field_id = db.Column(
        db.Integer, db.ForeignKey("mid.field.field_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<FieldCounts ( {} ) {} {} >".format(
            self.field_id, self.paper_count, self.citation_count
        )


class DomainCounts(db.Model):
    __table_args__ = {"schema": "mid"}
    __tablename__ = "citation_domains_mv"

    domain_id = db.Column(
        db.Integer, db.ForeignKey("mid.domain.domain_id"), primary_key=True
    )
    paper_count = db.Column(db.Numeric)
    oa_paper_count = db.Column(db.Numeric)
    citation_count = db.Column(db.Numeric)

    def __repr__(self):
        return "<DomainCounts ( {} ) {} {} >".format(
            self.domain_id, self.paper_count, self.citation_count
        )


def cache_expiration():
    return 3600 * 24 * 3


def cached_session():
    connection = Redis.from_url(REDIS_URL)
    cache_backend = RedisCache(connection=connection, expire_after=None)
    session = CachedSession(
        cache_name="cache", backend=cache_backend, expire_after=cache_expiration()
    )
    return session


def works_count_from_api(group_by_key, id):
    session = cached_session()
    r = session.get(f"https://api.openalex.org/works?group-by={group_by_key}")
    group_by = r.json().get("group_by")
    for group in group_by:
        if group.get("key") == id:
            return group.get("count")


def fetch_citation_sum(key, id):
    es = Elasticsearch([ELASTIC_URL], timeout=30)
    s = Search(using=es, index=WORKS_INDEX)
    s = s.query("term", **{key: id})
    s.aggs.bucket("citation_count", "sum", field="cited_by_count")
    response = s.execute()
    return response.aggregations.citation_count.value


def citation_count_from_elastic(key, id):
    redis = Redis.from_url(REDIS_URL)
    cache_key = f"{key}_{id}_citation_count"

    # try to retrieve the cached value
    cached_citation_count = redis.get(cache_key)
    if cached_citation_count is not None:
        cached_citation_count_str = cached_citation_count.decode("utf-8")
        return int(float(cached_citation_count_str))

    # if not cached, compute the value
    citation_count = fetch_citation_sum(key, id)

    # cache the newly computed value
    redis.set(cache_key, citation_count, ex=cache_expiration())
    return int(citation_count)


def fetch_works_count(key, id):
    es = Elasticsearch([ELASTIC_URL], timeout=30)
    s = Search(using=es, index=WORKS_INDEX).query("term", **{key: id})
    response = s.execute()
    return response.hits.total.value


def works_count_from_elastic(key, id):
    redis = Redis.from_url(REDIS_URL)
    cache_key = f"{key}_{id}_works_count"

    # try to retrieve the cached value
    cached_citation_count = redis.get(cache_key)
    if cached_citation_count is not None:
        cached_works_count = cached_citation_count.decode("utf-8")
        return int(float(cached_works_count))

    # if not cached, compute the value
    works_count = fetch_works_count(key, id)

    # cache the newly computed value
    redis.set(cache_key, works_count, ex=cache_expiration())
    return int(works_count)
