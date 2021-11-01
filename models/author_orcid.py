from app import db


class AuthorOrcid(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "author_orcid"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    orcid = db.Column(db.Text, primary_key=True) # shouldn't have more than one but might?
    evidence = db.Column(db.Text)

    @property
    def orcid_url(self):
        if not self.orcid:
            return None
        return "https://orcid.org/{}".format(self.orcid)

    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
            return {key: getattr(self, key) for key in keys}
        return [self.orcid, self.orcid_url]

    def __repr__(self):
        return "<AuthorOrcid ( {} ) {}>".format(self.paper_id, self.orcid)

# INSERTED THIS TO SEED IT
# CROSSREF
# insert into mid.author_orcid (
# select distinct mag_auth.author_id, crossref.orcid
# from mag_main_paper_author_affiliations mag_affil
# join mag_main_papers mag on mag_affil.paper_id=mag.paper_id
# join mag_main_authors mag_auth on mag_affil.author_id=mag_auth.author_id
# join crossref_main_author crossref on crossref.doi=mag.doi_lower
# where
# mag_affil.original_author ilike substring(crossref.given, 1, 2) + '% ' + crossref.family -- a space before and nothing after
# and crossref.orcid is not null
# )

# PUBMED
# insert into mid.author_orcid (author_id, orcid) (
# select distinct mag_auth.author_id, pubmed.orcid
# from mag_main_paper_author_affiliations mag_affil
# join mag_main_papers mag on mag_affil.paper_id=mag.paper_id
# join mag_main_authors mag_auth on mag_affil.author_id=mag_auth.author_id
# join pubmed_main_author pubmed on pubmed.doi=mag.doi_lower
# where
# mag_affil.original_author ilike substring(pubmed.given, 1, 2) + '% ' + pubmed.family -- a space before and nothing after
# and pubmed.orcid is not null
# )


# can explore more with these
#
# select mag_auth.display_name, given, family, TRUE, mag_auth.*, crossref.*, mag_affil.*, mag.*
# from mag_main_paper_author_affiliations mag_affil
# join mag_main_papers mag on mag_affil.paper_id=mag.paper_id
# join mag_main_authors mag_auth on mag_affil.author_id=mag_auth.author_id
# join crossref_main_author crossref on crossref.doi=mag.doi_lower
# where
# mag_affil.original_author ilike substring(crossref.given, 1, 2) + '% ' + crossref.family -- a space before and nothing after
# and crossref.orcid is not null
# order by random()
# limit 1000
#
#
#
# --select count(distinct crossref.orcid)
# select count(distinct doi_lower)
# --select count(*)
# from mag_main_paper_author_affiliations mag_affil
# join mag_main_papers mag on mag_affil.paper_id=mag.paper_id
# join mag_main_authors mag_auth on mag_affil.author_id=mag_auth.author_id
# join crossref_main_author crossref on crossref.doi=mag.doi_lower
# where
# mag_affil.original_author ilike substring(crossref.given, 1, 2) + '% ' + crossref.family -- a space before and nothing after
# and crossref.orcid is not null


