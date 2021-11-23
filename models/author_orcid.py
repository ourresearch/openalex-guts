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
            return self.orcid
        return self.orcid

    def __repr__(self):
        return "<AuthorOrcid ( {} ) {}>".format(self.author_id, self.orcid)

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

# then cleanup from pubmed
# update mid.author_orcid set orcid=replace(orcid, 's', '') where orcid ~ 's';
#
# update mid.author_orcid set orcid = substring(orcid, 1, 4) || '-' ||
#                 substring(orcid, 5, 4) || '-' ||
#                 substring(orcid, 9, 4) || '-' ||
#                 substring(orcid, 13, 4)
# from mid.author_orcid where not orcid ~ '-' ;

# update mid.zz_explore_author_orcid set orcid_given=orcid_names.given_names, orcid_family=orcid_names.family_name,
# orcid_match_name=f_matching_author_string(orcid_names.given_names + ' ' + orcid_names.family_name)
# from mid.zz_explore_author_orcid t1
# join ins.orcid_names orcid_names on orcid_names.orcid=t1.orcid
# where orcid_names.given_names is not null and orcid_names.family_name is not null


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
# #
# insert into mid.zz_explore_author_orcid (
# select distinct mag_auth.author_id, crossref.orcid, sysdate, 'crossref', crossref.doi, crossref.given, crossref.family,
# f_matching_author_string(crossref.given + ' ' + crossref.family)
# from mag_main_paper_author_affiliations mag_affil
# join mag_main_papers mag on mag_affil.paper_id=mag.paper_id
# join mag_main_authors mag_auth on mag_affil.author_id=mag_auth.author_id
# join crossref_main_author crossref on crossref.doi=mag.doi_lower
# where
# mag_affil.original_author ilike substring(crossref.given, 1, 2) + '% ' + crossref.family -- a space before and nothing after
# and crossref.orcid is not null
# )
#
#
# insert into mid.zz_explore_author_orcid  (
# select distinct
# mag_auth.author_id, pubmed.orcid, sysdate, 'pubmed', mag.doi_lower, pubmed.given, pubmed.family,
# f_matching_author_string(pubmed.given + ' ' + pubmed.family)
# from mag_main_paper_author_affiliations mag_affil
# join mag_main_papers mag on mag_affil.paper_id=mag.paper_id
# join mag_main_authors mag_auth on mag_affil.author_id=mag_auth.author_id
# join pubmed_main_author pubmed on pubmed.doi=mag.doi_lower
# where
# mag_affil.original_author ilike substring(pubmed.given, 1, 2) + '% ' + pubmed.family -- a space before and nothing after
# and pubmed.orcid is not null
# )
#
# update mid.zz_explore_author_orcid set publisher=t2.publisher
# from mid.zz_explore_author_orcid t1
# join mid.work work on work.doi=t1.evidence_doi
# join mid.journal t2 on t2.journal_id=work.journal_id
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


