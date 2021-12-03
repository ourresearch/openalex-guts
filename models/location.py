from app import db


# truncate mid.location
# insert into mid.location (select * from legacy.mag_main_paper_urls)

# update mid.location set source_url=replace(source_url, '\ten', ''), language_code='en' where source_url ~ '\ten';
# update mid.location set source_url=replace(source_url, '\tes', ''), language_code='es' where source_url ~ '\tes';
# update mid.location set source_url=replace(source_url, '\tfr', ''), language_code='fr' where source_url ~ '\tfr';
# update mid.location set source_url=replace(source_url, '\tsv', ''), language_code='sv' where source_url ~ '\tsv';
# update mid.location set source_url=replace(source_url, '\tko', ''), language_code='ko' where source_url ~ '\tko';
# update mid.location set source_url=replace(source_url, '\tpt', ''), language_code='pt' where source_url ~ '\tpt';
# update mid.location set source_url=replace(source_url, '\tfi', ''), language_code='fi' where source_url ~ '\tfi';
# update mid.location set source_url=replace(source_url, '\t', '') where source_url ~ '\t';


class Location(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "location"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    source_url = db.Column(db.Text, primary_key=True)
    source_type = db.Column(db.Numeric)
    language_code = db.Column(db.Text)
    url_for_landing_page = db.Column(db.Text)
    url_for_pdf = db.Column(db.Text)
    host_type = db.Column(db.Text)
    version = db.Column(db.Text)
    license = db.Column(db.Text)
    repository_institution = db.Column(db.Text)
    pmh_id = db.Column(db.Text)

    @property
    def source_description(self):
        if not self.source_type:
            return "unknown"
        # from https://docs.microsoft.com/en-us/academic-services/graph/reference-data-schema#paper-urls
        lookup = {1: "Html", 2: "Text", 3: "Pdf", 4: "Doc", 5: "Ppt", 6: "Xls", 8: "Rtf", 12: "Xml", 13: "Rss", 20: "Swf", 27: "Ics", 31: "Pub", 33: "Ods", 34: "Odp", 35: "Odt", 36: "Zip", 40: "Mp3"}
        return lookup.get(self.source_type, "unknown").lower()

    def to_dict(self, return_level="full"):
        response = {
            "source_url": self.source_url,
            "source_type": self.source_type,
            "source_description": self.source_description,
            "language_code": self.language_code,
            "url_for_landing_page": self.url_for_landing_page,
            "url_for_pdf": self.url_for_pdf,
            "host_type": self.host_type,
            "version": self.version,
            "license": self.license,
            "repository_institution": self.repository_institution,
            "oai_pmh_id": self.pmh_id
        }
        return response

    def __repr__(self):
        return "<Location ( {} ) {}>".format(self.paper_id, self.source_url)


