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
    def is_oa(self):
        if self.version != None:
            return True
        return None

    @property
    def source_description(self):
        # from https://docs.microsoft.com/en-us/academic-services/graph/reference-data-schema#paper-urls
        lookup = {1: "Html", 2: "Text", 3: "Pdf", 4: "Doc", 5: "Ppt", 6: "Xls", 8: "Rtf", 12: "Xml", 13: "Rss", 20: "Swf", 27: "Ics", 31: "Pub", 33: "Ods", 34: "Odp", 35: "Odt", 36: "Zip", 40: "Mp3"}
        if not self.source_type:
            if self.url_for_pdf:
                return lookup[4]
            return "unknown"
        return lookup.get(self.source_type, "unknown").lower()

    @property
    def display_license(self):
        if not self.license:
            return None
        return self.license.lower().split(":", 1)[0]

    @property
    def display_host_type(self):
        if self.host_type == "publisher":
            return "journal"
        return self.host_type


    @property
    def score(self):
        score = 0
        if self.version == "publishedVersion":
            score += 10000
        if self.version == "acceptedVersion":
            score += 5000
        if self.version == "submittedVersion":
            score += 1000
        if self.license:
            score += (500 - len(self.license))  # shorter is better, fewer restrictions
        if self.host_type == "publisher":
            score += 200
        if "doi.org/" in self.source_url:
            score += 100
        if ("pdf" in self.source_url) or (self.url_for_pdf is not None):
            score += 50
        if ("europepmc" in self.source_url) or ("ncbi.nlm.nih.gov" in self.source_url):
            score += 25
        return score

    @property
    def include_in_alternative(self):
        if self.is_oa:
            return True
        if self.host_type == "publisher":
            return True
        if self.source_url and "doi" in self.source_url:
            if self.version:
                return True  # else is probably a component or stub record
        return False

    def to_dict(self, return_level="full"):
        id = None
        display_name = self.repository_institution

        if self.host_type == "publisher":
            if self.work.journal:
                id = self.work.journal.openalex_id
                display_name = self.work.journal.display_name
        response = {
            "id": id,
            "display_name": display_name,
            "type": self.display_host_type,
            "url": self.source_url,
            "is_oa": self.is_oa,
            "version": self.version,
            "license": self.display_license,
            # "repository_institution": self.repository_institution,
        }
        # if return_level == "full":
        #     response.update({
        #         "url_for_landing_page": self.url_for_landing_page,
        #         "url_for_pdf": self.url_for_pdf,
        #         "url_type": self.source_description,
        #         "host_type": self.display_host_type,
        #         "oai_pmh_id": self.pmh_id
        #     })
        return response

    def __repr__(self):
        return "<Location ( {} ) {}>".format(self.paper_id, self.source_url)


