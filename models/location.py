from app import db


# truncate mid.location
# insert into mid.location (select * from legacy.mag_main_paper_urls)

class Location(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "location"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    source_url = db.Column(db.Text, primary_key=True)
    source_type = db.Column(db.Numeric)
    language_code = db.Column(db.Text)

    @property
    def source_description(self):
        if not self.source_type:
            return "unknown"
        # from https://docs.microsoft.com/en-us/academic-services/graph/reference-data-schema#paper-urls
        lookup = {1: "Html", 2: "Text", 3: "Pdf", 4: "Doc", 5: "Ppt", 6: "Xls", 8: "Rtf", 12: "Xml", 13: "Rss", 20: "Swf", 27: "Ics", 31: "Pub", 33: "Ods", 34: "Odp", 35: "Odt", 36: "Zip", 40: "Mp3"}
        return lookup.get(self.source_type, "unknown").lower()

    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
            return {key: getattr(self, key) for key in keys}
        else:
            return [self.source_description, self.source_url]

    def __repr__(self):
        return "<Location ( {} ) {}>".format(self.paper_id, self.source_url)


