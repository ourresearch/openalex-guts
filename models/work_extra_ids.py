from app import db


class WorkExtraIds(db.Model):
    __table_args__ = {'schema': 'mid'}
    __tablename__ = "work_extra_ids"

    paper_id = db.Column(db.BigInteger, db.ForeignKey("mid.work.paper_id"), primary_key=True)
    attribute_type = db.Column(db.Numeric)
    attribute_value = db.Column(db.Text)

    @property
    def id_type(self):
        if not self.attribute_type:
            return None
        # from https://docs.microsoft.com/en-us/academic-services/graph/reference-data-schema#paper-extended-attributes
        lookup = {1: "PatentId", 2: "pmid", 3: "pmcid", 4: "Alternative Title"}
        return lookup[self.attribute_type]

    @property
    def url(self):
        if self.id_type == "doi":
            return "https://doi.org/{}".format(self.attribute_value)
        if self.id_type == "pmid":
            return "https://pubmed.ncbi.nlm.nih.gov/{}".format(self.attribute_value)
        if self.id_type == "pmcid":
            return "https://www.ncbi.nlm.nih.gov/pmc/articles/{}".format(self.attribute_value)
        return None

    def to_dict(self, return_level="full"):
        if return_level=="full":
            keys = [col.name for col in self.__table__.columns]
            return {key: getattr(self, key) for key in keys}
        return [self.attribute_value, self.url]

    def __repr__(self):
        return "<WorkExtraIds ( {} ) {} {}>".format(self.paper_id, self.id_type, self.attribute_value)
