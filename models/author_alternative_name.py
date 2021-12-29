from cached_property import cached_property
from app import db

class AuthorAlternativeName(db.Model):
    __table_args__ = {'schema': 'legacy'}
    __tablename__ = "mag_main_author_extended_attributes"

    author_id = db.Column(db.BigInteger, db.ForeignKey("mid.author.author_id"), primary_key=True)
    attribute_value = db.Column(db.BigInteger, primary_key=True)

    @cached_property
    def display_name(self):
        return self.attribute_value

    def __repr__(self):
        return "<AuthorAlternativeName ( {} ) {}>".format(self.author_id, self.attribute_value)
