# from app import db
#
# # insert into institution_ror (institution_id, ror_id, evidence)
# # (select institution.affiliation_id, ror_summary_view.ror_id, 'mapped from mag grid_id' as evidence
# # from institution
# # join ror_summary_view on institution.grid_id=ror_summary_view.grid_id
# # )
#
#
# class InstitutionRor(db.Model):
#     __table_args__ = {'schema': 'mid'}
#     __tablename__ = "institution_ror"
#
#     institution_id = db.Column(db.Text, db.ForeignKey("mid.institution.affiliation_id"), primary_key=True)
#     ror_id = db.Column(db.Text, primary_key=True)
#     evidence = db.Column(db.Text)
#
#     @property
#     def ror_url(self):
#         return "https://ror.org/{}".format(self.ror_id)
#
#     def to_dict(self, return_level="full"):
#         if return_level=="full":
#             keys = [col.name for col in self.__table__.columns]
#         else:
#             keys = []
#         response = {key: getattr(self, key) for key in keys}
#         response["ror"] = [self.ror_id, self.ror_url]
#         response.update(self.ror.to_dict(return_level))
#         return response
#
#     def __repr__(self):
#         return "<InstitutionRor ( {} ) {}>".format(self.institution_id, self.ror_id)
#
