# coding: utf-8

# old
#
# from sqlalchemy.dialects.postgresql import JSONB
# from sqlalchemy.sql import text
# from executor import execute
# import requests
# from time import time
# from time import sleep
# import datetime
# import shortuuid
# from urllib import quote
# import os
# import re
# import simplejson as json
# from dateutil import parser
# from pprint import pprint
#
# from app import logger
# from app import db
# from util import safe_commit
#
# # from https://stackoverflow.com/a/39293287/596939
# import sys
# reload(sys)
# if sys.version_info.major < 3:
#     sys.setdefaultencoding('utf8')
#
#
# class Ror(db.Model):
#     __tablename__ = 'ror'
#     __bind_key__ = "redshift_db"
#
#     ror_id = db.Column(db.Text, primary_key=True)
#     name = db.Column(db.Text)
#     grid_id = db.Column(db.Text)
#     country = db.Column(db.Text)
#     country_code = db.Column(db.Text)
#     link = db.Column(db.Text)
#     types = db.Column(db.Text)
#     api_raw = db.Column(db.Text)
#     updated = db.Column(db.DateTime)
#
#     def __init__(self, **kwargs):
#         self.updated = datetime.datetime.utcnow().isoformat()
#         super(Ror, self).__init__(**kwargs)
#
#     def __repr__(self):
#         return u"{} ({}) {}".format(self.__class__.__name__, self.ror_id, self.name)
#
# def loop_through_lines():
#     temp_data_filename = u"/Users/hpiwowar/Downloads/2021-09-23-ror-data.json"
#     with open(temp_data_filename,'rB') as f:
#         lines = f.read()
#     data = json.loads(lines)
#     print len(data)
#     ror_objs = []
#     for my_dict in data:
#         ror_id = my_dict["id"].replace(u"https://ror.org/", u"")
#         # print "before get"
#         my_ror = Ror.query.get(ror_id)
#         # print "after get", my_ror
#         if my_ror:
#             print ".",
#         else:
#             my_ror = Ror()
#             links = my_dict["links"]
#             if links:
#                 link = links[0]
#             else:
#                 link = None
#             my_ror.ror_id = ror_id
#             my_ror.name = my_dict["name"].encode("utf-8", "ignore").decode("utf-8")
#             my_ror.grid_id = my_dict["external_ids"]["GRID"]["preferred"]
#             my_ror.country = my_dict["country"]["country_name"]
#             my_ror.country_code = my_dict["country"]["country_code"].lower()
#             my_ror.link = link
#             my_ror.types = json.dumps(my_dict["types"])
#             my_ror.api_raw = json.dumps(my_dict)
#             # print my_ror
#             ror_objs.append(my_ror)
#             print "adding", my_ror
#             db.session.add(my_ror)
#             if len(ror_objs) > 10:
#                 print "committing"
#                 safe_commit(db)
#                 ror_objs = []
#     print "committing"
#     safe_commit(db)
#     # pprint(data[0:2])
#
#
#
# if __name__ == "__main__":
#
#     loop_through_lines()
#


