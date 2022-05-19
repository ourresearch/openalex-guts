import argparse
from sqlalchemy import orm
from sqlalchemy.orm import selectinload
from os import getenv
from urllib.parse import urlparse
from time import time
import datetime
import sys

import models
from app import db

from util import elapsed

# python -m scripts.merge venue --away=2764397475 --into=190099528
# python -m scripts.merge institution --away=19744281 --into=74796645

bulk_merge_data = [
(182273258,103163165),
(75340821,3017902709),
(178089637,39727005),
(879789424,126596746),
(202438577,923382979),
(1288600564,184220567),
(1308403363,2738703131),
(119233137,3019271933),
(78570951,56590836),
(161076350,64295750),
(1325051341,1313323035),
(212013683,39727005),
(239527004,3019092743),
(129409704,150589677),
(1291253399,57053284),
(1306642263,8659980),
(70542479,142108993),
(105196157,51153154),
(179814638,3045169105),
(28239286,3019848993),
(1328046515,1342911587),
(1289912481,2799693246),
(85065924,174947986),
(865419266,99464096),
(179038810,9217761),
(14937891,153535764),
(50069286,907500627),
(1295755447,1336856363),
(19744281,74796645),
(56657469,126193024),
(193849324,99434035),
(145527848,245794714),
(174786783,1323121030),
(8821215,126193024),
(3017917047,3412056),
(70544011,74760111),
(203408516,95023434),
(3123023596,56067802),
(153219969,335685885),
(134909763,2800233406),
(102159200,3017928408),
(1310196399,866009140),
(159272336,3018999404)
]

def run(entity, merge_away_id, merge_into_id):
    entity = entity.lower()
    my_class = getattr(sys.modules["models"], entity.title())  # get entity class from models
    now = datetime.datetime.utcnow().isoformat()

    merge_away_obj = my_class.query.options(orm.Load(my_class).raiseload('*')).get(merge_away_id)
    merge_into_obj = my_class.query.options(orm.Load(my_class).raiseload('*')).get(merge_into_id)
    print(f"merging {merge_away_obj} \n away into {merge_into_obj}")

    merge_into_obj.paper_count += merge_away_obj.paper_count
    paper_family_count = merge_away_obj.paper_family_count if merge_away_obj.paper_family_count else 0
    merge_into_obj.paper_family_count += paper_family_count # column can go away after MAG format done
    merge_into_obj.citation_count += merge_away_obj.citation_count
    merge_into_obj.full_updated_date = now

    merge_away_obj.merge_into_id = merge_into_id
    merge_away_obj.merge_into_date = now
    merge_away_obj.updated_date = now
    merge_away_obj.paper_count = 0
    merge_away_obj.paper_family_count = 0  # column can go away after MAG format done
    merge_away_obj.citation_count = 0
    merge_away_obj.full_updated_date = now

    if entity == "venue":
        work_objects = models.Work.query.options(orm.Load(models.Work).raiseload('*')
                                                 ).filter(models.Work.journal_id==merge_away_id).all()
        print(f"updating journal_id for {len(work_objects)} works")
        for work_obj in work_objects:
            work_obj.journal_id = merge_into_id
            work_obj.updated_date = now
            work_obj.full_updated_date = now
    elif entity == "institution":
        affiliation_objects = models.Affiliation.query.options(selectinload(models.Affiliation.work).raiseload('*'),
                                                               orm.Load(models.Affiliation).raiseload('*')
                                                               ).filter(models.Affiliation.affiliation_id==merge_away_id).all()
        print(f"updating affiliation_id for {len(affiliation_objects)} works")
        for affil_obj in affiliation_objects:
            affil_obj.affiliation_id = merge_into_id
            affil_obj.updated_date = now
            affil_obj.work.full_updated_date = now

        author_objects = models.Author.query.options(
                orm.Load(models.Author).raiseload('*')
                ).filter(models.Author.last_known_affiliation_id==merge_away_id).all()
        print(f"updating affiliation_id for {len(author_objects)} last known affiliation in authors")
        for author_obj in author_objects:
            author_obj.last_known_affiliation_id = merge_into_id
            author_obj.updated = now
            author_obj.full_updated_date = now

    db.session.commit()
    print("done\n")



if __name__ == '__main__':
    # ap = argparse.ArgumentParser()
    # ap.add_argument('entity', help='one of:  work, author, venue, institution (concepts not merged)')
    # ap.add_argument('--away', '-a', nargs='?', type=int, help='ID of entity to merge away')
    # ap.add_argument('--into', '-i', nargs='?', type=int, help='ID of entity to merge into')
    #
    # parsed = ap.parse_args()
    # run(parsed.entity, parsed.away, parsed.into)

    for (away, into) in bulk_merge_data:
        run("institution", away, into)
