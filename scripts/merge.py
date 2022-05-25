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

# bulk_merge_data = [
# (182273258,103163165),
# (75340821,3017902709),
# (159272336,3018999404)
# ]

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
            if affil_obj.work:
                affil_obj.work.full_updated_date = now

        author_objects = models.Author.query.options(
                orm.Load(models.Author).raiseload('*')
                ).filter(models.Author.last_known_affiliation_id==merge_away_id).all()
        print(f"updating affiliation_id for {len(author_objects)} last known affiliation in authors")
        for author_obj in author_objects:
            author_obj.last_known_affiliation_id = merge_into_id
            author_obj.updated = now
            author_obj.full_updated_date = now
    elif entity == "author":
        affiliation_objects = models.Affiliation.query.options(selectinload(models.Affiliation.work).raiseload('*'),
                                                               selectinload(models.Affiliation.author).raiseload('*'),
                                                               orm.Load(models.Affiliation).raiseload('*')
                                                               ).filter(models.Affiliation.author_id==merge_away_id).all()
        print(f"updating author_id for {len(affiliation_objects)} works")
        for affil_obj in affiliation_objects:
            affil_obj.author_id = merge_into_id
            affil_obj.updated_date = now
            if affil_obj.work:
                affil_obj.work.full_updated_date = now
            if affil_obj.author:
                affil_obj.author.full_updated_date = now
    elif entity == "work":
        objs = models.Record.query.options(orm.Load(models.Record).raiseload('*')).filter(models.Record.work_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} Record work_id rows")
        for obj in objs:
            obj.work_id = merge_into_id

        objs = models.Abstract.query.options(orm.Load(models.Abstract).raiseload('*')).filter(models.Abstract.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} Abstract rows")
        for obj in objs:
            obj.paper_id = merge_into_id

        objs = models.Mesh.query.options(orm.Load(models.Mesh).raiseload('*')).filter(models.Mesh.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} Mesh rows")
        for obj in objs:
            obj.paper_id = merge_into_id

        objs = models.Citation.query.options(orm.Load(models.Citation).raiseload('*')).filter(models.Citation.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} Citation paper_id rows")
        for obj in objs:
            obj.paper_id = merge_into_id

        objs = models.Citation.query.options(orm.Load(models.Citation).raiseload('*')).filter(models.Citation.paper_reference_id==merge_away_id).all()
        print(f"updating paper_reference_id for {len(objs)} Citation paper_reference_id rows")
        for obj in objs:
            obj.paper_reference_id = merge_into_id
            # need to set the full_updated_date on this entity

        objs = models.Location.query.options(orm.Load(models.Location).raiseload('*')).filter(models.Location.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} Location rows")
        for obj in objs:
            obj.paper_id = merge_into_id

        objs = models.WorkExtraIds.query.options(orm.Load(models.WorkExtraIds).raiseload('*')).filter(models.WorkExtraIds.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} WorkExtraIds rows")
        for obj in objs:
            obj.paper_id = merge_into_id

        objs = models.WorkConceptFull.query.options(orm.Load(models.WorkConceptFull).raiseload('*')).filter(models.WorkConceptFull.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} WorkConceptFull rows")
        for obj in objs:
            obj.paper_id = merge_into_id

        objs = models.WorkRelatedWork.query.options(orm.Load(models.WorkRelatedWork).raiseload('*')).filter(models.WorkRelatedWork.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} WorkRelatedWork paper_id rows")
        for obj in objs:
            obj.paper_id = merge_into_id

        objs = models.WorkRelatedWork.query.options(orm.Load(models.WorkRelatedWork).raiseload('*')).filter(models.WorkRelatedWork.recommended_paper_id.merge_away_id).all()
        print(f"updating paper_id for {len(objs)} WorkRelatedWork recommended_paper_id rows")
        for obj in objs:
            obj.recommended_paper_id = merge_into_id
            # need to set the full_updated_date on this entity

        affiliation_objects = models.Affiliation.query.options(orm.Load(models.Affiliation).raiseload('*')).filter(models.Affiliation.paper_id==merge_away_id).all()
        print(f"updating paper_id for {len(affiliation_objects)} works")
        for affil_obj in affiliation_objects:
            affil_obj.paper_id = merge_into_id
            affil_obj.updated_date = now



    db.session.commit()
    print("done\n")



if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('entity', help='one of:  work, author, venue, institution (concepts not merged)')
    ap.add_argument('--away', '-a', nargs='?', type=int, help='ID of entity to merge away')
    ap.add_argument('--into', '-i', nargs='?', type=int, help='ID of entity to merge into')

    parsed = ap.parse_args()
    run(parsed.entity, parsed.away, parsed.into)

    # to do one-off bulk updates, comment out the above and comment this in with appropriate edits:
    # for (away, into) in bulk_merge_data:
    #     run("institution", away, into)
