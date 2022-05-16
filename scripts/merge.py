import argparse
from sqlalchemy import orm
from os import getenv
from urllib.parse import urlparse
from time import time
import datetime

import models
from app import db

from util import elapsed

# python -m scripts.merge venue --away=2764397475 --into=190099528

def run(parsed_args):
    if parsed_args.entity == "venue":
        my_class = models.Venue

    merge_away_id = parsed_args.away
    merge_into_id = parsed_args.into
    now = datetime.datetime.utcnow().isoformat()

    merge_away_obj = my_class.query.options(orm.Load(my_class).raiseload('*')).get(merge_away_id)
    merge_into_obj = my_class.query.options(orm.Load(my_class).raiseload('*')).get(merge_into_id)
    print(f"merging {merge_away_obj} \n away into {merge_into_obj}")

    merge_into_obj.paper_count += merge_away_obj.paper_count
    merge_into_obj.paper_family_count += merge_away_obj.paper_family_count # column can go away after MAG format done
    merge_into_obj.citation_count += merge_away_obj.citation_count
    merge_into_obj.full_updated_date = now

    merge_away_obj.merge_into_id = merge_into_id
    merge_away_obj.merge_into_date = now
    merge_away_obj.updated_date = now
    merge_away_obj.paper_count = 0
    merge_away_obj.paper_family_count = 0  # column can go away after MAG format done
    merge_away_obj.citation_count = 0
    merge_away_obj.full_updated_date = now

    if parsed_args.entity == "venue":
        work_objects = models.Work.query.options(orm.Load(models.Work).raiseload('*')).filter(models.Work.journal_id==merge_away_id).all()
        print(f"updating journal_id for {len(work_objects)} works")
        for work_obj in work_objects:
            work_obj.journal_id = merge_into_id
            work_obj.updated_date = now
            work_obj.full_updated_date = now

    db.session.commit()
    print("done\n")



if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('entity', help='one of:  work, author, venue, institution (concepts not merged)')
    ap.add_argument('--away', '-a', nargs='?', type=int, help='ID of entity to merge away')
    ap.add_argument('--into', '-i', nargs='?', type=int, help='ID of entity to merge into')

    parsed = ap.parse_args()
    run(parsed)
