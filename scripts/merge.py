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
from merge.merge_institution import process_institution as merge_one_institution

from util import elapsed

# python -m scripts.merge source --away=2764397475 --into=190099528
# python -m scripts.merge institution --away=19744281 --into=74796645

# bulk_merge_data = [
# (182273258,103163165),
# (75340821,3017902709),
# (159272336,3018999404)
# ]

def run(entity, merge_away_id, merge_into_id):
    entity = entity.lower()

    # starting to move away from this script and into dedicated code
    if entity == "institution":
        # institution is the first entity to be moved away from this script. the others will be moved later.
        merge_one_institution(old_id=merge_away_id, merge_into_id=merge_into_id)
        return

    my_class = getattr(sys.modules["models"], entity.title())  # get entity class from models
    now = datetime.datetime.utcnow().isoformat()

    merge_away_obj = my_class.query.options(orm.Load(my_class).raiseload('*')).get(merge_away_id)
    merge_into_obj = my_class.query.options(orm.Load(my_class).raiseload('*')).get(merge_into_id)
    print(f"merging {merge_away_obj} \n away into {merge_into_obj}")

    # TODO: many of these fields are deprecated. Document this better and/or clean up the code and database tables
    # merge_into_obj.paper_count += merge_away_obj.paper_count
    # paper_family_count = merge_away_obj.paper_family_count if merge_away_obj.paper_family_count else 0
    # merge_into_obj.paper_family_count += paper_family_count # column can go away after MAG format done
    # merge_into_obj.citation_count += merge_away_obj.citation_count
    merge_into_obj.full_updated_date = now

    merge_away_obj.merge_into_id = merge_into_id
    merge_away_obj.merge_into_date = now
    merge_away_obj.updated_date = now
    merge_away_obj.paper_count = 0
    merge_away_obj.paper_family_count = 0  # column can go away after MAG format done
    merge_away_obj.citation_count = 0
    merge_away_obj.full_updated_date = now

    if entity == "source":
        work_objects = models.Work.query.options(orm.Load(models.Work).raiseload('*')
                                                 ).filter(models.Work.journal_id==merge_away_id).all()
        print(f"updating journal_id for {len(work_objects)} works")
        for work_obj in work_objects:
            work_obj.journal_id = merge_into_id
            work_obj.updated_date = now
            work_obj.full_updated_date = now
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
        merge_away_work_object = models.Work.query.get(merge_away_id)
        print(f"updating work_object {merge_away_work_object}")

        # things to clear
        merge_away_work_object.references_unmatched = []
        merge_away_work_object.concepts_full = []
        merge_away_work_object.related_works = []
        merge_away_work_object.affiliations = []

        # things to merge in if missing
        merge_into_work_object = models.Work.query.get(merge_into_id)
        print(f"updating work_object {merge_into_work_object}")
        if not merge_into_work_object.abstract:
            merge_into_work_object.abstract = merge_away_work_object.abstract
        if not merge_into_work_object.mesh:
            merge_into_work_object.mesh = merge_away_work_object.mesh

        # things to update
        objs = models.Record.query.options(orm.Load(models.Record).raiseload('*')).filter(models.Record.work_id==merge_away_id).all()
        print(f"updating paper_id for {len(objs)} Record work_id rows")
        for obj in objs:
            obj.work_id = merge_into_id

         # things to merge

        for merge_away_location in merge_away_work_object.locations:
            location_source_urls = [loc.source_url for loc in merge_into_work_object.locations]
            if merge_away_location.source_url not in location_source_urls:
                merge_into_work_object.locations += [merge_away_location]

        for merge_away_reference in merge_away_work_object.references:
            paper_reference_ids = [ref.paper_reference_id for ref in merge_into_work_object.references]
            if merge_away_reference.paper_reference_id not in paper_reference_ids:
                merge_into_work_object.references += [merge_away_reference]

        for merge_away_extra_id in merge_away_work_object.extra_ids:
            extra_id_attribute_values = [extra_id.attribute_value for extra_id in merge_into_work_object.extra_ids]
            if merge_away_extra_id.attribute_value not in extra_id_attribute_values:
                merge_into_work_object.extra_ids += [merge_away_extra_id]

        # and now merge things pointing to them

        citations_to_merge_away = models.Citation.query.options(orm.Load(models.Citation).raiseload('*')).filter(models.Citation.paper_reference_id==merge_away_id).all()
        citations_to_merge_into = models.Citation.query.options(orm.Load(models.Citation).raiseload('*')).filter(models.Citation.paper_reference_id==merge_into_id).all()
        papers_citing_merge_into = [obj.paper_id for obj in citations_to_merge_into]
        for obj in citations_to_merge_away:
            if obj.paper_id not in papers_citing_merge_into:
                obj.paper_reference_id = merge_into_id
                # need to set the full_updated_date on the paper_id work

        related_works_to_merge_away = models.WorkRelatedWork.query.options(orm.Load(models.WorkRelatedWork).raiseload('*')).filter(models.WorkRelatedWork.recommended_paper_id==merge_away_id).all()
        related_works_to_merge_into = models.WorkRelatedWork.query.options(orm.Load(models.WorkRelatedWork).raiseload('*')).filter(models.WorkRelatedWork.recommended_paper_id==merge_into_id).all()
        papers_relating_to_merge_into = [obj.paper_id for obj in related_works_to_merge_into]
        for obj in related_works_to_merge_away:
            if obj.paper_id not in papers_relating_to_merge_into:
                obj.recommended_paper_id = merge_into_id
                # need to set the full_updated_date on the paper_id work

    db.session.commit()
    print("done\n")



if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('entity', help='one of:  work, author, source, institution (concepts not merged)')
    ap.add_argument('--away', '-a', nargs='?', type=int, help='ID of entity to merge away')
    ap.add_argument('--into', '-i', nargs='?', type=int, help='ID of entity to merge into')

    parsed = ap.parse_args()
    run(parsed.entity, parsed.away, parsed.into)

    # to do one-off bulk updates, comment out the above and comment this in with appropriate edits:
    # for (away, into) in bulk_merge_data:
    #     run("institution", away, into)
