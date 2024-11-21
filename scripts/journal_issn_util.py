from argparse import ArgumentParser

from sqlalchemy import text

from .add_things_queue import enqueue_jobs

from app import db
from models import Source

def enqueue_issn_works_to_slow_queue(issn):
    query = text("""
        SELECT work_id 
        FROM ins.recordthresher_record 
        WHERE json_to_text_array(journal_issns) @> ARRAY[:issn]::text[]
    """)

    rows = db.session.execute(query, {"issn": issn}).fetchall()
    work_ids = [work_id[0] for work_id in rows]
    chunk_size = 100
    chunk = []
    count = 0
    for i, work_id in enumerate(work_ids):
        chunk.append(work_id)
        count += 1

        if len(chunk) >= chunk_size:
            enqueue_jobs(chunk, priority=-1, fast_queue_priority=-1)
            chunk.clear()
            print(f'{count}/{len(work_ids)} works enqueued to slow queue')
    enqueue_jobs(chunk, priority=-1, fast_queue_priority=-1)
    chunk.clear()
    print(f'{count}/{len(work_ids)} works enqueued to slow queue')


def undelete_journal(source_id):
    deleted_journals = db.session.query(Source).filter(Source.display_name.ilike('%deleted%')).all()
    source = db.session.query(Source).get(source_id)
    if source is None:
        raise Exception(f'Source with ID {source_id} does not exist.')
    if source.merge_into_id not in [j.id for j in deleted_journals]:
        raise Exception(f'Journal {source} does not appear to be deleted')
    source.merge_into_id = None
    source.merge_into_date = None
    db.session.commit()
    source.store()

    run_issn_works_through_queues(source)


def run_issn_works_through_queues(source):
    for issn in source.issns_text_array:
        print(f'Enqueuing {issn} works to slow queue')
        enqueue_issn_works_to_slow_queue(issn)


def only_run_queues(source_id):
    source = db.session.query(Source).get(source_id)
    if source is None:
        raise Exception(f'Source with ID {source_id} does not exist.')
    run_issn_works_through_queues(source)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--source_id', type=int, required=True, help="The ID of the source to process.")
    parser.add_argument(
        '--only_run_queues',
        action='store_true',
        help="Flag to only run ISSN works through queues without undeleting."
    )
    parser.add_argument(
        '--undelete',
        action='store_true',
        help="Flag to explicitly undelete the source."
    )
    args = parser.parse_args()

    if args.only_run_queues and args.undelete:
        raise Exception("You cannot specify both --only_run_queues and --undelete at the same time.")
    elif args.only_run_queues:
        only_run_queues(args.source_id)
    elif args.undelete:
        undelete_journal(args.source_id)
    else:
        raise Exception("You must specify either --only_run_queues or --undelete.")