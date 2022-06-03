from app import db, logger
from sqlalchemy import text
import tempfile
import os

SNAPSHOT_DATA_DIR = os.path.join(tempfile.TemporaryDirectory().name, 'data')

def distinct_updated_dates(table_name):
    rows = db.session.execute(
        text(f'select distinct updated::date from {table_name} where merge_into_id is null and json_save is not null')
    ).fetchall()

    return [row[0].isoformat() for row in rows]

def export_table(table_name, entity_type):
    updated_dates = distinct_updated_dates(table_name)
    base_entity_dir = os.path.join(SNAPSHOT_DATA_DIR, entity_type)
    os.makedirs(base_entity_dir)

    for updated_date in updated_dates:
        date_dir = os.path.join(SNAPSHOT_DATA_DIR, entity_type, f'updated_date={updated_date}')
        os.makedirs(date_dir)

        q = f"""
            COPY (
                select json_save from {table_name}
                where updated >= '{updated_date}' and updated < '{updated_date}'::date + '1 day'::interval
                and merge_into_id is null and json_save is not null
            ) to stdout
        """

        cursor = db.session.connection().connection.cursor()
        entity_file = os.path.join(date_dir, entity_type)

        logger.info(f'writing {updated_date} {entity_type} to {entity_file}')

        with open(entity_file, "w") as f:
            cursor.copy_expert(q, f)

        # TODO: os.system(split) the file into ~5 GB chunks and gzip them

    # TODO: build and write a manifest file in base_entity_dir. keep redshift json format but only do {"entries": [{"url": ...}]} for now

def run():
    logger.info(f'snapshot data directory is {SNAPSHOT_DATA_DIR}')
    export_table('mid.json_venues', 'venues')
    export_table('mid.json_concepts', 'concepts')
    export_table('mid.json_institutions', 'institutions')
    export_table('mid.json_authors', 'authors')
    export_table('mid.json_works', 'works')

if __name__ == "__main__":
    run()

