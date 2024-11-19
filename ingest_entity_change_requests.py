import json
import os
from datetime import datetime
import argparse

import gspread
import heroku3
import pandas as pd
import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
from psycopg2 import IntegrityError
from sqlalchemy.orm import sessionmaker

from app import db, unpaywall_db_engine
from models.source import Source
from models.work import Work
from scripts.works_query import base_fast_queue_works_query
from scripts.unpaywall_recordthresher_refresh import refresh_single as unpaywall_recordthresher_refresh
from scripts.add_things_queue import enqueue_job as enqueue_slow_queue

WORK_COLUMN_MAP = {
    'Timestamp': 'timestamp',
    'Email Address': 'email',
    'Is this your work?': 'is_representative',
    'What\'s the main OpenAlex Work ID? (how to find) \n\n👉  Paste a single Work ID, like "W2884670852"': 'work_id',
    'What edit is needed to the work record?': 'edit_type',
    'What should be the title for this work?\n\n👉 Enter title below as it should be displayed.': 'title',
    'What should the primary language of this work be?\n\n👉 Enter the 2-letter code for the language from this chart. E.g., German should be "de"\n\n(more information on work language)': 'language',
    'What should be the Source of this work?\n👉 enter the OpenAlex Source ID for the Source,  e.g., "S4121844" for the Journal of Applied Phycology.\n\n(how do i find the OpenAlex Source ID)': 'source_id',
    'Which of these types best describes the work?\n(more information on work types)': 'work_type',
    'Is this work open access?\n\n👉 select \'true\' or open access and \'false\' for closed': 'is_oa',
    'Where can we find the fulltext or pdf?\n\n👉 paste the URL that links to the open version of the fulltext or pdf': 'fulltext_url',
    'How is the work licensed?\n\n👉 Select the licence type from the list below.\n': 'license',
    'What is the Work ID of the record that appears to be the main record (i.e., has more metadata available).\n\n👉 Paste a single Work ID, like "W2884670852"\n\n(how to find a work ID)': 'merge_into',
    'Which Work ID(s) are duplicates of the main record?\n\n👉 Paste a single Work ID, like "W2884670852", or multiple Work IDs separated by commas, like "W2884670852,W2317271409"\n\n(how to find a work ID)': 'merge_duplicates',
    'Finally, which OA color should this work be?': 'oa_status'
}

SOURCE_COLUMN_MAP = {
    'Timestamp': 'timestamp',
    'Email Address': 'email',
    'Do you represent this source?': 'is_representative',
    'What\'s the main OpenAlex Source ID? (how to find) \n\n👉  Paste a single source ID, like "S140251998"': 'source_id',
    'What edit is needed to the source profile?': 'edit_type',
    'What should be the primary display name for the source profile?\n\n👉 Paste the name as it should be displayed.': 'display_name',
    'What profile do you want to merge into yours?\n\n👉 Paste a single OpenAlex source ID, like "S4210206229" or multiple OpenAlex source IDs separated by comma, like "S4210206229,S2737589143"': 'merge_ids',
    'Is the source 100% Open Access?\n\n👉 if yes, mark "true"; if no, mark "false"': 'is_open_access',
    'Is the source indexed in DOAJ?\n\n👉 if yes, mark "true"; if no, mark "false"': 'in_doaj',
    'What should the APC List Price for this journal be?\n\n👉 enter APC List Price in USD below': 'apc_price',
    'Which Type best describes this source?\n\n👉 select one below (more information on source type)': 'source_type',
    'What is the OpenAlex Publisher ID for the organization hosting this source?\n\n👉 Paste a single OpenAlex Publisher ID below, like P4310320595 (how do i find the Publisher ID?)': 'publisher_id',
    'What changes are needed to the ISSN numbers for this source?': 'issn_changes',
    'Which ISSN number(s) should be removed from this Source?\n\n👉 Paste a single ISSN number below (e.g., 0031-8884) or multiple ISSN numbers separated by commas (e.g., 0022-3646,1529-8817)': 'issn_remove',
    'Which ISSN number(s) should be added to this Source?\n\n👉 Paste a single ISSN number below (e.g., 0031-8884) or multiple ISSN numbers separated by commas (e.g., 0022-3646,1529-8817)': 'issn_add',
    'What should be the homepage URL for this source?\n\n👉 Paste a single URL that directs to the homepage of this source': 'homepage_url'
}


class GoogleSheetsClient:
    def __init__(self, creds_env_var='GOOGLE_SHEETS_CREDS_JSON'):
        self.credentials = self._get_credentials(creds_env_var)
        self.client = gspread.authorize(self.credentials)
        self.drive_service = build('drive', 'v3', credentials=self.credentials)

    def _get_credentials(self, creds_env_var):
        json_creds = os.getenv(creds_env_var)
        if not json_creds:
            raise ValueError(f"Environment variable {creds_env_var} not found")

        json_creds = json_creds.replace('\\\n', '\\n')
        creds_dict = json.loads(json_creds)

        scopes = [
            'https://www.googleapis.com/auth/drive.readonly',
            'https://www.googleapis.com/auth/spreadsheets.readonly'
        ]

        return service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=scopes
        )

    def get_sheet_by_name(self, name_contains):
        results = self.drive_service.files().list(pageSize=10).execute()
        items = results.get('files', [])

        for item in items:
            if name_contains in item['name']:
                return item
        return None

    def read_sheet_to_df(self, sheet_id, column_map, worksheet_name=None):
        sheet = self.client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(
            worksheet_name) if worksheet_name else sheet.get_worksheet(0)
        data = worksheet.get_all_values()

        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data[1:], columns=data[0])
        df = df.rename(columns=column_map)
        return df


class EntityHandler:

    HEROKU_API_KEY = os.environ.get("HEROKU_API_KEY")

    def __init__(self, oax_db_session):
        self.oax_db_session = oax_db_session
        self.heroku_conn = heroku3.from_key(self.HEROKU_API_KEY)
        self.changes_made = False

    def log_change(self, field_name: str, before, after):
        if before == after:
            print(f"✓ {field_name} already set to: {before}")
            return False
        print(f"{field_name}: {before} -> {after}")
        return True

    def process_rows(self, df):
        raise NotImplementedError("Subclasses must implement process_rows")


class SourceHandler(EntityHandler):
    def change_display_name(self, source: Source,
                            new_display_name: str) -> None:
        if not new_display_name:
            return
        if self.log_change("display_name", source.display_name,
                           new_display_name):
            source.display_name = new_display_name
            source.updated_date = datetime.now()
            self.changes_made = True

    def change_oa_status(self, source: Source, is_oa: str) -> None:
        if not is_oa:
            return
        new_oa = is_oa.upper() == 'TRUE'
        if self.log_change("is_oa", source.is_oa, new_oa):
            source.is_oa = new_oa
            source.updated_date = datetime.now()
            self.changes_made = True

    def change_doaj_status_handler(self, source: Source, in_doaj: str) -> None:
        if not in_doaj:
            return
        new_doaj = in_doaj.upper() == 'TRUE'
        if self.log_change("is_in_doaj", source.is_in_doaj, new_doaj):
            source.is_in_doaj = new_doaj
            source.updated_date = datetime.now()
            self.changes_made = True

    def change_apc_handler(self, source: Source, apc_price: str) -> None:
        if not apc_price:
            return
        try:
            new_price = int(float(apc_price))
            if self.log_change("apc_usd", source.apc_usd, new_price):
                source.apc_usd = new_price
                source.apc_found = True
                source.updated_date = datetime.now()
                self.changes_made = True
                self.log_change("apc_found", source.apc_found, True)
        except ValueError:
            print(f"Invalid APC price format: {apc_price}")

    def change_homepage_url_handler(self, source: Source,
                                    homepage_url: str) -> None:
        if not homepage_url:
            return
        if self.log_change("webpage", source.webpage, homepage_url):
            source.webpage = homepage_url
            source.updated_date = datetime.now()
            self.changes_made = True

    def change_issn_handler(self, source: Source, issn_remove: str,
                            issn_add: str) -> None:
        if not (issn_remove or issn_add):
            return

        current_issns = set(source.issns_text_array or [])
        new_issns = current_issns.copy()

        if issn_remove:
            remove_issns = {issn.strip() for issn in issn_remove.split(',')}
            new_issns = new_issns - remove_issns

        if issn_add:
            add_issns = {issn.strip() for issn in issn_add.split(',')}
            new_issns = new_issns | add_issns

        if self.log_change("issns", sorted(list(current_issns)),
                           sorted(list(new_issns))):
            source.issns_text_array = list(new_issns)
            source.issns = json.dumps(list(new_issns))
            source.updated_date = datetime.now()
            self.changes_made = True

    def merge_sources_handler(self, source: Source, merge_ids: str) -> None:
        if not merge_ids:
            return

        merge_id_list = [s.strip() for s in merge_ids.split(',')]
        for merge_id in merge_id_list:
            if not merge_id.lower().startswith('s'):
                continue
            internal_id = int(merge_id[1:])
            if self.log_change("merge_into_id", source.merge_into_id,
                               internal_id):
                source.merge_into_id = internal_id
                source.merge_into_date = datetime.now()
                source.updated_date = datetime.now()
                self.changes_made = True
                self.log_change("merge_into_date", source.merge_into_date,
                                datetime.now())

    def fast_store_source(self, source_id ):
        app = self.heroku_conn.apps()["openalex-guts"]
        command = f"python -m scripts.fast_queue --entity=source --method=store --id={source_id}"
        app.run_command(command, printout=False)

    def process_rows(self, df):
        for row in df.to_dict('records'):
            self.changes_made = False

            if not row['source_id'].lower().startswith('s'):
                print(f"Invalid source ID format: {row['source_id']}")
                continue

            internal_id = int(row['source_id'][1:])
            source = self.oax_db_session.query(Source).filter_by(
                journal_id=internal_id).first()

            if not source:
                print(f"Source not found: {row['source_id']}")
                continue

            print(
                f"\nProcessing source {row['source_id']} - {row['edit_type']}")
            try:
                edit_type = row['edit_type'].lower()

                if 'display name' in edit_type:
                    self.change_display_name(source, row['display_name'])
                elif 'oa status' in edit_type:
                    self.change_oa_status(source, row['is_open_access'])
                elif 'doaj' in edit_type:
                    self.change_doaj_status_handler(source, row['in_doaj'])
                elif 'apc' in edit_type:
                    self.change_apc_handler(source, row['apc_price'])
                elif 'homepage' in edit_type:
                    self.change_homepage_url_handler(source,
                                                     row['homepage_url'])
                elif 'issn' in edit_type:
                    self.change_issn_handler(source, row['issn_remove'],
                                             row['issn_add'])
                elif 'merge' in edit_type:
                    self.merge_sources_handler(source, row['merge_ids'])
                else:
                    print(f"Unknown edit type: {edit_type}")

                if self.changes_made:
                    self.oax_db_session.commit()
                    self.fast_store_source(source.id)
                    print("✓ Changes committed successfully")
                else:
                    print("✓ No changes needed")
            except Exception as e:
                print(f"Error processing source {row['source_id']}: {str(e)}")
                self.oax_db_session.rollback()


class WorkHandler(EntityHandler):

    def __init__(self, oax_db_session, unpaywall_db_session):
        self.unpaywall_db_session = unpaywall_db_session
        super().__init__(oax_db_session)

    def refresh_in_unpaywall(self, doi):
        app = self.heroku_conn.apps()["oadoi"]
        command = f"python queue_pub.py --method=refresh --id='{doi}'"
        app.run_command(command, printout=False)

    def update_in_unpaywall(self, doi):
        app = self.heroku_conn.apps()["oadoi"]
        command = f"python queue_pub.py --method=update --id='{doi}'"
        app.run_command(command, printout=False)

    @staticmethod
    def get_unpaywall_response(doi):
        r = requests.get(f'https://api.unpaywall.org/v2/{doi}?email=team@ourresearch.org')
        r.raise_for_status()
        return r.json()

    def manual_close_in_unpaywall(self, doi):
        try:
            self.unpaywall_db_session.execute(
                'INSERT INTO oa_manual (doi, response_jsonb) VALUES (:doi, :response_jsonb)',
                {'doi': doi.lower(), 'response_jsonb': '{}'})
            self.unpaywall_db_session.commit()
        except IntegrityError as e:
            if e.args[0].startswith('duplicate key value'):
                print(f"WARNING: Duplicate DOI {doi} detected. Skipping oa_manual insertion.")
            else:
                raise

    def manual_open_in_openalex(self, work_id, fulltext_url, oa_status=None):
        existing_record = self.oax_db_session.execute(
            '''
            SELECT id FROM ins.recordthresher_record
            WHERE work_id = :work_id AND record_type = 'override'
            ''',
            {"work_id": work_id}
        ).fetchone()

        if existing_record:
            recordthresher_id = existing_record[0]
            self.oax_db_session.execute(
                '''
                UPDATE ins.recordthresher_record
                SET is_oa = TRUE, work_pdf_url = :fulltext_url, is_work_pdf_url_free_to_read = TRUE
                WHERE id = :record_id
                ''',
                {"fulltext_url": fulltext_url, "record_id": recordthresher_id}
            )
        else:
            result = self.oax_db_session.execute(
                '''
                INSERT INTO ins.recordthresher_record (id, work_id, record_type, is_oa, work_pdf_url)
                VALUES (make_recordthresher_id(), :work_id, 'override', TRUE, :fulltext_url)
                RETURNING id
                ''',
                {"work_id": work_id, "fulltext_url": fulltext_url}
            )
            recordthresher_id = result.fetchone()[0]

        if oa_status:
            self.oax_db_session.execute(
                '''
                INSERT INTO ins.oa_status_manual (recordthresher_id, oa_status)
                VALUES (:id, :oa_status)
                ''',
                {"id": recordthresher_id, "oa_status": oa_status}
            )

        self.oax_db_session.commit()

    def manual_close_in_openalex(self, work_id):
        existing_record = self.oax_db_session.execute(
            '''
            SELECT id FROM ins.recordthresher_record
            WHERE work_id = :work_id AND record_type = 'override'
            ''',
            {"work_id": work_id}
        ).fetchone()

        if existing_record:
            self.oax_db_session.execute(
                '''
                UPDATE ins.recordthresher_record
                SET is_oa = FALSE
                WHERE id = :record_id
                ''',
                {"record_id": existing_record[0]}
            )
        else:
            self.oax_db_session.execute(
                '''
                INSERT INTO ins.recordthresher_record (id, work_id, record_type, is_oa)
                VALUES (make_recordthresher_id(), :work_id, 'override', FALSE)
                ''',
                {"work_id": work_id}
            )

        self.oax_db_session.commit()

    @staticmethod
    def _get_oa_status(oa_status_str):
        statuses = {'green', 'gold', 'bronze', 'hybrid', 'diamond'}
        for status in statuses:
            if status in oa_status_str.lower():
                return status
        return 'bronze'

    def change_title(self, work: Work, new_title: str) -> None:
        if not new_title:
            return
        if self.log_change("paper_title", work.paper_title, new_title):
            work.paper_title = new_title
            work.updated_date = datetime.now()
            self.changes_made = True

    def change_oa_status(self, work: Work, is_oa: str, url: str = '', oa_status: str ='') -> None:
        if not is_oa and not url:
            return
        new_status = 'open' if is_oa.upper() == 'TRUE' or url else 'closed'
        if new_status == 'closed':
            self.manual_close_in_unpaywall(work.doi)
            self.manual_close_in_openalex(work.id)
        else:
            # should be open
            if not work.doi:
                if not url:
                    raise ValueError(
                        'URL must be provided to manually open a work')
                self.manual_open_in_openalex(work.id, url, oa_status)
            else:
                upw_response = self.get_unpaywall_response(work.doi)
                if upw_response['is_oa']:
                    print(f'Work {work.doi} is already oa in Unpaywall')
                else:
                    self.refresh_in_unpaywall(work.doi.lower())
                    upw_response = self.get_unpaywall_response(work.doi)
                    if upw_response['is_oa']:
                        print(f'Refresh successfully opened {work.doi} in Unpaywall')
                    else:
                        if not url:
                            raise ValueError('URL must be provided to manually open a work')
                        response_jsonb_override = {"pdf_url": url}
                        if oa_status:
                            response_jsonb_override['oa_status_set'] = self._get_oa_status(oa_status)
                        self.unpaywall_db_session.execute('INSERT INTO oa_manual (doi, response_jsonb) VALUES (:doi, :response_jsonb)', {'doi': work.doi.lower(), 'response_jsonb': json.dumps(response_jsonb_override)})
                        self.unpaywall_db_session.commit()
                self.update_in_unpaywall(work.doi.lower())
                unpaywall_recordthresher_refresh(work.doi.lower())
        work.updated_date = datetime.now()
        self.changes_made = True

    def change_language(self, work: Work, language: str) -> None:
        if not language:
            return
        new_language = language.lower()
        if self.log_change("language", work.language, new_language):
            work.language = new_language
            work.updated_date = datetime.now()
            self.changes_made = True

    def change_license(self, work: Work, license: str) -> None:
        if not license:
            return
        if self.log_change("license", work.license, license):
            work.license = license
            work.updated_date = datetime.now()
            self.changes_made = True

    def change_source(self, work: Work, source_id: str) -> None:
        if not source_id or not source_id.lower().startswith('s'):
            return
        new_journal_id = int(source_id[1:])
        if self.log_change("journal_id", work.journal_id, new_journal_id):
            work.journal_id = new_journal_id
            work.updated_date = datetime.now()
            self.changes_made = True

    def merge_works(self, work: Work, merge_into: str, merge_duplicates: str) -> None:
        if merge_into and merge_into.lower().startswith('w'):
            new_merge_id = int(merge_into[1:])
            if self.log_change("merge_into_id", work.merge_into_id, new_merge_id):
                work.merge_into_id = new_merge_id
                work.merge_into_date = datetime.now()
                self.changes_made = True
                self.log_change("merge_into_date", work.merge_into_date, datetime.now())

    def process_rows(self, df):
        for row in df.to_dict('records'):
            self.changes_made = False

            work_id = row['work_id']
            # Handle different work ID formats
            if 'openalex.org/' in work_id:
                work_id = work_id.split('/')[-1]

            if not work_id.lower().startswith('w'):
                print(f"Invalid work ID format: {work_id}")
                continue

            internal_id = int(work_id[1:])
            work = base_fast_queue_works_query().filter_by(paper_id=internal_id).first()

            if not work:
                print(f"Work not found: {work_id}")
                continue

            print(f"\nProcessing work {work_id} - {row['edit_type']}")
            try:
                edit_type = row['edit_type'].lower()

                if 'title' in edit_type:
                    self.change_title(work, row['title'])
                # Do not do oa status edit type for now, the form only specifies open/closed, we need to know if green, bronze, etc
                elif 'oa status' in edit_type:
                    self.change_oa_status(work, row['is_oa'], row['fulltext_url'], row['oa_status'])
                elif 'language' in edit_type:
                    self.change_language(work, row['language'])
                elif 'source' in edit_type:
                    self.change_source(work, row['source_id'])
                elif 'license' in edit_type:
                    self.change_license(work, row['license'])
                elif 'merge' in edit_type:
                    self.merge_works(work, row['merge_into'], row['merge_duplicates'])
                else:
                    print(f"Unknown edit type: {edit_type}")

                if self.changes_made:
                    self.oax_db_session.commit()
                    enqueue_slow_queue(work.id, priority=-1,
                                       fast_queue_priority=-1)
                    print("✓ Changes committed successfully")
                else:
                    print("✓ No changes needed")

            except Exception as e:
                print(f"Error processing work {work_id}: {str(e)}")
                self.oax_db_session.rollback()


def main():
    parser = argparse.ArgumentParser(
        description='Process OpenAlex entity changes')
    parser.add_argument('--entity', type=str, required=True,
                        choices=['works', 'sources'],
                        help='Entity type to process (works or sources)')
    args = parser.parse_args()

    sheets_client = GoogleSheetsClient()

    if args.entity == 'works':
        sheet = sheets_client.get_sheet_by_name('OpenAlex work record')
        column_map = WORK_COLUMN_MAP
        upw_session  = sessionmaker(bind=unpaywall_db_engine)
        handler = WorkHandler(db.session, upw_session())
    else:  # sources
        sheet = sheets_client.get_sheet_by_name('OpenAlex Source Profile')
        column_map = SOURCE_COLUMN_MAP
        handler = SourceHandler(db.session)

    if not sheet:
        print(f"Change requests sheet not found for {args.entity}")
        return

    df = sheets_client.read_sheet_to_df(sheet['id'], column_map)
    handler.process_rows(df)
    db.session.close()


if __name__ == "__main__":
    main()