import json
import os
from datetime import datetime
from typing import Dict, Any
import argparse

import gspread
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build

from app import db
from models.source import Source
from models.work import Work
from scripts.works_query import base_fast_queue_works_query

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
    'Where can we find the fulltext or pdf?\n\n👉 paste the URL that links to the open version of the fulltext or pdf': 'best_free_url',
    'How is the work licensed?\n\n👉 Select the licence type from the list below.\n': 'license',
    'What is the Work ID of the record that appears to be the main record (i.e., has more metadata available).\n\n👉 Paste a single Work ID, like "W2884670852"\n\n(how to find a work ID)': 'merge_into',
    'Which Work ID(s) are duplicates of the main record?\n\n👉 Paste a single Work ID, like "W2884670852", or multiple Work IDs separated by commas, like "W2884670852,W2317271409"\n\n(how to find a work ID)': 'merge_duplicates'
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
    def __init__(self, session):
        self.session = session

    def log_change(self, field_name: str, before, after):
        print(f"{field_name}: {before} -> {after}")

    def process_rows(self, df):
        raise NotImplementedError("Subclasses must implement process_rows")


class SourceHandler(EntityHandler):
    def change_display_name(self, source: Source,
                            new_display_name: str) -> None:
        if not new_display_name:
            return
        old_name = source.display_name
        source.display_name = new_display_name
        source.updated_date = datetime.now()
        self.log_change("display_name", old_name, new_display_name)

    def change_oa_status(self, source: Source, is_oa: str) -> None:
        if not is_oa:
            return
        old_oa = source.is_oa
        source.is_oa = is_oa.upper() == 'TRUE'
        source.updated_date = datetime.now()
        self.log_change("is_oa", old_oa, source.is_oa)

    def change_doaj_status_handler(self, source: Source, in_doaj: str) -> None:
        if not in_doaj:
            return
        old_doaj = source.is_in_doaj
        source.is_in_doaj = in_doaj.upper() == 'TRUE'
        source.updated_date = datetime.now()
        self.log_change("is_in_doaj", old_doaj, source.is_in_doaj)

    def change_apc_handler(self, source: Source, apc_price: str) -> None:
        if not apc_price:
            return
        try:
            old_price = source.apc_usd
            old_found = source.apc_found
            price = int(
                float(apc_price))  # handle both string integers and floats
            source.apc_usd = price
            source.apc_found = True
            source.updated_date = datetime.now()
            self.log_change("apc_usd", old_price, price)
            self.log_change("apc_found", old_found, source.apc_found)
        except ValueError:
            print(f"Invalid APC price format: {apc_price}")

    def change_homepage_url_handler(self, source: Source, homepage_url: str) -> None:
        if not homepage_url:
            return
        old_url = source.webpage
        source.webpage = homepage_url
        source.updated_date = datetime.now()
        self.log_change("webpage", old_url, homepage_url)

    def change_issn_handler(self, source: Source, issn_remove: str,
                            issn_add: str) -> None:
        if not (issn_remove or issn_add):
            return

        old_issns = set(source.issns_text_array or [])
        current_issns = old_issns.copy()

        if issn_remove:
            remove_issns = {issn.strip() for issn in issn_remove.split(',')}
            current_issns = current_issns - remove_issns

        if issn_add:
            add_issns = {issn.strip() for issn in issn_add.split(',')}
            current_issns = current_issns | add_issns

        source.issns_text_array = list(current_issns)
        source.issns = json.dumps(list(current_issns))
        source.updated_date = datetime.now()
        self.log_change("issns", sorted(list(old_issns)),
                   sorted(list(current_issns)))

    def merge_sources_handler(self, source: Source, merge_ids: str) -> None:
        if not merge_ids:
            return

        old_merge_id = source.merge_into_id
        old_merge_date = source.merge_into_date

        merge_id_list = [s.strip() for s in merge_ids.split(',')]
        for merge_id in merge_id_list:
            if not merge_id.lower().startswith('s'):
                continue
            # Convert OpenAlex ID to internal ID format
            internal_id = int(merge_id[1:])
            source.merge_into_id = internal_id
            source.merge_into_date = datetime.now()
        source.updated_date = datetime.now()
        self.log_change("merge_into_id", old_merge_id, source.merge_into_id)
        self.log_change("merge_into_date", old_merge_date, source.merge_into_date)

    def process_rows(self, df):
        for row in df.to_dict('records'):
            if not row['source_id'].lower().startswith('s'):
                print(f"Invalid source ID format: {row['source_id']}")
                continue

            internal_id = int(row['source_id'][1:])
            source = self.session.query(Source).filter_by(
                journal_id=internal_id).first()

            if not source:
                print(f"Source not found: {row['source_id']}")
                continue

            print(
                f"\nProcessing source {row['source_id']} - {row['edit_type']}")
            self._process_single_row(source, row)


class WorkHandler(EntityHandler):
    def change_title(self, work: Work, new_title: str) -> None:
        if not new_title:
            return
        old_title = work.paper_title
        work.paper_title = new_title
        work.updated_date = datetime.now()
        self.log_change("paper_title", old_title, new_title)

    def change_oa_status(self, work: Work, is_oa: str) -> None:
        if not is_oa:
            return
        old_status = work.oa_status
        work.oa_status = 'open' if is_oa.upper() == 'TRUE' else 'closed'
        work.updated_date = datetime.now()
        self.log_change("oa_status", old_status, work.oa_status)

    def change_language(self, work: Work, language: str) -> None:
        if not language:
            return
        old_language = work.language
        work.language = language.lower()
        work.updated_date = datetime.now()
        self.log_change("language", old_language, work.language)

    def change_free_url(self, work: Work, url: str) -> None:
        if not url:
            return
        old_url = work.best_free_url
        work.best_free_url = url
        work.updated_date = datetime.now()
        self.log_change("best_free_url", old_url, url)

    def change_license(self, work: Work, license: str) -> None:
        if not license:
            return
        old_license = work.license
        work.license = license
        work.updated_date = datetime.now()
        self.log_change("license", old_license, license)

    def change_source(self, work: Work, source_id: str) -> None:
        if not source_id or not source_id.lower().startswith('s'):
            return
        old_journal_id = work.journal_id
        new_journal_id = int(source_id[1:])
        work.journal_id = new_journal_id
        work.updated_date = datetime.now()
        self.log_change("journal_id", old_journal_id, new_journal_id)

    def merge_works(self, work: Work, merge_into: str,
                    merge_duplicates: str) -> None:
        if merge_into and merge_into.lower().startswith('w'):
            old_merge_id = work.merge_into_id
            new_merge_id = int(merge_into[1:])
            work.merge_into_id = new_merge_id
            work.merge_into_date = datetime.now()
            self.log_change("merge_into_id", old_merge_id, new_merge_id)

    def process_rows(self, df):
        for row in df.to_dict('records'):
            work_id = row['work_id']
            # Handle different work ID formats
            if 'openalex.org/' in work_id:
                work_id = work_id.split('/')[-1]

            if not work_id.lower().startswith('w'):
                print(f"Invalid work ID format: {work_id}")
                continue

            internal_id = int(work_id[1:])
            work = base_fast_queue_works_query().filter_by(
                paper_id=internal_id).first()

            if not work:
                print(f"Work not found: {work_id}")
                continue

            print(f"\nProcessing work {work_id} - {row['edit_type']}")
            try:
                edit_type = row['edit_type'].lower()

                if 'title' in edit_type:
                    self.change_title(work, row['title'])
                # Do not do oa status edit type for now, the form only specifies open/closed, we need to know if green, bronze, etc
                # elif 'oa status' in edit_type:
                #     self.change_oa_status(work, row['is_oa'])
                elif 'language' in edit_type:
                    self.change_language(work, row['language'])
                elif 'source' in edit_type:
                    self.change_source(work, row['source_id'])
                elif 'free url' in edit_type or 'fulltext' in edit_type:
                    self.change_free_url(work, row['best_free_url'])
                elif 'license' in edit_type:
                    self.change_license(work, row['license'])
                elif 'merge' in edit_type:
                    self.merge_works(work, row['merge_into'],
                                     row['merge_duplicates'])
                else:
                    print(f"Unknown edit type: {edit_type}")

                self.session.commit()
                print("✓ Changes committed successfully")
            except Exception as e:
                print(f"Error processing work {work_id}: {str(e)}")
                self.session.rollback()


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
        handler = WorkHandler(db.session)
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