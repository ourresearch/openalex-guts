import argparse
import boto3
import json
import os
import time
import requests
import psycopg2
import gspread
from google.oauth2 import service_account
from multiprocessing import Pool
from sqlalchemy import create_engine
from app import logger
from datetime import datetime
import scripts.works_magnet_auto_approver as auto_approver

# load config vars
AWS_ACCESS_KEY_ID = os.getenv('AWS_SAGEMAKER_ACCOUNT_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SAGEMAKER_ACCOUNT_SECRET')
GCLOUD_AUTHOR_CURATION_CREDS = os.getenv('GCLOUD_AUTHOR_CURATION')
GITHUB_PAT = os.getenv('GITHUB_PAT')
did_not_get_added = []
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_PAT}",
    "X-GitHub-Api-Version": "2022-11-28"
}

# define the scope
scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds_dict = json.loads(GCLOUD_AUTHOR_CURATION_CREDS.replace('\\\n', '\\n'))

# add credentials to the account
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scope)

# authorize the clientsheet
client = gspread.authorize(creds)


def get_secret(secret_name="prod/psqldb/conn_string"):
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except:
        logger.info("Can't get secret")

    # Decrypts secret using the associated KMS key.
    secret_string = get_secret_value_response['SecretString']

    secret = json.loads(secret_string)
    return secret


def connect_to_db():
    secret = get_secret()
    conn = psycopg2.connect(
        host=secret['host'],
        port=secret['port'],
        user=secret['username'],
        password=secret['password'],
        database=secret['dbname']
    )
    return conn


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--method', '-m', action='store', type=str, required=True,
                        help='actions to perform', choices=['load_latest_issues', 'full'])
    parser.add_argument('--limit', '-l', action='store', type=int, default=None,
                        help='limit the number of issues to process (e.g., --limit 10 for next 10 issues)')
    return parser.parse_args()


def parse_body(body, fields):
    if not body or not isinstance(body, str):
        return {}
    result = {}
    for line in body.split('\n'):
        for field in fields:
            if field + ':' in line:
                result[field] = line.replace(field + ':', '').strip()
    return result


def get_open_github_issues(limit=None):
    fields = ['raw_affiliation_name', 'new_rors', 'previous_rors', 'works_examples', 'body']

    issues = []
    url = 'https://api.github.com/repos/dataesr/openalex-affiliations/issues?per_page=100'
    page_count = 1

    # Track if we've reached our limit
    issues_needed = limit if limit else float('inf')

    while url and len(issues) < issues_needed:
        logger.info(f"Getting page {page_count} of issues")
        res = requests.get(url, headers=headers)

        # Check if the response is valid
        if res.status_code != 200:
            logger.error(f"Error fetching issues: {res.status_code} - {res.text}")
            break

        try:
            current_issues = res.json()
            # Verify this is a list of issues
            if not isinstance(current_issues, list):
                logger.error(f"Unexpected response format on page {page_count}: {current_issues}")
                break

            if len(current_issues):
                # Only add as many issues as we need to reach the limit
                if limit:
                    remaining = issues_needed - len(issues)
                    issues += current_issues[:remaining]
                else:
                    issues += current_issues

                # If we've reached our limit, break out of the loop
                if limit and len(issues) >= limit:
                    logger.info(f"Reached limit of {limit} issues, stopping fetch")
                    break
            else:
                break

            # Look for pagination link in headers
            if 'Link' in res.headers:
                links = {}
                for link in res.headers['Link'].split(','):
                    url_part, rel_part = link.split(';')
                    url_part = url_part.strip('<>')
                    rel = rel_part.split('=')[1].strip('"')
                    links[rel] = url_part

                url = links.get('next', None)
            else:
                url = None

            page_count += 1

        except Exception as e:
            logger.error(f"Error parsing JSON response: {e}")
            break

    logger.info(f"{len(issues)} open issues have been retrieved from github")

    valid_issues = []
    for issue in issues:
        try:
            if 'body' in issue and issue['body']:
                parsed = parse_body(issue['body'], fields)
                for key, value in parsed.items():
                    issue[key] = value
                issue_id = issue['url'].split('/')[-1]
                issue['issue_id'] = int(issue_id)
                # Only include fields we need
                filtered_issue = {}
                for field in fields + ['issue_id']:
                    filtered_issue[field] = issue.get(field, '')
                valid_issues.append(filtered_issue)
            else:
                logger.warning(f"Issue missing body: {issue.get('url', 'Unknown URL')}")
        except Exception as e:
            logger.warning(f"Error processing issue: {e}")

    logger.info(f"{len(valid_issues)} valid issues processed")

    # Sort by issue_id
    valid_issues.sort(key=lambda x: x['issue_id'])

    return valid_issues


def process_github_issues(issues):
    """Add derived fields to the issues"""
    processed_issues = []

    for issue in issues:
        processed = issue.copy()

        # Split RORs
        new_rors = [r for r in processed.get('new_rors', '').split(';') if r != '']
        prev_rors = [r for r in processed.get('previous_rors', '').split(';') if r != '']

        # Calculate added/removed RORs
        added = [r for r in new_rors if r not in prev_rors]
        processed['has_added'] = len(added) > 0
        processed['added_rors'] = ';'.join(added)

        removed = [r for r in prev_rors if r not in new_rors]
        processed['has_removed'] = len(removed) > 0
        processed['removed_rors'] = ';'.join(removed)

        # Process contact information
        body = processed.get('body', '')
        contact_line = next((line for line in body.split('\n') if line.startswith('contact')), '')
        processed['contact_full'] = contact_line

        contact_parts = contact_line.split('@')
        processed['contact'] = contact_parts[0] if len(contact_parts) > 0 else ""
        processed['contact_domain'] = contact_parts[1] if len(contact_parts) > 1 else ""

        # Add empty fields
        processed['OpenAlex'] = ""
        processed['Notes'] = ""
        processed['Notes1'] = ""
        processed['Notes2'] = ""
        processed['Notes3'] = ""

        processed_issues.append(processed)

    return processed_issues


def load_latest_github_issues(sheet_instance, max_issue_id_from_previous, last_row_index, limit=None):
    # Get issues and process them
    issues = get_open_github_issues(limit)
    processed_issues = process_github_issues(issues)

    # Filter to only new issues
    new_issues = [issue for issue in processed_issues
                  if issue['issue_id'] > max_issue_id_from_previous]

    if not new_issues:
        logger.info("No new issues to load")
        return processed_issues

    # Sort by issue_id
    new_issues.sort(key=lambda x: x['issue_id'])

    # Prepare data for spreadsheet update
    sheet_data = []
    for issue in new_issues:
        row = [
            issue['issue_id'],
            issue['has_added'],
            issue['has_removed'],
            issue['new_rors'],
            issue['previous_rors'],
            issue['raw_affiliation_name'],
            issue['added_rors'],
            issue['removed_rors'],
            issue['works_examples'],
            issue['contact'],
            issue['contact_domain']
        ]
        sheet_data.append(row)

    # Get current spreadsheet size and ensure we have enough rows
    try:
        # Get all values from column A which holds issue_number
        issue_numbers_column = sheet_instance.col_values(1)  # Assuming issue_number is in column A
        non_empty_rows = [i for i, val in enumerate(issue_numbers_column, start=1) if val.strip().isdigit()]
        if non_empty_rows:
            last_issue_row = max(non_empty_rows)
        else:
            last_issue_row = 1

        start_row = last_issue_row + 1
        logger.info(f"Determined last row with issue_number: {last_issue_row}")
        logger.info(f"Will append {len(sheet_data)} rows starting at row {start_row}")

        for i, row_data in enumerate(sheet_data):
            row_num = start_row + i
            row_range = f"A{row_num}:K{row_num}"  # Assuming columns A through K
            sheet_instance.update(row_range, [row_data])
            # Still use a delay to avoid rate limits
            time.sleep(1)

            # Log progress every few rows
            if (i + 1) % 5 == 0 or i == len(sheet_data) - 1:
                logger.info(f"Updated {i + 1}/{len(sheet_data)} rows")

    except Exception as e:
        logger.error(f"Error updating spreadsheet: {e}")

    logger.info(f"Loaded {len(new_issues)} github issues to the spreadsheet")

    return processed_issues


def get_id_from_ror(rors_string, ror_to_aff_id):
    """Convert ROR IDs to affiliation IDs"""
    if not rors_string:
        return []

    if isinstance(rors_string, str):
        return [ror_to_aff_id.get(x, "") for x in rors_string.strip().split(";") if x]
    elif isinstance(rors_string, list):
        return rors_string
    else:
        return []


def get_list_of_works(works_string):
    """Convert works string to list of work IDs"""
    if not works_string:
        return []

    if isinstance(works_string, str):
        return [int(x) for x in works_string.strip()[1:].split(";W") if x]
    elif isinstance(works_string, list):
        return works_string
    else:
        return []


def turn_ror_back_to_none(ror_list):
    """Clean up ROR list, returning None if empty"""
    if not ror_list:
        return None

    filtered = [x for x in ror_list if x != '']
    if not filtered:
        return None

    return filtered


def get_string_nan(value):
    """Handle NaN values in strings"""
    if isinstance(value, float) and (value != value):  # NaN check
        return 'NaN'
    return value


def check_aff_ids(old_affs, new_affs):
    """Compare old and new affiliation IDs"""
    if isinstance(old_affs, list):
        if isinstance(new_affs, list):
            return set(old_affs) == set(new_affs)
        return False
    else:
        return not isinstance(new_affs, list)


def auto_approve_requests(sheet_instance, records_data):
    # Get ROR to affiliation ID mapping
    secret = get_secret()
    engine = create_engine(
        f"postgresql://awsuser:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}"
    )

    with engine.connect() as conn:
        # Get institution data
        result = conn.execute(
            "SELECT affiliation_id, ror_id, display_name FROM mid.institution WHERE merge_into_id IS NULL"
        )

        # Build mapping
        institutions = result.fetchall()
        ror_to_aff_id = {row[1]: row[0] for row in institutions if row[1]}

    # Create filtered records list with only needed columns
    issues_and_approve = []
    seen_issue_numbers = set()

    for record in records_data:
        issue_number = record.get('issue_number')
        # Skip duplicates
        if issue_number in seen_issue_numbers:
            continue

        seen_issue_numbers.add(issue_number)
        issues_and_approve.append({
            'issue_number': issue_number,
            'raw_affiliation_name': record.get('raw_affiliation_name', ''),
            'added_rors': record.get('added_rors', ''),
            'removed_rors': record.get('removed_rors', ''),
            'OpenAlex Approve?': record.get('OpenAlex Approve?', ''),
            'Notes2': record.get('Notes2', '')
        })

    # Filter to issues needing approval
    issues_needing_approval = [
        issue for issue in issues_and_approve
        if issue['OpenAlex Approve?'] == ''
    ]

    num_issues_to_approve = len(issues_needing_approval)
    logger.info(f"Found {num_issues_to_approve} issues to auto-approve")

    if num_issues_to_approve == 0:
        return

    # Process ROR IDs for each issue
    for issue in issues_needing_approval:
        issue['added_rors'] = get_id_from_ror(issue['added_rors'], ror_to_aff_id)
        issue['removed_rors'] = get_id_from_ror(issue['removed_rors'], ror_to_aff_id)
        issue['added_rors'] = turn_ror_back_to_none(issue['added_rors'])
        issue['removed_rors'] = turn_ror_back_to_none(issue['removed_rors'])

    # Prepare arguments for multiprocessing
    approval_args = [
        (issue['raw_affiliation_name'],
         issue['added_rors'],
         issue['removed_rors'],
         issue['issue_number'])
        for issue in issues_needing_approval
    ]

    # Process approvals in parallel
    with Pool(4) as p:
        all_arrays = p.starmap(
            func=auto_approver.approve_works_magnet_request,
            iterable=approval_args
        )

    # Process results
    approval_results = []
    for result in all_arrays:
        approval_results.append({
            'OpenAlex Approve?': result[0],
            'Notes2': result[1],
            'issue_number': result[2],
            'original_affiliation': result[3]
        })

    # Count results
    num_approved = sum(1 for r in approval_results if r['OpenAlex Approve?'] == 'Yes')
    num_need_human = sum(1 for r in approval_results if r['OpenAlex Approve?'] == '')
    num_failed = sum(1 for r in approval_results if r['OpenAlex Approve?'] == 'No')
    num_rejected = sum(1 for r in approval_results if r['OpenAlex Approve?'] == 'No - auto')

    logger.info(f"Auto-approved {num_approved} issues")
    logger.info(f"Auto-rejected {num_rejected} issues")
    logger.info(f"Failed to auto-approve {num_failed} issues")
    logger.info(f"Need human approval for {num_need_human} issues")

    # Remove duplicates
    unique_results = {}
    for result in approval_results:
        unique_results[result['issue_number']] = result

    # Update original data with approval results
    issue_number_to_result = {r['issue_number']: r for r in unique_results.values()}

    for issue in issues_and_approve:
        issue_number = issue['issue_number']
        if issue_number in issue_number_to_result:
            issue['OpenAlex Approve?'] = issue_number_to_result[issue_number]['OpenAlex Approve?']
            issue['Notes2'] = issue_number_to_result[issue_number]['Notes2']

    # Sort by issue number
    final_approval_list = sorted(issues_and_approve, key=lambda x: x['issue_number'])

    # Update the spreadsheet
    if len(issues_and_approve) == len(final_approval_list):
        logger.info("Updating the spreadsheet with the auto-approval results")

        # Update "OpenAlex Approve?" column
        approve_column = [[issue['OpenAlex Approve?']] for issue in final_approval_list]
        sheet_instance.update(approve_column, 'L2')

        # Update "Notes2" column
        notes_column = [[issue['Notes2']] for issue in final_approval_list]
        sheet_instance.update(notes_column, 'O2')


def load_latest_approvals_to_db(records_data, open_issues):
    # Create mapping from issue_id to raw_affiliation_name
    issue_id_to_raw_name = {
        issue['issue_id']: issue['raw_affiliation_name']
        for issue in open_issues
    }

    # Process sheet data
    google_sheet_data = []
    for record in records_data:
        issue_number = record.get('issue_number')

        # Skip issues without raw affiliation name
        if issue_number not in issue_id_to_raw_name:
            continue

        google_sheet_data.append({
            'issue_number': issue_number,
            'previous_rors': record.get('previous_rors', ''),
            'new_rors': record.get('new_rors', ''),
            'added_rors': record.get('added_rors', ''),
            'removed_rors': record.get('removed_rors', ''),
            'works_examples': record.get('works_examples', ''),
            'OpenAlex Approve?': record.get('OpenAlex Approve?', ''),
            'contact_domain': record.get('contact_domain', ''),
            'original_affiliation': issue_id_to_raw_name[issue_number]
        })

    # Get institution mapping
    secret = get_secret()
    engine = create_engine(
        f"postgresql://awsuser:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}"
    )

    with engine.connect() as conn:
        # Get institution data
        result = conn.execute(
            "SELECT affiliation_id, ror_id, display_name FROM mid.institution WHERE merge_into_id IS NULL"
        )

        # Build mapping
        institutions = result.fetchall()
        ror_to_aff_id = {row[1]: row[0] for row in institutions if row[1]}

    # Process data
    for record in google_sheet_data:
        record['added_rors'] = get_id_from_ror(record['added_rors'], ror_to_aff_id)
        record['removed_rors'] = get_id_from_ror(record['removed_rors'], ror_to_aff_id)
        record['works_examples'] = get_list_of_works(record['works_examples'])
        record['works_examples_len'] = len(record['works_examples'])
        record['added_rors_len'] = len(record['added_rors'])
        record['removed_rors_len'] = len(record['removed_rors'])
        record['added_rors'] = turn_ror_back_to_none(record['added_rors'])
        record['removed_rors'] = turn_ror_back_to_none(record['removed_rors'])
        record['contact_domain'] = get_string_nan(record['contact_domain'])
        record['OpenAlex Approve?'] = record['OpenAlex Approve?'] in ['Yes', 'yes', True]

    # Filter records for curation
    final_records_for_curation = []

    for record in google_sheet_data:
        if (record['OpenAlex Approve?'] and
                record['works_examples_len'] > 0 and
                (record['added_rors_len'] > 0 or record['removed_rors_len'] > 0)):

            # Create a record for each work example
            for work_id in record['works_examples']:
                final_records_for_curation.append({
                    'github_issue_number': record['issue_number'],
                    'affiliation_ids_add': record['added_rors'],
                    'affiliation_ids_remove': record['removed_rors'],
                    'work_id': work_id,
                    'openalex_approve': record['OpenAlex Approve?'],
                    'contact_domain': record['contact_domain'],
                    'original_affiliation': record['original_affiliation']
                })

    if not final_records_for_curation:
        logger.info("No new approvals to load to the database")
        return

    logger.info(f"Loading {len(final_records_for_curation)} new approvals to the database")

    # Get existing curation records
    with engine.connect() as conn:
        result = conn.execute(
            "SELECT * FROM authorships.work_specific_affiliation_string_curation"
        )
        current_curation_table = result.fetchall()

    # Create mapping for existing records
    existing_records = {}
    for row in current_curation_table:
        key = (row.github_issue_number, row.work_id)
        existing_records[key] = {
            'github_issue_number': row.github_issue_number,
            'work_id': row.work_id,
            'original_affiliation': row.original_affiliation,
            'affiliation_ids_add': row.affiliation_ids_add,
            'affiliation_ids_remove': row.affiliation_ids_remove,
            'contact_domain': row.contact_domain,
            'openalex_approve': row.openalex_approve,
            'create_date': row.create_date
        }

    # Find records to update and new records
    new_rows = []
    for record in final_records_for_curation:
        key = (record['github_issue_number'], record['work_id'])

        if key in existing_records:
            # This would handle updates, but we're skipping this as mentioned in original code
            # "this script is not set up for table updates"
            continue
        else:
            # This is a new record
            record['create_date'] = datetime.today()
            record['update_date'] = datetime.today()
            new_rows.append(record)

    logger.info(f"{len(new_rows)} new rows to insert into the database")

    if not new_rows:
        logger.info("No new rows to insert into the database")
        return

    # Insert new records
    conn = connect_to_db()
    cur = conn.cursor()

    for insert_row in new_rows:
        insert_into_curation_table(cur, conn, insert_row)

    logger.info(
        f"The following issues did not get added to the database because of an error: {', '.join([str(x) for x in did_not_get_added])}")

    # Update add_most_things table
    for insert_row in new_rows:
        if insert_row['github_issue_number'] not in did_not_get_added:
            insert_into_add_most_things(cur, conn, insert_row['work_id'])

    cur.close()
    conn.close()

    # Close GitHub issues
    curr_date = datetime.now().strftime("%Y-%m-%d")

    # Get list of issues to close
    final_issues_to_add = []
    seen_issues = set()

    for row in new_rows:
        if (row['openalex_approve'] and
                row['github_issue_number'] not in did_not_get_added and
                row['github_issue_number'] not in seen_issues):
            final_issues_to_add.append(row['github_issue_number'])
            seen_issues.add(row['github_issue_number'])

    # Close GitHub issues
    did_not_finish = False
    for i, github_issue_number in enumerate(final_issues_to_add):
        time.sleep(2)  # sleep to avoid rate limit on GitHub
        logger.info(f"row {str(i)}: closing issue {str(github_issue_number)}")

        # Comment on the issue
        data = {
            'body': f'This issue was accepted and ingested by the OpenAlex team on {curr_date}. The new affiliations should be visible within the next 7 days.'}
        url = f"https://api.github.com/repos/dataesr/openalex-affiliations/issues/{github_issue_number}/comments"
        comments = requests.get(url=url, headers=headers).json()

        if not comments:
            resp = requests.post(url=url, data=json.dumps(data), headers=headers)
            if resp.status_code != 201:
                time.sleep(60)
                resp = requests.post(url=url, data=json.dumps(data), headers=headers)
                if resp.status_code != 201:
                    logger.info(f"Error while commenting on issue {str(github_issue_number)} (second try)")
                    logger.info(
                        f"Exiting GitHub approval process. The following issues need to be closed but were skipped: {', '.join([str(x) for x in final_issues_to_add[i:]])}")
                    did_not_finish = True
                    break

        # Close the ticket
        data = {'state_reason': 'completed', 'state': 'closed'}
        url = f"https://api.github.com/repos/dataesr/openalex-affiliations/issues/{github_issue_number}"
        resp = requests.post(url=url, data=json.dumps(data), headers=headers)

    if not did_not_finish:
        logger.info("All new approvals have been loaded to the database")


def insert_into_add_most_things(cursor, connection, work_id):
    values = [work_id, ]

    update_query = """
    INSERT INTO queue.run_once_work_add_most_things (work_id, rand, methods)
    VALUES(%s, random(), 'update_affiliations') on conflict do nothing
    """

    try:
        cursor.execute(update_query, values)
        connection.commit()
    except:
        connection.rollback()  # Roll back the changes in case of an error
        logger.info("Error while updating row in PostgreSQL:", values)


def insert_into_curation_table(cursor, connection, row):
    values = [
        row['github_issue_number'],
        row['work_id'],
        row['original_affiliation'],
        row['contact_domain'],
        row['openalex_approve'],
        row['create_date'],
        row['update_date'],
    ]

    # Handle the JSON arrays differently based on what data we have
    if isinstance(row['affiliation_ids_add'], list) and isinstance(row['affiliation_ids_remove'], list):
        affs_to_add = ','.join([str(x) for x in row['affiliation_ids_add']])
        affs_to_remove = ','.join([str(x) for x in row['affiliation_ids_remove']])
        update_query = f"""
        INSERT INTO authorships.work_specific_affiliation_string_curation (
            github_issue_number, work_id, original_affiliation,
            affiliation_ids_add, affiliation_ids_remove, contact_domain, 
            openalex_approve, create_date, update_date
        )
        VALUES (
            %s, %s, %s, 
            jsonb_build_array({affs_to_add}), jsonb_build_array({affs_to_remove}), 
            %s, %s, %s, %s
        )
        """
    elif isinstance(row['affiliation_ids_add'], list):
        affs_to_add = ','.join([str(x) for x in row['affiliation_ids_add']])
        update_query = f"""
        INSERT INTO authorships.work_specific_affiliation_string_curation (
            github_issue_number, work_id, original_affiliation,
            affiliation_ids_add, affiliation_ids_remove, contact_domain, 
            openalex_approve, create_date, update_date
        )
        VALUES (
            %s, %s, %s, 
            jsonb_build_array({affs_to_add}), null, 
            %s, %s, %s, %s
        )
        """
    elif isinstance(row['affiliation_ids_remove'], list):
        affs_to_remove = ','.join([str(x) for x in row['affiliation_ids_remove']])
        update_query = f"""
        INSERT INTO authorships.work_specific_affiliation_string_curation (
            github_issue_number, work_id, original_affiliation,
            affiliation_ids_add, affiliation_ids_remove, contact_domain, 
            openalex_approve, create_date, update_date
        )
        VALUES (
            %s, %s, %s, 
            null, jsonb_build_array({affs_to_remove}), 
            %s, %s, %s, %s
        )
        """
    else:
        # Handle the case where both are None
        update_query = """
        INSERT INTO authorships.work_specific_affiliation_string_curation (
            github_issue_number, work_id, original_affiliation,
            affiliation_ids_add, affiliation_ids_remove, contact_domain, 
            openalex_approve, create_date, update_date
        )
        VALUES (
            %s, %s, %s, 
            null, null, 
            %s, %s, %s, %s
        )
        """

    try:
        cursor.execute(update_query, values)
        connection.commit()
    except Exception as e:
        # Roll back the changes in case of an error
        connection.rollback()
        logger.info(f"Error while updating row in PostgreSQL: {values}")
        logger.info(f"Error details: {e}")
        did_not_get_added.append(row['github_issue_number'])


def main():
    # Read arguments
    args = parse_args()

    # Get the instance of the Spreadsheet
    sheet = client.open('affiliation curation (DO NOT SORT EXCEPT BY ISSUE NUMBER)')

    # Get the first sheet of the Spreadsheet
    sheet_instance = sheet.get_worksheet(0)

    # Get all the records of the data
    records_data = sheet_instance.get_all_records()

    # Getting number of rows of data
    last_row_index = len(records_data)

    # Get highest issue number that is in the spreadsheet
    max_issue_id_from_previous = max([r.get('issue_number', 0) for r in records_data]) if records_data else 0

    if args.method == 'load_latest_issues':
        logger.info("Loading latest github issues")
        _ = load_latest_github_issues(sheet_instance, max_issue_id_from_previous, last_row_index, args.limit)
    else:
        logger.info("Full processing")

        logger.info("Loading latest github issues")
        open_issues = load_latest_github_issues(sheet_instance, max_issue_id_from_previous, last_row_index, args.limit)

        logger.info(
            "Use automatic approval to update approval column (if there are any rows with empty approval field)")
        auto_approve_requests(sheet_instance, records_data)

        logger.info("Load latest approvals to the database")
        load_latest_approvals_to_db(records_data, open_issues)


if __name__ == '__main__':
    main()