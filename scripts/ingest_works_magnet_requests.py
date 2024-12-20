import pandas as pd
import argparse
import boto3
import json
import os
import time
import requests
import psycopg2
import gspread
import scripts.works_magnet_auto_approver as auto_approver
from google.oauth2 import service_account
from multiprocessing import Pool
from sqlalchemy import create_engine
from app import logger
from datetime import datetime

# load config vars
AWS_ACCESS_KEY_ID = os.getenv('AWS_SAGEMAKER_ACCOUNT_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SAGEMAKER_ACCOUNT_SECRET')
GCLOUD_AUTHOR_CURATION_CREDS = os.getenv('GCLOUD_AUTHOR_CURATION')
GITHUB_PAT = os.getenv('GITHUB_PAT')
did_not_get_added = []
headers = {
        "Accept":  "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_PAT}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

# define the scope
scope = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
creds_dict = json.loads(GCLOUD_AUTHOR_CURATION_CREDS.replace('\\\n', '\\n'))

# add credentials to the account
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scope)

# authorize the clientsheet 
client = gspread.authorize(creds)

def get_secret(secret_name = "prod/psqldb/conn_string"):

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
    return parser.parse_args()

def get_open_github_issues():
    fields = ['raw_affiliation_name', 'new_rors', 'previous_rors', 'works_examples','body']
    def parse_body(e):
        elt = {}
        for lx, line in enumerate(e.split('\n')):
            for f in fields:
                if f+':' in line:
                    elt[f] = line.replace(f+':', '').strip()
        return elt

    issues = []
    for p in range(1, 100000):
        res = requests.get(f'https://api.github.com/repos/dataesr/openalex-affiliations/issues?page={p}', headers=headers)
        current_issues=res.json()
        if len(current_issues):
            issues += current_issues
        else:
            break
    logger.info(f"{len(issues)} open issues have been retrieved from github")

    for issue in issues:
        parsed = parse_body(issue['body'])
        issue.update(parsed)
        issue_id = issue['url'].split('/')[-1]
        issue['issue_id'] = int(issue_id)
    df_issues = pd.DataFrame(issues)[fields+['issue_id']]

    return df_issues.sort_values('issue_id')


def load_latest_github_issues(sheet_instance, max_issue_id_from_previous, last_row_index):

    df_issues = get_open_github_issues()

    df_issues['has_added'] = df_issues.apply(lambda x: True if [i for i in [new for new in x.new_rors.split(";") if new != ''] if 
                                                               i not in [prev for prev in x.previous_rors.split(";") if prev !='']] else False, axis=1)
    df_issues['added_rors'] = df_issues.apply(lambda x: ";".join([i for i in [new for new in x.new_rors.split(";") if new != ''] if 
                                                                i not in [prev for prev in x.previous_rors.split(";") if prev !='']]), axis=1)
    df_issues['has_removed'] = df_issues.apply(lambda x: True if [i for i in [prev for prev in x.previous_rors.split(";") if prev !=''] if 
                                                                i not in [new for new in x.new_rors.split(";") if new != '']] else False, axis=1)
    df_issues['removed_rors'] = df_issues.apply(lambda x: ";".join([i for i in [prev for prev in x.previous_rors.split(";") if prev !=''] if 
                                                                i not in [new for new in x.new_rors.split(";") if new != '']]), axis=1)
    df_issues['contact_full'] = df_issues['body'].apply(lambda x: [i for i in x.split('\n') if i.startswith('contact')][0])
    df_issues['contact'] = df_issues['contact_full'].apply(lambda x: x.split("@")[0] if len(x.split("@"))>0 else "")
    df_issues['contact_domain'] = df_issues['contact_full'].apply(lambda x: x.split("@")[1] if len(x.split("@"))>0 else "")
    df_issues['OpenAlex'] = ""
    df_issues['Notes'] = ""
    df_issues['Notes1'] = ""
    df_issues['Notes2'] = ""
    df_issues['Notes3'] = ""

    num_issues_to_load = df_issues[df_issues['issue_id']>max_issue_id_from_previous].shape[0]
    if num_issues_to_load == 0:
        logger.info("No new issues to load")
        return
    _ = sheet_instance.update(df_issues[df_issues['issue_id']>max_issue_id_from_previous]\
        [['issue_id','has_added','has_removed','new_rors','previous_rors','raw_affiliation_name','added_rors',
        'removed_rors','works_examples','contact','contact_domain']].sort_values('issue_id').values.tolist(), f'A{last_row_index+2}')
    
    logger.info(f"Loaded {num_issues_to_load} github issues to the spreadsheet")

    return df_issues

def get_id_from_ror(list_of_rors, ror_to_aff_id):
    if isinstance(list_of_rors, str):
        return [ror_to_aff_id.get(x, "") for x in list_of_rors.strip().split(";")]
    elif isinstance(list_of_rors, list):
        return list_of_rors
    else:
        return []
    
def get_list_of_works(works_string):
    if isinstance(works_string, str):
        return [int(x) for x in works_string.strip()[1:].split(";W") if x !='']
    elif isinstance(works_string, list):
        return works_string
    else:
        return []
    
def turn_ror_back_to_none(ror_list):
    if not ror_list:
        return None
    elif not [x for x in ror_list if x != '']:
        return None
    else:
        return [x for x in ror_list if x != '']
    
def get_string_nan(contact_domain):
    if isinstance(contact_domain, float):
        return 'NaN'
    else:
        return contact_domain
    
def check_aff_ids(old_affs, new_affs):
    if isinstance(old_affs, list):
        if isinstance(new_affs, list):
            if set(old_affs) == set(new_affs):
                return True
            else:
                return False
        else:
            return False
    else:
        if isinstance(new_affs, list):
            return False
        else:
            return True
        
def insert_into_add_most_things(cursor, connection, work_id):
    values = [work_id,]
    
    update_query = \
    f"""INSERT INTO queue.run_once_work_add_most_things (work_id, rand, methods)
    VALUES(%s, random(), 'update_affiliations') on conflict do nothing"""

    try:
        cursor.execute(update_query, values)
        connection.commit()

    except:
        connection.rollback()  # Roll back the changes in case of an error
        logger.info("Error while updating row in PostgreSQL:", values)
        
def insert_into_curation_table(cursor, connection, row):
    values = [row.github_issue_number,row.work_id,row.original_affiliation, 
              row.contact_domain,row.openalex_approve,row.create_date,row.update_date,]

    if isinstance(row.affiliation_ids_add, list) and isinstance(row.affiliation_ids_remove, list):
        affs_to_add = ','.join([str(x) for x in row.affiliation_ids_add])
        affs_to_remove = ','.join([str(x) for x in row.affiliation_ids_remove])
        update_query = \
        f"""INSERT INTO authorships.work_specific_affiliation_string_curation (github_issue_number,work_id,original_affiliation,
           affiliation_ids_add,affiliation_ids_remove,contact_domain,openalex_approve,create_date,update_date)
           VALUES(%s,%s,%s, jsonb_build_array({affs_to_add}), jsonb_build_array({affs_to_remove}), %s, %s, %s, %s)"""
    elif isinstance(row.affiliation_ids_add, list):
        affs_to_add = ','.join([str(x) for x in row.affiliation_ids_add])
        update_query = \
        f"""INSERT INTO authorships.work_specific_affiliation_string_curation (github_issue_number,work_id,original_affiliation,
           affiliation_ids_add,affiliation_ids_remove,contact_domain,openalex_approve,create_date,update_date)
           VALUES(%s,%s,%s, jsonb_build_array({affs_to_add}), null, %s, %s, %s, %s)"""
    elif isinstance(row.affiliation_ids_remove, list):
        affs_to_remove = ','.join([str(x) for x in row.affiliation_ids_remove])
        update_query = \
        f"""INSERT INTO authorships.work_specific_affiliation_string_curation (github_issue_number,work_id,original_affiliation,
           affiliation_ids_add,affiliation_ids_remove,contact_domain,openalex_approve,create_date,update_date)
           VALUES(%s,%s,%s, null, jsonb_build_array({affs_to_remove}), %s, %s, %s, %s)"""

    try:
        cursor.execute(update_query, values)
        connection.commit()

    except:
        # Roll back the changes in case of an error
        connection.rollback()
        logger.info("Error while updating row in PostgreSQL:", values)
        did_not_get_added.append(row.github_issue_number)

def auto_approve_requests(sheet_instance, records_data):
    # get the ror to aff id mapping
    secret = get_secret()
    engine = \
    create_engine(f"postgresql://awsuser:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}")

    query = f"SELECT affiliation_id, ror_id, display_name FROM mid.institution where merge_into_id is null"

    current_institution_table = pd.read_sql_query(query, engine)

    ror_to_aff_id = current_institution_table.set_index('ror_id').to_dict()['affiliation_id']

    # pull issues into a dataframe
    issues_and_approve = pd.DataFrame(records_data)\
        [['issue_number','OpenAlex Approve?','Notes2']]
    
    issues_needing_approval = issues_and_approve[issues_and_approve['OpenAlex Approve?'] == ''].copy()
    
    num_issues_to_approve = issues_needing_approval.shape[0]
    
    logger.info(f"Found {num_issues_to_approve} issues to auto-approve")

    if num_issues_to_approve == 0:
        return
    
    issues_needing_approval['added_rors'] = issues_needing_approval['added_rors']\
        .apply(lambda x: get_id_from_ror(x, ror_to_aff_id))
    issues_needing_approval['removed_rors'] = issues_needing_approval['removed_rors']\
        .apply(lambda x: get_id_from_ror(x, ror_to_aff_id))
    issues_needing_approval['added_rors'] = issues_needing_approval['added_rors'].apply(turn_ror_back_to_none)
    issues_needing_approval['removed_rors'] = issues_needing_approval['removed_rors'].apply(turn_ror_back_to_none)

    with Pool(4) as p:
        arrays = p.starmap_async(func= auto_approver.approve_works_magnet_request, 
                             iterable=[(i,j,k,m) for i,j,k,m in zip(issues_needing_approval['raw_affiliation_name'].tolist(), 
                                                                    issues_needing_approval['added_rors'].tolist(), 
                                                                    issues_needing_approval['removed_rors'].tolist(), 
                                                                    issues_needing_approval['issue_number'].tolist())])
        all_arrays = arrays.get()
    
    auto_approve_output = pd.DataFrame(all_arrays)
    auto_approve_output.columns = ['OpenAlex Approve?', 'Notes2', 'issue_number','original_affiliation']

    num_approved = auto_approve_output[auto_approve_output['OpenAlex Approve?'] == 'Yes'].shape[0]
    num_need_human = auto_approve_output[auto_approve_output['OpenAlex Approve?'] == ''].shape[0]
    num_failed = auto_approve_output[auto_approve_output['OpenAlex Approve?'] == 'No'].shape[0]
    num_rejected = auto_approve_output[auto_approve_output['OpenAlex Approve?'] == 'No - auto'].shape[0]

    logger.info(f"Auto-approved {num_approved} issues")
    logger.info(f"Auto-rejected {num_rejected} issues")
    logger.info(f"Failed to auto-approve {num_failed} issues")
    logger.info(f"Need human approval for {num_need_human} issues")

    df1 = issues_and_approve.set_index('issue_number')
    df2 = auto_approve_output.set_index('issue_number')
    df1.update(df2)

    final_approval_df = df1.reset_index().sort_values('issue_number').copy()

    logger.info("Updating the spreadsheet with the auto-approval results")
    logger.info(f"TEST: {issues_and_approve.shape[0]} issues in original DF")
    logger.info(f"TEST: {len(final_approval_df['OpenAlex Approve?'].tolist())} rows in new notes/approval list")

    if issues_and_approve.shape[0] == len(final_approval_df['OpenAlex Approve?'].tolist()):
        logger.info("Updating the spreadsheet with the auto-approval results")
        # _ = sheet_instance.update(final_approval_df\
        #                           [['OpenAlex Approve?']].values.tolist(), 'L2')
        
        # _ = sheet_instance.update(final_approval_df\
        #                           [['Notes2']].values.tolist(), 'O2')

def load_latest_approvals_to_db(sheet_instance, records_data, open_issues, max_issue_id_from_previous, last_row_index):
    raw_strings = open_issues[['issue_id','raw_affiliation_name']].rename(columns={'issue_id':'issue_number', 'raw_affiliation_name': 'original_affiliation'})\
    .sort_values('issue_number').reset_index(drop=True).copy()

    google_sheet_data = pd.DataFrame(records_data)\
        [['issue_number','previous_rors','new_rors','added_rors','removed_rors','works_examples',
        'OpenAlex Approve?','contact_domain']] \
        .merge(raw_strings, how='inner', on='issue_number')

    # Get latest institution table
    secret = get_secret()
    engine = \
        create_engine(f"postgresql://awsuser:{secret['password']}@{secret['host']}:{secret['port']}/{secret['dbname']}")

    query = f"SELECT affiliation_id, ror_id, display_name FROM mid.institution where merge_into_id is null"

    current_institution_table = pd.read_sql_query(query, engine)

    ror_to_aff_id = current_institution_table.set_index('ror_id').to_dict()['affiliation_id']

    # Process data in the google sheet
    google_sheet_data['added_rors'] = google_sheet_data['added_rors'].apply(lambda x: get_id_from_ror(x, ror_to_aff_id))
    google_sheet_data['removed_rors'] = google_sheet_data['removed_rors'].apply(lambda x: get_id_from_ror(x, ror_to_aff_id))
    google_sheet_data['works_examples'] = google_sheet_data['works_examples'].apply(get_list_of_works)
    google_sheet_data['works_examples_len'] = google_sheet_data['works_examples'].apply(len)
    google_sheet_data['added_rors_len'] = google_sheet_data['added_rors'].apply(len)
    google_sheet_data['removed_rors_len'] = google_sheet_data['removed_rors'].apply(len)
    google_sheet_data['added_rors'] = google_sheet_data['added_rors'].apply(turn_ror_back_to_none)
    google_sheet_data['removed_rors'] = google_sheet_data['removed_rors'].apply(turn_ror_back_to_none)
    google_sheet_data['contact_domain'] = google_sheet_data['contact_domain'].apply(get_string_nan)
    google_sheet_data['OpenAlex Approve?'] = google_sheet_data['OpenAlex Approve?'].apply(lambda x: True if x in ['Yes','yes',True] else False)

    final_df_for_curation = google_sheet_data[(google_sheet_data['OpenAlex Approve?']) & 
                                              (google_sheet_data['works_examples_len'] > 0) & 
                                              ((google_sheet_data['added_rors_len'] > 0) | 
                                               (google_sheet_data['removed_rors_len'] > 0))]\
        [['issue_number','added_rors','removed_rors', 'works_examples', 'OpenAlex Approve?', 'contact_domain', 
        'original_affiliation']].explode('works_examples').copy()
    final_df_for_curation.columns = ['github_issue_number','affiliation_ids_add','affiliation_ids_remove', 'work_id', 'openalex_approve', 
                                    'contact_domain', 'original_affiliation']
    
    if final_df_for_curation.shape[0] == 0:
        logger.info("No new approvals to load to the database")
        return
    
    logger.info(f"Loading {final_df_for_curation.shape[0]} new approvals to the database")

    # Get the latest work_specific_affiliation_string_curation table
    query = f"SELECT * FROM authorships.work_specific_affiliation_string_curation"

    current_curation_table = pd.read_sql_query(query, engine)

    same_rows = final_df_for_curation.merge(current_curation_table[['github_issue_number', 'work_id','create_date']], 
                                        how='inner', on=['github_issue_number','work_id'])
    same_rows.shape
    
    current_same_rows = current_curation_table.merge(same_rows[['github_issue_number', 'work_id']], 
                                                    how='inner', on=['github_issue_number','work_id'])\
        .sort_values(['github_issue_number','work_id'])
    new_same_rows = final_df_for_curation.merge(same_rows[['github_issue_number', 'work_id','create_date']], 
                                                    how='inner', on=['github_issue_number','work_id'])\
        .sort_values(['github_issue_number','work_id'])
    
    same_data = []
    for (i1, r1), (i2, r2) in zip(current_same_rows.iterrows(), new_same_rows.iterrows()):
        if r1.original_affiliation == r2.original_affiliation:
            if (r1.contact_domain == r2.contact_domain) or ((not isinstance(r1.contact_domain, str)) & (not isinstance(r2.contact_domain, str))):
                if r1.openalex_approve == r2.openalex_approve:
                    if check_aff_ids(r1.affiliation_ids_add, r2.affiliation_ids_add):
                        if check_aff_ids(r1.affiliation_ids_remove, r2.affiliation_ids_remove):
                            same_data.append(1)
                            continue
                            
        same_data.append(0)

    new_same_rows['same'] = same_data

    if new_same_rows[new_same_rows['same']==0].shape[0] > 0:
        rows_to_update = new_same_rows[new_same_rows['same']==0] \
            [['github_issue_number', 'affiliation_ids_add', 'affiliation_ids_remove',
            'work_id', 'openalex_approve', 'contact_domain', 'original_affiliation',
            'create_date']].copy()
        rows_to_update['update_date'] = pd.Timestamp.today()

        logger.info(f"{rows_to_update.shape[0]} rows to update in the database but this script is not set up for table updates.")

    new_rows = final_df_for_curation.merge(current_curation_table[['github_issue_number', 'work_id','create_date']], 
                                        how='left', on=['github_issue_number','work_id'])

    new_rows = new_rows[new_rows['create_date'].isnull()] \
        [['github_issue_number', 'affiliation_ids_add', 'affiliation_ids_remove',
        'work_id', 'openalex_approve', 'contact_domain', 'original_affiliation']].copy()
    new_rows['create_date'] = pd.Timestamp.today()
    new_rows['update_date'] = pd.Timestamp.today()
    
    logger.info(f"{new_rows.shape[0]} new rows to insert into the database")

    # Establish a connection to the PostgreSQL database
    conn = connect_to_db()

    # Create a cursor object to execute SQL queries
    cur = conn.cursor()

    if new_rows.shape[0] == 0:
        logger.info("No new rows to insert into the database")
        return
    
    for index, insert_row in new_rows.iterrows():
        _ = insert_into_curation_table(cur, conn, insert_row)

    logger.info(f"The following issues did not get added to the database because of a error: {', '.join(did_not_get_added)}")
    for index, insert_row in new_rows[~new_rows['github_issue_number'].isin(did_not_get_added)].iterrows():
        _ = insert_into_add_most_things(cur, conn, insert_row.work_id)

    cur.close()
    conn.close()

    curr_date = datetime.now().strftime("%Y-%m-%d")

    final_issues_to_add_list = new_rows[new_rows['openalex_approve'] & (~new_rows['github_issue_number'].isin(did_not_get_added))]\
        .drop_duplicates(subset=['github_issue_number'])['github_issue_number'].tolist()

    did_not_finish = False
    # close the github tickets and comment on them
    for i, github_issue_number in enumerate(final_issues_to_add_list):
        time.sleep(10) # sleep for 10 seconds to avoid rate limit on github
        logger.info(f"row {i}: closing issue {github_issue_number}")
        # comment on the issue
        data = {'body': f'This issue was accepted and ingested by the OpenAlex team on {curr_date}. The new affiliations should be visible within the next 7 days.'}
        url = f"https://api.github.com/repos/dataesr/openalex-affiliations/issues/{github_issue_number}/comments"
        comments = requests.get(url=url, headers=headers).json()
        if not comments:
            resp = requests.post(url=url, data=json.dumps(data), headers=headers)
            if resp.status_code != 201:
                time.sleep(60)
                resp = requests.post(url=url, data=json.dumps(data), headers=headers)
                if resp.status_code != 201:
                    logger.info(f"Error while commenting on issue {github_issue_number} (second try)")
                    logger.info(f"Exiting github approval process. The following issues need to be closed but were skipped: {', '.join(final_issues_to_add_list[i:])}")
                    did_not_finish = True
                    break

        # close the ticket
        data = {'state_reason': 'completed',
                'state': 'closed'}
        url = f"https://api.github.com/repos/dataesr/openalex-affiliations/issues/{github_issue_number}"
        resp = requests.post(url=url, data=json.dumps(data), headers=headers)

    if not did_not_finish:
        logger.info("All new approvals have been loaded to the database")

def main():
    # read arguments
    args = parse_args()

    # get the instance of the Spreadsheet
    sheet = client.open('affiliation curation (DO NOT SORT EXCEPT BY ISSUE NUMBER)')

    # get the first sheet of the Spreadsheet
    sheet_instance = sheet.get_worksheet(0)

    # get all the records of the data
    records_data = sheet_instance.get_all_records()

    # getting number of rows of data
    last_row_index = len(records_data)

    # get highest issue number that is in the spreadsheet
    max_issue_id_from_previous = pd.DataFrame(records_data)['issue_number'].max()

    if args.method == 'load_latest_issues':
        logger.info("Loading latest github issues")
        _ = load_latest_github_issues(sheet_instance, max_issue_id_from_previous, last_row_index)
    else:
        logger.info("Full processing")

        logger.info("Loading latest github issues")
        open_issues = load_latest_github_issues(sheet_instance, max_issue_id_from_previous, last_row_index)

        logger.info("Use automatic approval to update approval column (if there are any rows with empty approval field)")
        _ = auto_approve_requests(sheet_instance, records_data)

        logger.info("Load latest approvals to the database")
        # _ = load_latest_approvals_to_db(sheet_instance, records_data, open_issues, max_issue_id_from_previous, last_row_index)

if __name__ == '__main__':
    main()