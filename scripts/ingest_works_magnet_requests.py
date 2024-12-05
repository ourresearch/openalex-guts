import pandas as pd
import argparse
import boto3
import json
import os
import requests
import psycopg2
import gspread
from google.oauth2 import service_account
from app import logger

# load config vars
AWS_ACCESS_KEY_ID = os.getenv('AWS_SAGEMAKER_ACCOUNT_KEY')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SAGEMAKER_ACCOUNT_SECRET')
GCLOUD_AUTHOR_CURATION_CREDS = os.getenv('GCLOUD_AUTHOR_CURATION')
GITHUB_PAT = os.getenv('GITHUB_PAT')

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
    fields = ['raw_affiliation_name', 'new_rors', 'previous_rors', 'works_examples']
    def parse_body(e):
        elt = {}
        for lx, line in enumerate(e.split('\n')):
            for f in fields:
                if f+':' in line:
                    elt[f] = line.replace(f+':', '').strip()
        return elt

    headers = {
        "Accept":  "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_PAT}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
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
    df_issues['contact_full'] = df_issues['body'].apply(lambda x: [i for i in df_issues.iloc[0]['body'].split('\n') if i.startswith('contact')][0])
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



def main():
    # read arguments
    args = parse_args()

    # get the instance of the Spreadsheet
    sheet = client.open('affiliation curation')

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

if __name__ == '__main__':
    main()