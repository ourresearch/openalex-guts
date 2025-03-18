import pandas as pd
import requests
import time
import os
import json
from datetime import datetime

GITHUB_PAT = os.getenv('GITHUB_PAT')
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {GITHUB_PAT}",
    "X-GitHub-Api-Version": "2022-11-28"
}

import gspread
from google.oauth2 import service_account

GCLOUD_AUTHOR_CURATION_CREDS = os.getenv('GCLOUD_AUTHOR_CURATION')
scope = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']
creds_dict = json.loads(GCLOUD_AUTHOR_CURATION_CREDS.replace('\\\n', '\\n'))
creds = service_account.Credentials.from_service_account_info(creds_dict, scopes=scope)
client = gspread.authorize(creds)

# Get the instance of the Spreadsheet
sheet = client.open('affiliation curation (DO NOT SORT EXCEPT BY ISSUE NUMBER)')
sheet_instance = sheet.get_worksheet(0)
records_data = sheet_instance.get_all_records()


# Get all issues from GitHub to check which ones are still open
def get_all_github_issues():
    all_issues = []
    for p in range(1, 1000):
        print(f"Getting page {p} of issues")
        res = requests.get(
            f'https://api.github.com/repos/dataesr/openalex-affiliations/issues?state=all&page={p}&per_page=100',
            headers=headers)
        current_issues = res.json()
        if not isinstance(current_issues, list) or len(current_issues) == 0:
            break
        all_issues.extend(current_issues)
    return all_issues


# Get issues from spreadsheet that should be closed
approved_issues = pd.DataFrame(records_data)
approved_issues = approved_issues[approved_issues['OpenAlex Approve?'].isin(['Yes', 'yes'])]
issues_to_close = approved_issues['issue_number'].tolist()
print(f"Found {len(issues_to_close)} approved issues in spreadsheet")

# Get all GitHub issues to check their status
github_issues = get_all_github_issues()
open_issues = {int(issue['number']): issue for issue in github_issues if issue['state'] == 'open'}
print(f"Found {len(open_issues)} open issues on GitHub")

# Find intersection - approved issues that are still open
issues_to_close = [issue for issue in issues_to_close if issue in open_issues]
print(f"Found {len(issues_to_close)} approved issues that are still open")

# Close these issues
curr_date = datetime.now().strftime("%Y-%m-%d")
for i, github_issue_number in enumerate(issues_to_close):
    time.sleep(2)  # Be gentle with the API
    print(f"{i + 1}/{len(issues_to_close)}: Closing issue {github_issue_number}")

    # Add comment
    data = {
        'body': f'This issue was accepted and ingested by the OpenAlex team on {curr_date}. The new affiliations should be visible within the next 7 days.'}
    url = f"https://api.github.com/repos/dataesr/openalex-affiliations/issues/{github_issue_number}/comments"
    comments = requests.get(url=url, headers=headers).json()
    if not comments or not any(curr_date in comment.get('body', '') for comment in comments):
        resp = requests.post(url=url, data=json.dumps(data), headers=headers)
        print(f"  Comment status: {resp.status_code}")
        if resp.status_code != 201:
            print(f"  Error posting comment: {resp.text}")

    # Close the issue
    data = {'state_reason': 'completed', 'state': 'closed'}
    url = f"https://api.github.com/repos/dataesr/openalex-affiliations/issues/{github_issue_number}"
    resp = requests.patch(url=url, data=json.dumps(data), headers=headers)
    print(f"  Close status: {resp.status_code}")
    if resp.status_code != 200:
        print(f"  Error closing issue: {resp.text}")