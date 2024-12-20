import json
import boto3
import os
import botocore
import psycopg2
import openai
print(openai.__version__)
from pydantic import BaseModel

openai_api_key = os.getenv("OPENAI_API_KEY")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

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

def get_secret():

    secret_name = "prod/psqldb/conn_string"
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
    except botocore.exceptions.ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    # Decrypts secret using the associated KMS key.
    secret_string = get_secret_value_response['SecretString']
    
    secret = json.loads(secret_string)
    return secret

def validate_affiliation_id(aff_id, conn, cur):
    # connect to openalex DB and search for affiliation_id in institutions table
    cur.execute(f"SELECT display_name, city, country, ror_id FROM mid.institution WHERE affiliation_id = {aff_id} and merge_into_id is null")
    aff_data = cur.fetchone()

    if aff_data:
        cur.execute("SELECT acronym FROM ins.ror_acronyms WHERE ror_id = %s", (aff_data[3],))
        acronyms_fetch = cur.fetchall()
        acronyms = []
        if acronyms_fetch:
            acronyms = [x[0] for x in acronyms]

        aliases = []
        cur.execute("SELECT alias from ins.ror_aliases WHERE ror_id = %s", (aff_data[3],))
        aliases_fetch = cur.fetchall()
        aliases = []
        if aliases_fetch:
            aliases = [x[0] for x in aliases]
        if len(aliases) > 3:
            alt_names = list(set(acronyms + aliases[:3]))
        else:
            alt_names = list(set(acronyms + aliases))
        return {"affiliation_id": aff_id, "display_name": aff_data[0], "city": aff_data[1], "country": aff_data[2], "alt_names": alt_names}
    else:
        return None

def approve_works_magnet_request(affiliation_string, added_aff_ids, removed_aff_ids, github_issue_num):
    """
    affiliation_string: str
    added_aff_ids: list
    removed_aff_ids: list
    github_issue_num: int
    """

    conn = connect_to_db()
    cur = conn.cursor()
    client = OpenAI(api_key=openai_api_key)

    if not isinstance(added_aff_ids, list):
        added_aff_ids = []

    if not isinstance(removed_aff_ids, list):
        removed_aff_ids = []

    # check if there is data in either added aff_ids or removed aff_ids
    if not added_aff_ids and not removed_aff_ids:
        return {"verdict": "No", "reason": "No changes requested", "github_issue_num": github_issue_num, "original_affiliation": affiliation_string}
    
    invalid_aff_ids = []
    
    # check to see if there are valid aff_ids to add
    valid_added_aff_ids = []
    if added_aff_ids:
        for aff_id in added_aff_ids:
            validation_resp = validate_affiliation_id(aff_id, conn, cur)
            if validation_resp:
                valid_added_aff_ids.append(validation_resp)
            else:
                invalid_aff_ids.append(aff_id)
            
    # check to see if there are valid aff_ids to remove
    valid_removed_aff_ids = []
    if removed_aff_ids:
        for aff_id in removed_aff_ids:
            validation_resp = validate_affiliation_id(aff_id, conn, cur)
            if validation_resp:
                valid_removed_aff_ids.append(validation_resp)
            else:
                invalid_aff_ids.append(aff_id)
    
    if not valid_added_aff_ids and not valid_removed_aff_ids:
        return {"verdict": "No", "reason": "No valid aff_ids to add or remove", "github_issue_num": github_issue_num, "original_affiliation": affiliation_string}
    
    approval_rejections = []
    # send valid aff_ids and affiliation string to OpenAI to approve/reject for adding
    if valid_added_aff_ids:
        for aff_id in valid_added_aff_ids:
            openai_approve = openai_approve_aff_id_add_or_remove(aff_id, affiliation_string, client)
            if openai_approve['unknown']:
                approval_rejections.append(["", f"OpenAI is not sure about the aff_ids to add for the following reason: {openai_approve['reason']}"])
            else:
                if not openai_approve['verdict']:
                    approval_rejections.append(["No", f"OpenAI has rejected the aff_ids to add for the following reason: {openai_approve['reason']}"])
                else:
                    approval_rejections.append(["Yes", "aff_ids to add have been approved"])
            
    # send valid aff_ids and affiliation string to OpenAI to approve/reject for removing
    if valid_removed_aff_ids:
        for aff_id in valid_removed_aff_ids:
            openai_approve = openai_approve_aff_id_add_or_remove(aff_id, affiliation_string, client)
            if openai_approve['unknown']:
                approval_rejections.append(["", f"OpenAI is not sure about the aff_ids to remove for the following reason: {openai_approve['reason']}"])
            else:
                if openai_approve['verdict']:
                    approval_rejections.append(["No", f"OpenAI has rejected the aff_ids to remove for the following reason: {openai_approve['reason']}"])
                else:
                    approval_rejections.append(["Yes", "aff_ids to remove have been approved"])

    approval_rejection_summary = summarize_approval_rejections(approval_rejections, github_issue_num, affiliation_string)

    cur.close()
    conn.close()

    return approval_rejection_summary


def summarize_approval_rejections(approval_rejections, github_issue_num, affiliation_string):
    verdicts = [x[0] for x in approval_rejections]
    reasons = "; ".join([x[1] for x in approval_rejections if (x[0] == "No") or (x[0] == "")])
    
    if "No" in verdicts:
        return {"verdict": "No - auto", "reason": reasons, "github_issue_num": github_issue_num, "original_affiliation": affiliation_string}
    elif "" in verdicts:
        return {"verdict": "", "reason": reasons, "github_issue_num": github_issue_num, "original_affiliation": affiliation_string}
    else:
        return {"verdict": "Yes", "reason": "All changes have been approved - Automated process", "github_issue_num": github_issue_num, "original_affiliation": affiliation_string}
    
def openai_approve_aff_id_add_or_remove(aff_id, affiliation_string, client):
    openai_context = messages_for_aff_id_add(aff_id, affiliation_string)

    completion = client.beta.chat.completions.parse(
            model="gpt-4o-2024-08-06",
            messages=openai_context,
            response_format=WorksMagnetApprovalObject
        )
    
    return json.loads(completion.choices[0].message.content)

def create_system_information():
    system_info = "If you are unsure of the answer, do not guess.\n\n"
    system_info += "Always give short reason for answer (as few words as possible)."
    return system_info

def messages_for_aff_id_add(aff_id, aff_string):
    information_for_system = create_system_information()

    aff_string_1 = "Hotel Dieu de France hospital, St Joseph University"
    display_name_1 = "Hôtel-Dieu de France"
    country_1 = "France"
    example_1 = f"Look at the following string:\n {aff_string_1}\n\nDoes the institution '{display_name_1}' in {country_1} show up in this string?"
    example_1_answer = {
        "verdict": True,
        "unknown": False,
        "reason": "Institution in string"
        }
    
    aff_string_2 = "Hotel Dieu de France hospital, St Joseph University"
    display_name_2 = "St. Jos. Univ."
    country_2 = "France"
    example_2 = f"Look at the following string:\n {aff_string_2}\n\nDoes the institution '{display_name_2}' in {country_2} show up in this string?"
    example_2_answer = {
        "verdict": False,
        "unknown": True,
        "reason": "Can't tell if that institution is in the string"
        }
    
    aff_string_3 = "Ivey Business School at Western University"
    display_name_3 = "Western University"
    country_3 = "Canada"
    example_3 = f"Look at the following string:\n {aff_string_3}\n\nDoes the institution '{display_name_3}' in {country_3} show up in this string?"
    example_3 += f" (Keep in mind the alternate names or acronyms: Université de Western Ontario, University of Western Ontario)"
    example_3_answer = {
        "verdict": True,
        "unknown": False,
        "reason": "Institution in string"
        }
    
    aff_string_4 = "Sorbonne University, Paris, France"
    display_name_4 = "Université Paris-Saclay"
    country_4 = "France"
    example_4 = f"Look at the following string:\n {aff_string_4}\n\nDoes the institution '{display_name_4}' in {country_4} show up in this string?"
    example_4 += f" (Keep in mind the alternate names or acronyms: Universitat París-Saclay, University of Paris-Saclay, Paris-Saclayko Unibertsitatea)"
    example_4_answer = {
        "verdict": False,
        "unknown": False,
        "reason": "Institution in string"
        }
    
    aff_string_final = aff_string
    display_name_final = aff_id['display_name']
    country_final = aff_id['country']
    final_question = f"Look at the following string:\n {aff_string_final}\n\nDoes the institution '{display_name_final}' in {country_final} show up in this string?"
    if aff_id['alt_names']:
        final_question += f" (Keep in mind the alternate names or acronyms: {', '.join(aff_id['acronyms'])})"

    messages = [
        {"role": "system", 
         "content": "You are helping to approve or disapprove curation requests for OpenAlex."},
        {"role": "user", "content": information_for_system},
        {"role": "assistant", 
         "content": "I will refer back to this information when determining the different elements of the prompt"},
        {"role": "user","content": example_1}, 
        {"role": "assistant","content": json.dumps(example_1_answer)},
        {"role": "user","content": example_2}, 
        {"role": "assistant","content": json.dumps(example_2_answer)},
        {"role": "user","content": example_3}, 
        {"role": "assistant","content": json.dumps(example_3_answer)},
        {"role": "user","content": example_4}, 
        {"role": "assistant","content": json.dumps(example_4_answer)},
        {"role": "user","content": final_question}
    ]
    return messages

class WorksMagnetApprovalObject(BaseModel):
    verdict: bool
    unknown: bool
    reason: str