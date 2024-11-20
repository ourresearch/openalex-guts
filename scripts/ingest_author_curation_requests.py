import pandas as pd
import boto3
import json
import os
import random
import psycopg2
import unicodedata
from datetime import datetime
from nameparser import HumanName
from unidecode import unidecode
import gspread
from google.oauth2 import service_account
from app import logger

# load config vars
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
GCLOUD_AUTHOR_CURATION_CREDS = os.getenv('GCLOUD_AUTHOR_CURATION')
logger.info("g_cloud_cred: ", GCLOUD_AUTHOR_CURATION_CREDS[:10])

# the following actions are allowed by this curation workflow
workflows_allowed = ['Merge another profile into mine','Change the display name', 'Remove works from my profile','Add works to my profile']

# define the scope
scope = ['https://www.googleapis.com/auth/drive','https://www.googleapis.com/auth/spreadsheets']
creds_dict = json.loads(GCLOUD_AUTHOR_CURATION_CREDS)
logger.info("g_cloud_cred_type: ", type(creds_dict))
logger.info("g_cloud_cred: ", creds_dict['type'])

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

def check_first_and_last(first_names_1, first_names_2, last_names_1, last_names_2):
    """This function tries to catch times when the first and last name have been swapped
    at some point and so an authors first name and last name would show up in both the list
    of first names and the list of last names. This was causing authors to match with one name
    matching even though the other name did not match"""
    
    if first_names_1 and first_names_2 and last_names_1 and last_names_2:
        # check if both names in both lists
        
        if ((any(names in first_names_1 for names in last_names_1) and 
            ((len(first_names_1) > 1) or (len(first_names_1) > 1))) or 
            (any(names in first_names_2 for names in last_names_2) and 
            ((len(first_names_2) > 1) or (len(first_names_1) > 2)))):
            # if both names in both lists for both sets, each unique names need to match
            if (len(first_names_1) == 2 and 
                len(first_names_2) == 2 and 
                len(last_names_1) == 2 and 
                len(last_names_2) == 2):
                name_set_1 = set(first_names_1 + last_names_1)
                name_set_2 = set(first_names_2 + last_names_2)
                if all(names in name_set_1 for names in name_set_2):
                    return True
                else:
                    return False
            else:
                names_to_remove = []
                for first_name in first_names_1:
                    if ((first_name in first_names_2) and 
                        (first_name in last_names_1) and 
                        (first_name in last_names_2)):
                        names_to_remove.append(first_name)
                        
                new_first_names_1 = [x for x in first_names_1 if x not in names_to_remove]
                new_last_names_1 = [x for x in last_names_1 if x not in names_to_remove]
                new_first_names_2 = [x for x in first_names_2 if x not in names_to_remove]
                new_last_names_2 = [x for x in first_names_2 if x not in names_to_remove]
                
                names_are_good = 1
                if new_first_names_1 and new_last_names_1 and new_first_names_2 and new_last_names_2:
                    if (any(names in new_first_names_1 for names in new_first_names_2) or 
                        any(names in new_last_names_1 for names in new_last_names_2)):
                        return True
                    else:
                        return False
                elif (not new_last_names_1) or (not new_last_names_2):
                    if any(names in new_first_names_1 for names in new_first_names_2):
                        return True
                    else:
                        return False
                elif (not new_first_names_1) or (not new_first_names_2):
                    if any(names in new_last_names_1 for names in new_last_names_2):
                        return True
                    else:
                        return False
                else:
                    return True
        else:
            return True
            
    else:
        return True

def check_block_vs_block_reg(block_1_names_list, block_2_names_list):
    
    # check first names
    first_check, _ = match_block_names(block_1_names_list[0], block_1_names_list[1], block_2_names_list[0], 
                                    block_2_names_list[1])
    # logger.info(f"FIRST {first_check}")
    
    if first_check:
        last_check, _ = match_block_names(block_1_names_list[-2], block_1_names_list[-1], block_2_names_list[-2], 
                                           block_2_names_list[-1])
        # logger.info(f"LAST {last_check}")
        if last_check:

            # check to see if first name in last and last name in first
            # if that is the case, need to do matching between first/last of both pairs
            first_last_check = check_first_and_last(block_1_names_list[0], block_2_names_list[0], 
                                                    block_1_names_list[-2], block_2_names_list[-2])
            if first_last_check:
                pass
            else:
                return 0

            m1_check, more_to_go = match_block_names(block_1_names_list[2], block_1_names_list[3], block_2_names_list[2], 
                                           block_2_names_list[3])
            if m1_check:
                if not more_to_go:
                    return 1
                m2_check, more_to_go = match_block_names(block_1_names_list[4], block_1_names_list[5], block_2_names_list[4], 
                                                block_2_names_list[5])
                
                if m2_check:
                    if not more_to_go:
                        return 1
                    m3_check, more_to_go = match_block_names(block_1_names_list[6], block_1_names_list[7], block_2_names_list[6], 
                                                block_2_names_list[7])
                    if m3_check:
                        if not more_to_go:
                            return 1
                        m4_check, more_to_go = match_block_names(block_1_names_list[8], block_1_names_list[8], block_2_names_list[8], 
                                                block_2_names_list[9])
                        if m4_check:
                            if not more_to_go:
                                return 1
                            m5_check, _ = match_block_names(block_1_names_list[10], block_1_names_list[11], block_2_names_list[10], 
                                                block_2_names_list[11])
                            if m5_check:
                                return 1
                            else:
                                return 0
                        else:
                            return 0
                    else:
                        return 0
                else:
                    return 0
            else:
                return 0
        else:
            return 0
    else:
        swap_check = check_if_last_name_swapped_to_front_creates_match(block_1_names_list, block_2_names_list)
        # logger.info(f"SWAP {swap_check}")
        if swap_check:
            return 1
        else:
            return 0
        
def check_if_last_name_swapped_to_front_creates_match(block_1, block_2):
    name_1 = get_name_from_name_list(block_1)
    if len(name_1) != 2:
        return False
    else:
        name_2 = get_name_from_name_list(block_2)
        if len(name_2)==2:
            if " ".join(name_1) == " ".join(name_2[-1:] + name_2[:-1]):
                return True
            else:
                return False
        else:
            return False
        
def get_name_from_name_list(name_list):
    name = []
    for i in range(0,12,2):
        if name_list[i]:
            name.append(name_list[i][0])
        elif name_list[i+1]:
            name.append(name_list[i+1][0])
        else:
            break
    if name_list[-2]:
        name.append(name_list[-2][0])
    elif name_list[-1]:
        name.append(name_list[-1][0])
    else:
        pass

    return name
        
def match_block_names(block_1_names, block_1_initials, block_2_names, block_2_initials):
    if block_1_names and block_2_names:
        if any(x in block_1_names for x in block_2_names):
            return True, True
        else:
            return False, True
    elif block_1_names and not block_2_names:
        if block_2_initials:
            if any(x in block_1_initials for x in block_2_initials):
                return True, True
            else:
                return False, True
        else:
            return True, True
    elif not block_1_names and block_2_names:
        if block_1_initials:
            if any(x in block_1_initials for x in block_2_initials):
                return True, True
            else:
                return False, True
        else:
            return True, True
    elif block_1_initials and block_2_initials:
        if any(x in block_1_initials for x in block_2_initials):
            return True, True
        else:
            return False, True
    else:
        return True, False
    
def transform_name_for_search(name):
    name = unidecode(unicodedata.normalize('NFKC', name))
    name = name.lower().replace(" ", " ").replace(".", " ").replace(",", " ").replace("|", " ").replace(")", "").replace("(", "")\
        .replace("-", "").replace("&", "").replace("$", "").replace("#", "").replace("@", "").replace("%", "").replace("0", "") \
        .replace("1", "").replace("2", "").replace("3", "").replace("4", "").replace("5", "").replace("6", "").replace("7", "") \
        .replace("8", "").replace("9", "").replace("*", "").replace("^", "").replace("{", "").replace("}", "").replace("+", "") \
        .replace("=", "").replace("_", "").replace("~", "").replace("`", "").replace("[", "").replace("]", "").replace("\\", "") \
        .replace("<", "").replace(">", "").replace("?", "").replace("/", "").replace(";", "").replace(":", "").replace("\'", "") \
        .replace("\"", "")
    name = " ".join(name.split())
    return name

def get_name_match_list_reg(name):
    name_split_1 = name.replace("-", "").split()
    name_split_2 = ""
    if "-" in name:
        name_split_2 = name.replace("-", " ").split()

    fn = []
    fni = []
    
    m1 = []
    m1i = []
    m2 = []
    m2i = []
    m3 = []
    m3i = []
    m4 = []
    m4i = []
    m5 = []
    m5i = []

    ln = []
    lni = []
    for name_split in [name_split_1, name_split_2]:
        if len(name_split) == 0:
            pass
        elif len(name_split) == 1:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[0]) > 1:
                ln.append(name_split[0])
                lni.append(name_split[0][0])
            else:
                lni.append(name_split[0][0])
            
        elif len(name_split) == 2:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 3:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 4:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 5:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])
                
            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 6:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
            
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        elif len(name_split) == 7:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
            
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            if len(name_split[5]) > 1:
                m5.append(name_split[5])
                m5i.append(name_split[5][0])
            else:
                m5i.append(name_split[5][0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
        else:
            if len(name_split[0]) > 1:
                fn.append(name_split[0])
                fni.append(name_split[0][0])
            else:
                fni.append(name_split[0][0])

            if len(name_split[1]) > 1:
                m1.append(name_split[1])
                m1i.append(name_split[1][0])
            else:
                m1i.append(name_split[1][0])

            if len(name_split[2]) > 1:
                m2.append(name_split[2])
                m2i.append(name_split[2][0])
            else:
                m2i.append(name_split[2][0])

            if len(name_split[3]) > 1:
                m3.append(name_split[3])
                m3i.append(name_split[3][0])
            else:
                m3i.append(name_split[3][0])
                
            if len(name_split[4]) > 1:
                m4.append(name_split[4])
                m4i.append(name_split[4][0])
            else:
                m4i.append(name_split[4][0])

            joined_names = " ".join(name_split[5:-1])
            m5.append(joined_names)
            m5i.append(joined_names[0])

            if len(name_split[-1]) > 1:
                ln.append(name_split[-1])
                lni.append(name_split[-1][0])
            else:
                lni.append(name_split[-1][0])
            

    return [list(set(x)) for x in [fn,fni,m1,m1i,m2,m2i,m3,m3i,m4,m4i,m5,m5i,ln,lni]]

def transform_author_name(author):
    if author.startswith("None "):
        author = author.replace("None ", "")
    elif author.startswith("Array "):
        author = author.replace("Array ", "")

    author = unicodedata.normalize('NFKC', author)
    
    author_name = HumanName(" ".join(author.split()))

    if (author_name.title == 'Dr.') | (author_name.title == ''):
        temp_new_author_name = f"{author_name.first} {author_name.middle} {author_name.last}"
    else:
        temp_new_author_name = f"{author_name.title} {author_name.first} {author_name.middle} {author_name.last}"

    new_author_name = " ".join(temp_new_author_name.split())

    author_names = new_author_name.split(" ")
    
    if (author_name.title != '') : 
        final_author_name = new_author_name
    else:
        if len(author_names) == 1:
            final_author_name = new_author_name
        elif len(author_names) == 2:
            if (len(author_names[1]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 3:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        elif len(author_names) == 4:
            if (len(author_names[1]) == 1) & (len(author_names[2]) == 1) & (len(author_names[3]) == 1) & (len(author_names[0]) > 3):
                final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
            elif (len(author_names[1]) == 2) & (len(author_names[2]) == 2) & (len(author_names[3]) == 2) & (len(author_names[0]) > 3):
                if (author_names[1][1]==".") & (author_names[2][1]==".") & (author_names[3][1]=="."):
                    final_author_name = f"{author_names[1]} {author_names[2]} {author_names[3]} {author_names[0]}"
                else:
                    final_author_name = new_author_name
            else:
                final_author_name = new_author_name
        else:
            final_author_name = new_author_name
    return final_author_name

def quick_string_check(raw_string):
    # string should only include the letter 'a', 'w', numbers, and commas
    if raw_string.strip().replace(" ", ""):
        if all([x in ['a','w',','] or x.isdigit() for x in raw_string.lower().replace(" ", "").strip()]):
            return True
    return False

def check_author_id(author_id_string):
    author_id = 0
    if quick_string_check(author_id_string):
        if author_id_string.strip().lower()[0]=='a':
            author_id = int(''.join(filter(str.isdigit, author_id_string)))
            if author_id < 5000000000:
                author_id = 0
            elif author_id > 6000000000:
                author_id = 0
            else:
                conn = connect_to_db()
                cur = conn.cursor()

                cur.execute("select author_id from mid.author where author_id = %s", (author_id,))
                if cur.fetchone() is None:
                    author_id = 0
                
                cur.close()
                conn.close()
            
    return author_id

def check_work_id(work_id_string):
    work_id = 0
    if quick_string_check(work_id_string):
        if work_id_string.strip().lower()[0]=='w':
            work_id = int(''.join(filter(str.isdigit, work_id_string)))
            if work_id < 0:
                work_id = 0
            else:
                conn = connect_to_db()
                cur = conn.cursor()

                cur.execute("select paper_id from mid.work where paper_id = %s", (work_id,))
                if cur.fetchone() is None:
                    work_id = 0

                cur.close()
                conn.close()
            
    return work_id

def check_work_id_and_get_seq_no(work_id_string, author_id):
    work_author_id = ''
    if quick_string_check(work_id_string):
        if work_id_string.strip().lower()[0]=='w':
            work_id = int(''.join(filter(str.isdigit, work_id_string)))
            if work_id < 0:
                work_author_id = '0'
            else:
                conn = connect_to_db()
                cur = conn.cursor()

                cur.execute("select author_sequence_number from mid.affiliation where paper_id = %s and author_id = %s", 
                            (work_id, author_id))
                seq_no = cur.fetchone()
                if seq_no is None:
                    work_author_id = ''
                else:
                    work_author_id = str(work_id) + '_' + str(seq_no[0])

                cur.close()
                conn.close()
            
    return work_author_id

def get_list_of_ids(id_list_string):
    # get list of ideas that are separated by commas
    if quick_string_check(id_list_string):
        return [x for x in id_list_string.split(',') if x.strip()]
    else:
        return []
    
def freeze_author_id(author_id, email, session_cur, session_conn):
    # check if author_id is already frozen
    session_cur.execute("select author_id from authorships.author_freeze where author_id = %s", (author_id,))
    if session_cur.fetchone() is None:
        rand_int = random.randint(1, 39)
        session_cur.execute("insert into authorships.author_freeze (author_id, freeze_reason, freeze_requester, freeze_date, partition_col) values (%s, 'profile_request_form', %s, now(), %s)", (author_id, email, rand_int))
        session_conn.commit()

def get_best_authorship(author_id, work_id, session_conn, session_cur):
    session_cur.execute("select display_name from mid.author where author_id = %s", (author_id,))
    author_name = session_cur.fetchone()[0]
    logger.info("Matching authorships to: ", author_name)

    author_name_blocked = get_name_match_list_reg(transform_name_for_search(transform_author_name(author_name)))

    session_cur.execute("select distinct author_id, author_sequence_number, original_author from mid.affiliation where paper_id = %s", (work_id,))
    authorships = session_cur.fetchall()

    match = []
    for authorship in authorships:
        authorship_name_blocked = get_name_match_list_reg(transform_name_for_search(transform_author_name(authorship[2])))
        block_check = check_block_vs_block_reg(author_name_blocked, authorship_name_blocked)
        if block_check:
            match.append(authorship)
    
    if len(match) == 1:
        if match[0][0] == author_id:
            return -2
        else:
            return match[0][1]
    else:
        return -1
    
def get_orcid_for_author_id(author_id):
    conn = connect_to_db()
    cur = conn.cursor()
    
    try:
        cur.execute("select orcid from mid.author_orcid where author_id = %s", (author_id,))
        orcid = cur.fetchone()
        if not orcid:
            orcid = ""
        else:
            orcid = orcid[0]
    except:
        conn.rollback
        orcid = ""
    cur.close()
    conn.close()
    return orcid

def merge_author_profiles(curation_data, sheet_instance):
    conn = connect_to_db()
    cur = conn.cursor()

    for i, row in curation_data.iterrows():

        # check author id
        author_id = check_author_id(row['author_id'])
        if author_id == 0:
            _ = sheet_instance.update([["no", f"Invalid author id: {row['author_id']}"]], 
                                      'L'+str(row.request_row_num+2))
            continue
        else:
            # get ORCID if available
            main_orcid = get_orcid_for_author_id(author_id)

            # try to get author ids to merge into
            author_ids_to_merge = get_list_of_ids(row['author_ids_to_merge'])
            if author_ids_to_merge:
                author_ids_bad_merge = []
                for author_id_to_merge in author_ids_to_merge:
                    merge_orcid = get_orcid_for_author_id(author_id_to_merge)

                    author_id_to_merge = check_author_id(author_id_to_merge)
                    if author_id_to_merge == 0:
                        logger.info(f'Invalid author id to merge: {author_id_to_merge}')
                        author_ids_bad_merge.append(author_id_to_merge)
                        continue
                    elif author_id == author_id_to_merge:
                        logger.info(f'Author id and author id to merge are the same: {author_id}')
                        author_ids_bad_merge.append(author_id_to_merge)
                        continue
                    elif (main_orcid != merge_orcid) and (main_orcid != '') and (merge_orcid != ''):
                        logger.info(f'ORCID for author id {author_id} does not match ORCID for author id to merge {author_id_to_merge}')
                        author_ids_bad_merge.append(author_id_to_merge)
                        continue
                    else:
                        try:
                            logger.info(f'Merging author {author_id_to_merge} into author {author_id}')
                            cur.execute("insert into authorships.author_id_merges (merge_from_id, merge_to_id, request_type, request_date) values (%s, %s, %s, now())", 
                                        (author_id_to_merge, author_id, "user_curation"))
                            cur.execute("update mid.author set merge_into_id = %s, merge_into_date = now() where author_id = %s", (author_id, author_id_to_merge))
                            conn.commit()
                        except:
                            conn.rollback
                            author_ids_bad_merge.append(author_id_to_merge)

            if author_ids_bad_merge:
                _ = sheet_instance.update([["no", f"Invalid author ids to merge: {''.join(str(author_ids_bad_merge))}"]], 
                                          'L'+str(row.request_row_num+2))
            else:
                _ = sheet_instance.update([["yes", ""]], 
                                          'L'+str(row.request_row_num+2))
                
                _ = freeze_author_id(author_id, row.email, cur, conn)

    cur.close()
    conn.close()

def remove_works_from_profile(curation_data, sheet_instance):
    conn = connect_to_db()
    cur = conn.cursor()

    for i, row in curation_data.iterrows():

        # check author id
        author_id = check_author_id(row['author_id'])
        if author_id == 0:
            _ = sheet_instance.update([["no", f"Invalid author id: {row['author_id']}"]], 
                                      'L'+str(row.request_row_num+2))
            continue
        else:

            # try to get work ids to remove
            work_ids_to_remove = get_list_of_ids(row['works_to_remove'])
            if work_ids_to_remove:
                work_ids_bad_remove = []
                if len(work_ids_to_remove) == 1:
                    work_id_to_remove = work_ids_to_remove[0]
                    work_id_to_remove = check_work_id(work_id_to_remove)
                    if work_id_to_remove == 0:
                        logger.info(f'Invalid work id to remove: {work_id_to_remove}')
                        work_ids_bad_remove.append(work_id_to_remove)
                        continue
                    else:
                        try:
                            random_int = random.randint(1, 39)
                            logger.info(f'Removing work {work_id_to_remove} from author {author_id}')
                            cur.execute("insert into authorships.remove_works (paper_id, author_id, request_type, request_date, partition_col) values (%s, %s, 'user_curation', now(), %s)", 
                                        (work_id_to_remove, author_id, random_int))
                            cur.execute("update mid.affiliation set updated_date = now(), author_id = null where author_id = %s and paper_id = %s", 
                                        (author_id, work_id_to_remove))
                            conn.commit()
                        except:
                            conn.rollback
                            work_ids_bad_remove.append(work_id_to_remove)
                else:
                    final_work_ids_to_remove = []
                    for work_id_to_remove in work_ids_to_remove:
                        work_id_to_remove = check_work_id_and_get_seq_no(work_id_to_remove, author_id)
                        if work_id_to_remove == '0':
                            logger.info(f'Invalid work author id to remove: {work_id_to_remove}')
                            work_ids_bad_remove.append(work_id_to_remove)
                            continue
                        else:
                            final_work_ids_to_remove.append(work_id_to_remove)

                    if final_work_ids_to_remove:
                        logger.info("Moving all works to a new cluster")
                        cluster_label = f"{final_work_ids_to_remove[0]}_user_curation_{datetime.now().strftime('%Y%m%d%H')}"
                        for work_id_to_remove in final_work_ids_to_remove:
                            try:
                                random_int = random.randint(1, 39)
                                logger.info(f'- Removing work {work_id_to_remove} from author {author_id}')
                                cur.execute("insert into authorships.overmerged_authors (work_author_id_for_cluster, all_works_To_cluster, request_type, request_date, partition_col) values (%s, %s, 'user_curation', now(), %s)", 
                                            (cluster_label, work_id_to_remove, random_int))
                                conn.commit()
                            except:
                                conn.rollback
                                work_ids_bad_remove.append(work_id_to_remove)
                    else:
                        work_ids_bad_remove = work_ids_to_remove.copy()

            if work_ids_bad_remove:
                _ = sheet_instance.update([["no", f"Invalid work ids to remove: {''.join(str(work_ids_bad_remove))}"]], 
                                          'L'+str(row.request_row_num+2))
            else:
                _ = sheet_instance.update([["yes", ""]], 
                                          'L'+str(row.request_row_num+2))
                
                _ = freeze_author_id(author_id, row.email, cur, conn)

    cur.close()
    conn.close()

def add_works_to_profile(curation_data, sheet_instance):
    conn = connect_to_db()
    cur = conn.cursor()

    for i, row in curation_data.iterrows():

        # check author id
        author_id = check_author_id(row['author_id'])
        if author_id == 0:
            _ = sheet_instance.update([["no", f"Invalid author id: {row['author_id']}"]], 
                                      'L'+str(row.request_row_num+2))
            continue
        else:
            # try to get work ids to add
            work_ids_to_add = get_list_of_ids(row['works_to_add'])
            if work_ids_to_add:
                work_ids_bad_add = []
                for work_id_to_add in work_ids_to_add:
                    work_id_to_add = check_work_id(work_id_to_add)
                    if work_id_to_add == 0:
                        logger.info(f'Invalid work id to add: {work_id_to_add}')
                        work_ids_bad_add.append(work_id_to_add)
                        continue
                    else:
                        author_sequence_number = get_best_authorship(author_id, work_id_to_add, conn, cur)
                        if author_sequence_number == -1:
                            logger.info(f'Could not find a good match for work id {work_id_to_add}')
                            work_ids_bad_add.append(work_id_to_add)
                            continue
                        elif author_sequence_number == -2:
                            logger.info(f'Author ID is already matched to this work')
                            work_ids_bad_add.append(work_id_to_add)
                            continue
                        else:
                            try:
                                work_author_id = f"{work_id_to_add}_{author_sequence_number}"
                                random_int = random.randint(1, 39)
                                logger.info(f'Adding work {work_id_to_add} to author {author_id}')
                                cur.execute("insert into authorships.add_works (work_author_id, new_author_id, request_type, request_date, partition_col) values (%s, %s, 'user_curation', now(), %s)", 
                                            (work_author_id, author_id, random_int))
                                conn.commit()
                            except:
                                conn.rollback
                                work_ids_bad_add.append(work_id_to_add)
                if work_ids_bad_add:
                    _ = sheet_instance.update([["no", f"Invalid work ids to add: {''.join(str(work_ids_bad_add))}"]], 
                                              'L'+str(row.request_row_num+2))
                else:
                    _ = sheet_instance.update([["yes", ""]], 
                                              'L'+str(row.request_row_num+2))
                    
                    _ = freeze_author_id(author_id, row.email, cur, conn)
            else:
                _ = sheet_instance.update([["no", "Could not find works to connect"]], 
                                          'L'+str(row.request_row_num+2))
                
def change_display_name(curation_data, sheet_instance):
    conn = connect_to_db()
    cur = conn.cursor()

    for i, row in curation_data.iterrows():
        # check author id
        author_id = check_author_id(row['author_id'])
        if author_id == 0:
            _ = sheet_instance.update([["no", f"Invalid author id: {row['author_id']}"]], 
                                      'L'+str(row.request_row_num+2))
            continue
        else:
            try:
                cur.execute("select author_id from authorships.change_author_display_name where author_id = %s", (author_id,))
                author_id_found = cur.fetchone()
                if author_id_found:
                    # update table for new display name
                    logger.info(f'Updating display name for author {author_id}: {row["display_name_requested"]}')
                    cur.execute("update authorships.change_author_display_name set new_display_name = %s, request_date = now() where author_id = %s", 
                                (row['display_name_requested'], author_id))
                    conn.commit()
                else:
                    try:
                        cur.execute("insert into authorships.change_author_display_name (author_id, new_display_name, requested_by, request_date) values (%s, %s, 'user_curation', now())", 
                                    (author_id, row['display_name_requested']))
                        conn.commit()
                        logger.info(f'Adding display name for author {author_id}: {row["display_name_requested"]}')
                    except:
                        conn.rollback
                        _ = sheet_instance.update([["no", "Error updating display name"]], 
                                          'L'+str(row.request_row_num+2))
                        continue
                _ = sheet_instance.update([["yes", ""]], 
                                    'L'+str(row.request_row_num+2))
                _ = freeze_author_id(author_id, row.email, cur, conn)
            except:
                conn.rollback()
                _ = sheet_instance.update([["no", "Error updating display name"]], 
                                          'L'+str(row.request_row_num+2))

    cur.close()
    conn.close()

def main():
    # get the instance of the Spreadsheet
    sheet = client.open('Fix an OpenAlex author profile (Responses)')

    # get the first sheet of the Spreadsheet
    sheet_instance = sheet.get_worksheet(0)

    # get all the records of the data
    records_data = sheet_instance.get_all_records()

    # get the data as DataFrame
    records_data = pd.DataFrame(records_data).reset_index()
    records_data.columns = ['request_row_num','request_time','email','own_profile', 'author_id','workflow_type',
                            'display_name_requested','author_ids_to_merge','works_to_remove','works_to_add',
                            'openalex_accept','notes','ingested','ingest_notes']

    # check for new data
    if records_data[(records_data['ingested']!='yes') & 
                    (records_data['openalex_accept']=='yes') & 
                    (records_data['workflow_type'].isin(workflows_allowed))].shape[0]>0:
        new_data = records_data[(records_data['ingested']!='yes') & 
                                (records_data['openalex_accept']=='yes') & 
                                (records_data['workflow_type'].isin(workflows_allowed))].copy()

        # Merge profiles
        if new_data[new_data['workflow_type']=='Merge another profile into mine'].shape[0]>0:
            logger.info("---- New profile merge requests: ", new_data[new_data['workflow_type']=='Merge another profile into mine'].shape[0])
            _ = merge_author_profiles(new_data[new_data['workflow_type']=='Merge another profile into mine'].copy(), sheet_instance)
        else:
            logger.info("---- No new profile merge requests")

        # Remove works
        if new_data[new_data['workflow_type']=='Remove works from my profile'].shape[0]>0:
            logger.info("---- New works removal requests: ", new_data[new_data['workflow_type']=='Remove works from my profile'].shape[0])
            _ = remove_works_from_profile(new_data[new_data['workflow_type']=='Remove works from my profile'].copy(), sheet_instance)
        else:
            logger.info("---- No new remove works requests")

        # Add works
        if new_data[new_data['workflow_type']=='Add works to my profile'].shape[0]>0:
            logger.info("---- New works addition requests: ", new_data[new_data['workflow_type']=='Add works to my profile'].shape[0])
            _ = add_works_to_profile(new_data[new_data['workflow_type']=='Add works to my profile'].copy(), sheet_instance)
        else:
            logger.info("---- No new add works requests")

        # Change display name
        if new_data[new_data['workflow_type']=='Change the display name'].shape[0]>0:
            logger.info("---- New display name change requests: ", new_data[new_data['workflow_type']=='Change the display name'].shape[0])
            _ = change_display_name(new_data[new_data['workflow_type']=='Change the display name'].copy(), sheet_instance)
        else:
            logger.info("---- No new display name change requests")
    else:
        logger.info("No new data to process")

if __name__ == '__main__':
    main()