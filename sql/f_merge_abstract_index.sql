--  nest the abstract_inverted_index in the openalex works api response

create or replace function
util.f_merge_abstract_index(api_json character varying(65535), abstract_json character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
import json
from collections import OrderedDict

if not api_json:
    return api_json

api_dict = json.loads(api_json, object_pairs_hook=OrderedDict)
if not api_dict:
    return api_json

if not abstract_json:
    api_dict["abstract_inverted_index"] = {}
    response_string = json.dumps(api_dict, ensure_ascii=False).encode('utf-8')
    return response_string

abstract_dict = json.loads(abstract_json, object_pairs_hook=OrderedDict)
if not abstract_dict:
    api_dict["abstract_inverted_index"] = {}
    response_string = json.dumps(api_dict, ensure_ascii=False).encode('utf-8')
    return response_string

api_dict["abstract_inverted_index"] = abstract_dict["InvertedIndex"]
response_string = json.dumps(api_dict, ensure_ascii=False).encode('utf-8')
if len(response_string) >= 65535:
    api_dict["abstract_inverted_index"] = {}
    response_string = json.dumps(api_dict, ensure_ascii=False).encode('utf-8')
return response_string

$$LANGUAGE plpythonu;


create table mid.json_works_with_abstract distkey (id) sortkey (id, updated) as (
select id, sysdate as updated, util.f_merge_abstract_index(json_save, indexed_abstract) as json_save, version
from mid.json_works_before_abstracts works
left outer join mid.abstract abstract on works.id=abstract.paper_id
)