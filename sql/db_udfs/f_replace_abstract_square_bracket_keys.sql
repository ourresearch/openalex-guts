--  elasticsearch doesn't like square brackets in keys, which they sometimes are in abstracts, so sub them

create or replace function
util.f_replace_abstract_square_bracket_keys(api_json_with_abstract character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
import json
from collections import OrderedDict

if not api_json_with_abstract:
    return api_json_with_abstract

api_dict = json.loads(api_json_with_abstract, object_pairs_hook=OrderedDict)
if not api_dict:
    return api_json_with_abstract

if not api_dict["abstract_inverted_index"]:
    return api_json_with_abstract

new_abstract_dict = OrderedDict()
for my_key, my_value in api_dict["abstract_inverted_index"].iteritems():
    my_key = my_key.replace("[", "_opensquare_")
    my_key = my_key.replace("]", "_closesquare_")
    new_abstract_dict[my_key] = my_value

api_dict["abstract_inverted_index"] = new_abstract_dict
response_string = json.dumps(api_dict, ensure_ascii=False).encode('utf-8')
if len(response_string) >= 65535:
    api_dict["abstract_inverted_index"] = {}
    response_string = json.dumps(api_dict, ensure_ascii=False).encode('utf-8')
return response_string

$$LANGUAGE plpythonu;

-- takes 6386.9 seconds
# update mid.json_works set json_save_no_square_brackets=f_replace_abstract_square_bracket_keys(json_save) where id=77490