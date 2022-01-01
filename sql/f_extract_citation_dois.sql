--  generate citations from nested doi json

create or replace function
util.f_extract_citation_dois(citation_json character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
import re
import json

dois = re.findall('"doi": "(10.*?)"', citation_json)
return json.dumps([doi.lower() for doi in dois])

$$LANGUAGE plpythonu;
