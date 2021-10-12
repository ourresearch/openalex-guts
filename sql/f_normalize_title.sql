-- functionality of normalize_title in utils.py

create library l_unidecode
language plpythonu
from 's3://openalex-sandbox/unidecode.zip'
credentials 'CREDS HERE'
region as 'us-east-1';

create or replace function
f_normalize_title(title character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
    import unidecode
    import re

    response = title

    if not response:
        return u""

    # just first n characters
    response = response[0:500]

    response = unidecode.unidecode(response.decode('utf-8')).encode('ascii', 'ignore')

    # lowercase
    response = response.lower()

    # has to be before remove_punctuation
    # the kind in titles are simple <i> etc, so this is simple
    response = re.sub(u'<.*?>', u'', response)

    # remove articles and common prepositions
    response = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", u"", response)

    # remove everything except alphas
    response = u"".join(e for e in response if (e.isalpha()))

    return response
$$LANGUAGE plpythonu;

