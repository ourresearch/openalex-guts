-- functionality of normalize_title in utils.py

create library l_unidecode
language plpythonu
from 's3://openalex-sandbox/unidecode.zip'
credentials 'CREDS HERE'
region as 'us-east-1';

create or replace library l_cyrtranslit
language plpythonu
from 's3://openalex-sandbox/cyrtranslit.zip'
credentials 'CREDS HERE'
region as 'us-east-1';

create or replace function
f_mag_normalize_string(input_string character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
    from unidecode import unidecode
    from cyrtranslit import to_latin
    import re
    import string

    response = input_string
    simplified_decode = response
    if (not response) or len(response) < 3:
        simplified_decode = response.decode('utf-8')
        response = unidecode(response).encode('ascii', 'ignore')

    if len(response) < 2:
        as_latin = to_latin(simplified_decode)
        response = u" ".join([unidecode(piece.decode("utf-8")) for piece in as_latin.split(" ")])

    response = response.lower()

    # replace punctuation with a space
    response = re.sub(u"[^a-zA-Z0-9,.;@#?!&$+\ *-]", u"", response)
    response = re.sub(u"[^a-zA-Z0-9\? ]", u" ", response)

    # remove everything else
    # response = u"".join(e for e in response if (e.isalpha() or e==u" "))

    # reduce consecutive spaces
    response = re.sub(r'\s+', u' ', response)

    return response

$$LANGUAGE plpythonu;


