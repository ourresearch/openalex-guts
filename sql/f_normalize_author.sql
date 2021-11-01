
create or replace library l_unidecode
language plpythonu 
from 's3://openalex-sandbox/unidecode.zip'
credentials 'CREDS HERE'
region as 'us-east-1';  


create or replace library l_nameparser
language plpythonu
from 's3://openalex-sandbox/nameparser.zip'
credentials 'CREDS HERE'
region as 'us-east-1';


create or replace library l_cyrtranslit
language plpythonu
from 's3://openalex-sandbox/cyrtranslit.zip'
credentials 'CREDS HERE'
region as 'us-east-1';


create or replace function
f_normalize_author(original_name character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
    from unidecode import unidecode
    from cyrtranslit import to_latin
    from nameparser import HumanName


    simplified = u"".join(e for e in original_name if (e.isalpha() or e==" "))
    simplified_decode = simplified
    if (not simplified) or len(simplified) < 3:
        simplified_decode = original_name.decode('utf-8')
        simplified = unidecode(simplified_decode).encode('ascii', 'ignore')

    parsed_name = HumanName(simplified)

    # check to see if was LASTNAME FIRSTINITIAL and if so swap
    if (len(parsed_name.last) == 1) and (len(parsed_name.first) > 1):
        split_simplified = simplified.rsplit(" ", 1)
        if len(split_simplified) == 2:
            flipped = u"{} {}".format(split_simplified[1], split_simplified[0])
            parsed_name = HumanName(flipped)

    if len(parsed_name.last) < 1:
        as_latin = to_latin(original_name)
        decoded = u" ".join([unidecode(piece.decode("utf-8")) for piece in as_latin.split(" ")])
        parsed_name = HumanName(decoded)

    first = parsed_name.first
    last = parsed_name.last

    first_initial = u""
    if first:
        first_initial = first[0]

    response = u"{}\t{}".format(first_initial, last)
    response = u"".join(e for e in response if (e.isalpha() or e==u"\t"))
    response = response.lower()
    return response

$$LANGUAGE plpythonu;



-- select original_author, f_normalize_author(original_author) as parsed_name from mag_main_paper_author_affiliations limit 1000