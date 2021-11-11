-- functionality of f_normalize_string in utils.py

create or replace library l_namenormalizer
language plpythonu
from 's3://openalex-sandbox/namenormalizer.zip'
credentials 'CREDS HERE'


create or replace function
util.f_mag_normalize_string(original_string character varying(65535))
RETURNS character varying(65535)
STABLE
as $$
    from namenormalizer import *
    import re
    punctuation = r"!#$%&'()*+,-./:;<=>?@[\]^_`{|}~–" + r'"'  #string.punctuation except for space

    try:
        original_string.decode('ascii')
        result = original_string
        for punc in punctuation:
            result = result.replace(punc, "")
    except UnicodeDecodeError:
        result = original_string
        words = result.split()
        for i in range(len(words)):

            # convert unicode
            rawWord = unicode(words[i], 'utf-8')
            convertedWord = ''
            for rLetter in rawWord:
                cLetter = unicode_map(rLetter)
                if type(cLetter) == 'str':
                    cLetter = unicode(words[i], 'utf-8')
                if cLetter not in punctuation:
                    convertedWord += unicode_map(rLetter)
            words[i] = convertedWord

            words[i] = words[i].replace('-', '').replace('.', '').strip()
        result = ' '.join(words)

    result = result.lower()
    # leave the spaces for mag version
--    result = re.sub("\s*", "", result)

    return result

$$LANGUAGE plpythonu;


-- select original_title, util.f_mag_normalize_string(original_title) from mid.work limit 1000;

