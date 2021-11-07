create or replace library l_namenormalizer
language plpythonu 
from 's3://openalex-sandbox/namenormalizer.zip'
credentials 'CREDS HERE'
region as 'us-east-1';  

create or replace function
f_normalize_author1(origName character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
    from namenormalizer import *

    tag = 'unknown'
    try:
        origName.decode('ascii')
        tag = 'ascii'
        words = origName.replace("'", '').replace('-', '').replace(',', '').replace('.', '').split()
        for i in reversed(range(len(words))):
            # Upper letter
            if len(words[i]) == 2 and words[i] == words[i].upper():
                words[i] = words[i][0] + ' ' + words[i][1]
            # word in ()
            if len(words[i]) > 2 and words[i][0] == '(' and words[i][-1] == ')':
                del words[i]
        result = ' '.join(words)
        if len(words) == 2:
            # adjust japanese sort
            if len(words[0]) > 1 and popularity_as_japanese_lastname(words[0]) > 0:
                if popularity_as_japanese_lastname(words[0]) > popularity_as_japanese_lastname(words[1]):
                    result = words[1] + ' ' + words[0]
                    tag = 'japanese-english'
            # adjust chinese sort
            elif len(words[0]) > 1 and popularity_as_chinese_lastname(words[0]) > 0:
                if popularity_as_chinese_lastname(words[0]) > popularity_as_chinese_lastname(words[1]):
                    result = words[1] + ' ' + words[0]
                    tag = 'chinese-english'
            # adjust korean sort
            elif len(words[0]) > 1 and popularity_as_korean_lastname(words[0]) > 0:
                if popularity_as_korean_lastname(words[0]) > popularity_as_korean_lastname(words[1]):
                    result = words[1] + ' ' + words[0]
                    tag = 'korean-english'
            # 1 letter in second
            elif len(words[1]) == 1:
                result = words[1] + ' ' + words[0]
                tag = '1-letter-in-second'
            # # 2 letters in second: Ea
            # elif len(words[1]) == 2 and words[1][0] == words[1][0].upper() and words[1][1] == words[1][1].lower():
            #     result = words[1][0] + ' ' + words[1][1] + ' ' + words[0]
            #     tag = '2-letters-in-second'

        elif len(words) == 3:
            # 1 letter in second and third
            if len(words[1]) == 1 and len(words[2]) == 1:
                result = words[1] + ' ' + words[2] + ' ' + words[0]
        result = result.lower()

    except UnicodeDecodeError:
        tag = 'unicode'
        result = origName
        words = result.split()
        for i in range(len(words)):

            # convert unicode
            rawWord = unicode(words[i], 'utf-8')
            convertedWord = ''
            for rLetter in rawWord:
                # if rLetter == u'.':
                #     continue
                cLetter = unicode_map(rLetter)
                if type(cLetter) == 'str':
                    cLetter = unicode(words[i], 'utf-8')
                convertedWord += unicode_map(rLetter)            
            words[i] = convertedWord            
           
            if len(words[i]) == 2 and words[i][1] == '.':
                words[i] = words[i][:-1]
            # elif len(words[i]) == 2 and words[i] == words[i].upper():
            #     words[i] = words[i][0] + ' ' + words[i][1]
            words[i] = words[i].replace('-', '').replace('.', '').strip()
        result = ' '.join(words)
        result = result.lower()

    return result

$$LANGUAGE plpythonu;
