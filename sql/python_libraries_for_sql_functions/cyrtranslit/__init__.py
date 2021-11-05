# -*- coding: utf-8 -*-
from .mapping import TRANSLIT_DICT
import sys

def __encode_utf8(_string):
    if sys.version_info < (3, 0):
        return _string.encode('utf-8')
    else:
        return _string

def __decode_utf8(_string):
    if sys.version_info < (3, 0):
        return _string.decode('utf-8')
    else:
        return _string

def to_latin(string_to_transliterate, lang_code='sr'):
    ''' Transliterate cyrillic string of characters to latin string of characters.
    :param string_to_transliterate: The cyrillic string to transliterate into latin characters.
    :param lang_code: Indicates the cyrillic language code we are translating from. Defaults to Serbian (sr).
    :return: A string of latin characters transliterated from the given cyrillic string.
    '''

    # First check if we support the cyrillic alphabet we want to transliterate to latin.
    if lang_code.lower() not in TRANSLIT_DICT:
        # If we don't support it, then just return the original string.
        return string_to_transliterate

    # If we do support it, check if the implementation is not missing before proceeding.
    elif not TRANSLIT_DICT[lang_code.lower()]['tolatin']:
        return string_to_transliterate

    # Everything checks out, proceed with transliteration.
    else:

        # Get the character per character transliteration dictionary
        transliteration_dict = TRANSLIT_DICT[lang_code.lower()]['tolatin']

        # Initialize the output latin string variable
        latinized_str = ''

        # Transliterate by traversing the input string character by character.
        string_to_transliterate = __decode_utf8(string_to_transliterate)


        for c in string_to_transliterate:

            # If character is in dictionary, it means it's a cyrillic so let's transliterate that character.
            if c in transliteration_dict:
                # Transliterate current character.
                latinized_str += transliteration_dict[c]

            # If character is not in character transliteration dictionary,
            # it is most likely a number or a special character so just keep it.
            else:
                latinized_str += c

        # Return the transliterated string.
        return __encode_utf8(latinized_str)


def to_cyrillic(string_to_transliterate, lang_code='sr'):
    ''' Transliterate latin string of characters to cyrillic string of characters.
    :param string_to_transliterate: The latin string to transliterate into cyrillic characters.
    :param lang_code: Indicates the cyrillic language code we are translating to. Defaults to Serbian (sr).
    :return: A string of cyrillic characters transliterated from the given latin string.
    '''

    # First check if we support the cyrillic alphabet we want to transliterate to latin.
    if lang_code.lower() not in TRANSLIT_DICT:
        # If we don't support it, then just return the original string.
        return string_to_transliterate

    # If we do support it, check if the implementation is not missing before proceeding.
    elif not TRANSLIT_DICT[lang_code.lower()]['tocyrillic']:
        return string_to_transliterate

    else:
        # Get the character per character transliteration dictionary
        transliteration_dict = TRANSLIT_DICT[lang_code.lower()]['tocyrillic']

        # Initialize the output cyrillic string variable
        cyrillic_str = ''

        string_to_transliterate = __decode_utf8(string_to_transliterate)

        # Transliterate by traversing the inputted string character by character.
        length_of_string_to_transliterate = len(string_to_transliterate)
        index = 0

        while index < length_of_string_to_transliterate:
            # Grab a character from the string at the current index
            c = string_to_transliterate[index]

            # Watch out for Lj and lj. Don't want to interpret Lj/lj as L/l and j.
            # Watch out for Nj and nj. Don't want to interpret Nj/nj as N/n and j.
            # Watch out for Dž and and dž. Don't want to interpret Dž/dž as D/d and j.
            c_plus_1 = u''
            if index != length_of_string_to_transliterate - 1:
                c_plus_1 = string_to_transliterate[index + 1]

            if ((c == u'L' or c == u'l') and c_plus_1 == u'j') or \
               ((c == u'N' or c == u'n') and c_plus_1 == u'j') or \
               ((c == u'D' or c == u'd') and c_plus_1 == u'ž') or \
               (lang_code == 'mk' and (c == u'D' or c == u'd') and c_plus_1 == u'z') or \
               (lang_code == 'bg' and (
                   (c in u'Zz' and c_plus_1 in u'Hh') or # Zh, zh
                   (c in u'Tt' and c_plus_1 in u'Ss') or # Ts, ts
                   (c in u'Ss' and c_plus_1 in u'Hh') or # Sh, sh (and also covers Sht, sht)
                   (c in u'Cc' and c_plus_1 in u'Hh') or # Ch, ch
                   (c in u'Yy' and c_plus_1 in u'Uu') or # Yu, yu
                   (c in u'Yy' and c_plus_1 in u'Aa') # Ya, ya
                )) or \
               (lang_code == 'ru' and (
                    (c in u'Cc' and c_plus_1 in u'Hh')   or  # c, ch
                    (c in u'Ee' and c_plus_1 in u'Hh')   or  # eh
                    (c == u'i'  and c_plus_1 == u'y' and
                     string_to_transliterate[index + 2:index + 3] not in u'aou') or  # iy[^AaOoUu]
                    (c in u'Jj' and c_plus_1 in u'UuAaEe') or  # j, ju, ja, je
                    (c in u'Ss' and c_plus_1 in u'HhZz') or  # s, sh, sz
                    (c in u'Yy' and c_plus_1 in u'AaOoUu') or  # y, ya, yo, yu
                    (c in u'Zz' and c_plus_1 in u'Hh')       # z, zh
               )) or \
               (lang_code == 'ua' and (
                    (c in u'Jj' and c_plus_1 in u'eau') or #je, ja, ju
                    (c in u'Šš' and c_plus_1 in u'č')      #šč
               )):

                index += 1
                c += c_plus_1

                # In Bulgarian, the letter "щ" is represented by three latin letters: "sht", 
                # so we need this logic to support the third latin letter
                if (lang_code == 'bg' and (c == 'sh' or c == 'Sh' or c == 'SH') and string_to_transliterate[index + 1] in u'Tt'):
                    index += 1
                    c += string_to_transliterate[index]


            # If character is in dictionary, it means it's a cyrillic so let's transliterate that character.
            if c in transliteration_dict:
                # ay, ey, iy, oy, uy
                if lang_code == 'ru' and c in u'Yy' and \
                        cyrillic_str and cyrillic_str[-1].lower() in u"аеиоуэя":
                    cyrillic_str += u"й" if c == u'y' else u"Й"
                else:
                    # Transliterate current character.
                    cyrillic_str += transliteration_dict[c]

            # If character is not in character transliteration dictionary,
            # it is most likely a number or a special character so just keep it.
            else:
                cyrillic_str += c

            index += 1

        return __encode_utf8(cyrillic_str)

      
def supported():
    ''' Returns list of supported languages, sorted alphabetically.
    :return:
    '''
    return sorted(TRANSLIT_DICT.keys())
