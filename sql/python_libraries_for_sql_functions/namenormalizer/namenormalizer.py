from japanese_last_name import gJapaneseLastName
from chinese_last_name import gChineseLastName
from korean_last_name import gKoreanLastName
from unicode_mapping import gUnicodeMapping

def popularity_as_chinese_firstname(word):
    return float(gChineseFirstName.get(word.lower(), '0.0'))

def popularity_as_japanese_lastname(word):
    return float(gJapaneseLastName.get(word.lower(), '0.0'))

def popularity_as_chinese_lastname(word):
    return float(gChineseLastName.get(word.lower(), '0.0'))

def popularity_as_korean_lastname(word):
    return float(gKoreanLastName.get(word.lower(), '0.0'))

def unicode_map(uLetter):
    sLetter = uLetter.encode("utf-8")
    if sLetter in gUnicodeMapping:
        return gUnicodeMapping[sLetter]['translated']
    return sLetter