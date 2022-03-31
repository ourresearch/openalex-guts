-- functionality of f_unpaywall_normalize_title in utils.py
create or replace function
util.f_unpaywall_normalize_title(title character varying(65535))
RETURNS character varying(65535)
STABLE
as $$
    import re

    try:
        response = title

        if not response:
            return u""

        # just first n characters
        response = response[0:500]

        # lowercase
        response = response.lower()

        # has to be before remove_punctuation
        # the kind in titles are simple <i> etc, so this is simple
        response = re.sub(u'<.*?>', u'', response)

        # remove articles and common prepositions
        response = re.sub(r"\b(the|a|an|of|to|in|for|on|by|with|at|from)\b", "", response)

        # remove everything except alphas
        response = "".join(e for e in response if (e.isalpha()))
    except UnicodeDecodeError:
        return None

    return response


$$LANGUAGE plpythonu;


-- select paper_id, original_title, util.f_unpaywall_normalize_title(original_title) from mid.work limit 1000

