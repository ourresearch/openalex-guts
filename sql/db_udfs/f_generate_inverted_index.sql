--  generate inverted index from abstract string

create or replace function
util.f_generate_inverted_index(abstract_string character varying(65535))
 RETURNS character varying(65535)
STABLE
AS $$
	import re
	import json
	from collections import OrderedDict

	# remove jat tags and unnecessary tags and problematic white space
	abstract_string = re.sub('\b', ' ', abstract_string)
	abstract_string = re.sub('\n', ' ', abstract_string)
	abstract_string = re.sub('\t', ' ', abstract_string)
	abstract_string = re.sub('<jats:[^<]+>', ' ', abstract_string)
	abstract_string = re.sub('</jats:[^<]+>', ' ', abstract_string)
	abstract_string = re.sub('<p>', ' ', abstract_string)
	abstract_string = re.sub('</p>', ' ', abstract_string)
	abstract_string = " ".join(re.split("\s+", abstract_string))

	# build inverted index
	invertedIndex = OrderedDict()
	words = abstract_string.split()
	for i in range(len(words)):
		if words[i] not in invertedIndex:
			invertedIndex[words[i]] = []
		invertedIndex[words[i]].append(i)
	result = {
		'IndexLength': len(words),
		'InvertedIndex': invertedIndex,
	}
	
	return json.dumps(result, ensure_ascii=False)

$$LANGUAGE plpythonu;


-- select abstract, f_generate_inverted_index_v1(abstract) from legacy.crossref_main_works where abstract != '' limit 100;
-- select abstract, f_generate_inverted_index_v1(abstract) from legacy.pubmed_main_works where abstract != '' limit 100;