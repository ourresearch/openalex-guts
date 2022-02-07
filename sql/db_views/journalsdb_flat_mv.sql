CREATE materialized VIEW mid.journalsdb_flat_mv distkey (issn) sortkey (issn) AS (
SELECT jdb.issn_l, jdb.publisher, jdb.title,
    'true' = json_extract_path_text(jdb.open_access, 'is_in_doaj') as is_in_doaj,
    'true' = json_extract_path_text(jdb.open_access, 'is_gold_journal') as is_gold_journal,
    jdb.issns as issns_string,
    json_extract_array_element_text(jdb.issns::text, seq.i) AS issn
    FROM ins.journalsdb_raw jdb
  CROSS JOIN (((((((((( SELECT 0 AS i
UNION ALL
         SELECT 1)
UNION ALL
         SELECT 2)
UNION ALL
         SELECT 3)
UNION ALL
         SELECT 4)
UNION ALL
         SELECT 5)
UNION ALL
         SELECT 6)
UNION ALL
         SELECT 7)
UNION ALL
         SELECT 8)
UNION ALL
         SELECT 9)
UNION ALL
         SELECT 10) seq
  WHERE seq.i < json_array_length(jdb.issns::text)
  );


