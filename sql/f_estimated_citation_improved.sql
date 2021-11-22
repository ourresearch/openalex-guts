-- get estimated_citation based on Redshift ML model's function

CREATE OR REPLACE FUNCTION utils.f_estimated_citation_improved ("citation_count" bigint, "year" int4, "reference_count" bigint) 
RETURNS int4 
STABLE AS $$ 

	SELECT CASE WHEN $1 = 0 THEN 0
    ELSE INT4 (ROUND( f_estimated_citation_multipler( $1, (date_part_year(trunc(getdate())) - $2), $3 ) * $1 ) )
  END

$$ LANGUAGE SQL;

-- select citation_count, estimated_citation, f_estimated_citation(citation_count, year, reference_count) from papers where citation_count != 0 offset 1000000 limit 10000;
-- select avg(abs(f_estimated_citation(citation_count, year, reference_count) * 100.0 / estimated_citation - 100.0)) from (select * from papers where citation_count != 0 offset 1000000 limit 1000000);