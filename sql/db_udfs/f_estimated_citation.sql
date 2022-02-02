
create or replace function
util.f_estimated_citation(citation_count bigint, publication_date_string character varying(65535), publisher character varying(65535))
 RETURNS bigint
STABLE
AS $$
    import datetime
    import math

    if not publication_date_string:
        return None

    if not citation_count or citation_count == 0:
        return 0

    publishers_with_high_estimate_multiplier = """Society for Neuroscience
                    Association for Computing Machinery
                    Journal of Bone and Joint Surgery
                    American Diabetes Association
                    American Society for Microbiology
                    American Economic Association
                    American Psychological Association
                    EMBO
                    Institute for Operations Research and the Management Sciences
                    Cold Spring Harbor Laboratory
                    The Endocrine Society
                    The Rockefeller University Press
                    American Society for Clinical Investigation
                    The American Association of Immunologists
                    Annual Reviews
                    Ovid Technologies Wolters Kluwer -American Heart Association
                    Proceedings of the National Academy of Sciences""".split()

    d0 = datetime.datetime.strptime(publication_date_string, '%Y-%m-%d')
    d1 = datetime.datetime.now()
    delta_days = (d1 - d0).days
    years_since_publication = max(0.0, float(delta_days)/365)

    is_publisher_in_list = 1.0 if (publisher in publishers_with_high_estimate_multiplier) else 0.0
    coef_citation_count = 1.07156829
    coef_years_since_publication = -0.02960674
    coef_is_publisher_in_list = 0.07476177

    estimate = pow(10,
                        coef_citation_count * math.log10(citation_count+0.1) +
                        coef_years_since_publication * math.log10(years_since_publication+0.1) +
                        coef_is_publisher_in_list * is_publisher_in_list)
    estimate = int(estimate)
    return max(estimate, citation_count)

$$LANGUAGE plpythonu;



--select util.f_estimated_citation(citation_count, publication_date, publisher), estimated_citation, citation_count, * from mid.work limit 1000

