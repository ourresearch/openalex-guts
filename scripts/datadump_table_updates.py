# coding: utf-8
from sqlalchemy import text
from app import db

#  python -m scripts.datadump_table_updates

# date_of_old_release = "20211025"
# date_of_new_release = "20211108"

date_of_old_release = "20211108b"
date_of_new_release = "20211206"


## run this every release
# rename mag legacy.new_ to legacy.date_
if False:
    # get table names
    q = """
     SELECT pg_table_def.tablename
       FROM pg_table_def
      WHERE pg_table_def.schemaname = 'legacy' AND pg_table_def.tablename ilike 'new_%'
      GROUP BY pg_table_def.schemaname, pg_table_def.tablename
      order by pg_table_def.tablename
    """

    rows = db.session.execute(text(q)).fetchall()
    new_mag_table_names = [row[0] for row in rows]

    rename_sql = ""
    for table in new_mag_table_names:
        rename_to = table.replace("new_", f"{date_of_new_release}_")
        rename_sql += f"""alter table legacy.{table} rename to zz{rename_to}; 
        """
    print(rename_sql)
    db.session.execute(rename_sql)
    db.session.commit()

# rename outs to have the date of the previous release
if False:
    #   get table names
    q = """SET enable_case_sensitive_identifier=true; """
    q += """
     SELECT pg_table_def.tablename
       FROM pg_table_def
      WHERE pg_table_def.schemaname = 'outs' 
      and pg_table_def.tablename not ilike 'zz%'
      GROUP BY pg_table_def.schemaname, pg_table_def.tablename
      order by pg_table_def.tablename
    """
    rows = db.session.execute(text(q)).fetchall()
    outs_table_names = [row[0] for row in rows]
    print(outs_table_names)

    rename_sql = """SET enable_case_sensitive_identifier=true; """
    for table in outs_table_names:
        rename_to = f"{date_of_old_release}_{table}"
        rename_sql += f"""alter table outs."{table}" rename to "zz{rename_to}"; 
        """
    print(rename_sql)
    db.session.execute(rename_sql)
    db.session.commit()

## run this every release to recreate the legacy.mag_ tables from the named releases made above
if False:
    #  get table names
    q = f"""
     SELECT pg_table_def.tablename
       FROM pg_table_def
      WHERE pg_table_def.schemaname = 'legacy' AND pg_table_def.tablename ilike 'zz{date_of_new_release}_%'
      GROUP BY pg_table_def.schemaname, pg_table_def.tablename
      order by pg_table_def.tablename
    """
    rows = db.session.execute(text(q)).fetchall()
    mag_table_names = [row[0] for row in rows]

    for table in mag_table_names:
        rename_to = table.replace(f"zz{date_of_new_release}_", "")
        q = f"""drop table legacy.{rename_to}; 
            create table legacy.{rename_to} as (select * from legacy.{table}); """
        print(q)
        db.session.execute(q)
        db.session.commit()


### run this every release to back up the mid tables
if False:
    #  get table names
    q = f"""
     SELECT pg_table_def.tablename
       FROM pg_table_def
      WHERE pg_table_def.schemaname = 'mid'
      GROUP BY pg_table_def.schemaname, pg_table_def.tablename
      order by pg_table_def.tablename
    """
    rows = db.session.execute(text(q)).fetchall()
    mag_table_names = [row[0] for row in rows]

    for table in mag_table_names:
        tables_to_copy = """abstract
            affiliation
            author
            author_orcid
            citation
            concept
            institution
            institution_ror
            journal
            location
            mesh
            work
            work_concept
            work_extra_ids""".split()
        if table in tables_to_copy:
            rename_to = f"zz{date_of_old_release}_{table}"
            q = f"""
                create table mid.{rename_to} (like {table}); 
                insert into mid.{rename_to} (select * from mid.{table});
            """
            print(q)
            db.session.execute(q)
            db.session.commit()



### run this every release
##  fills the mid tables with new data from the legacy tables
if False:
    q_list = []
    q_list.append("""
        truncate table mid.affiliation;
        insert into mid.affiliation (paper_id, author_id, affiliation_id, author_sequence_number,
            original_author, original_affiliation) 
            (select paper_id, author_id, affiliation_id, author_sequence_number,
            original_author, original_affiliation from legacy.mag_main_paper_author_affiliations);""")
    q_list.append("""
        truncate table mid.abstract;
        insert into mid.abstract (paper_id, indexed_abstract)
            (select paper_id, inverted_index_json from legacy.mag_nlp_abstracts_inverted);""")
    q_list.append("""
        truncate table mid.citation;
        insert into mid.citation (paper_id, paper_reference_id)
            (select paper_id, paper_reference_id from legacy.mag_main_paper_references_id);""")
    q_list.append("""
        truncate table mid.concept;
        insert into mid.concept 
            (select * from legacy.mag_advanced_fields_of_study);""")
    q_list.append("""
        truncate table mid.mesh;
        insert into mid.mesh (paper_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, is_major_topic) 
            (select paper_id, descriptor_ui, descriptor_name, qualifier_ui, qualifier_name, 
            is_major_topc from legacy.mag_advanced_paper_mesh);""")
    q_list.append("""
        truncate table mid.work_concept;
        insert into mid.work_concept 
            (select * from legacy.mag_advanced_paper_fields_of_study);""")
    q_list.append("""
        truncate table mid.work_extra_ids;
        insert into mid.work_extra_ids 
            (select * from legacy.mag_main_paper_extended_attributes);""")
    q_list.append("""
        truncate table mid.institution;
        insert into mid.institution (affiliation_id, rank, normalized_name, display_name, 
        grid_id, official_page, wiki_page, paper_count, paper_family_count, citation_count, 
        iso3166_code, latitude, longitude, created_date)
            (select affiliation_id, rank, normalized_name, display_name, 
        grid_id, official_page, wiki_page, paper_count, paper_family_count, citation_count, 
        iso3166_code, latitude, longitude, created_date 
        from legacy.mag_main_affiliations);""")
    q_list.append("""
        truncate table mid.journal;
        insert into mid.journal (journal_id, rank, normalized_name, display_name, issn, 
        publisher, webpage, paper_count, paper_family_count, citation_count, created_date)
            (select journal_id, rank, normalized_name, display_name, issn, 
        publisher, webpage, paper_count, paper_family_count, citation_count, created_date 
        from legacy.mag_main_journals);""")
    q_list.append("""
        truncate table mid.author;
        insert into mid.author (author_id, rank, normalized_name, display_name, last_known_affiliation_id, 
        paper_count, paper_family_count, citation_count, created_date)
            (select author_id, rank, normalized_name, display_name, last_known_affiliation_id, 
        paper_count, paper_family_count, citation_count, created_date
        from legacy.mag_main_authors);""")
    q_list.append("""
        truncate table mid.location;
        insert into mid.location (paper_id, source_type, source_url, language_code)
            (select paper_id, source_type, source_url, language_code
        from legacy.mag_main_paper_urls);""")
    q_list.append("""
        truncate table mid.work;
        insert into mid.work (paper_id, rank, doi, doc_type, paper_title, original_title, 
        book_title, year, publication_date, online_date, publisher, journal_id, 
        conference_series_id, conference_instance_id, volume, issue, 
        first_page, last_page, reference_count, citation_count, estimated_citation, 
        original_venue, family_id, family_rank, doc_sub_types, created_date
        )
            (select paper_id, rank, doi, doc_type, paper_title, original_title, 
        book_title, year, publication_date, online_date, publisher, journal_id, 
        conference_series_id, conference_instance_id, volume, issue, 
        first_page, last_page, reference_count, citation_count, estimated_citation, 
        original_venue, family_id, family_rank, doc_sub_types, created_date
        from legacy.mag_main_papers);""")

    for q in q_list:
        print(q)
        db.session.execute(q)
        db.session.commit()

#### use this every release
if False:
    # this should be the date of the previous release, the date we copy previous data from
    q_list = []
    q_list.append(f"""
        update mid.institution set updated_date=t2.updated_date, ror_id=t2.ror_id
        from mid.institution t1 
        join mid.zz{date_of_old_release}_institution t2 on t1.affiliation_id=t2.affiliation_id
        """)
    q_list.append(f"""
        update mid.journal set is_oa=t2.is_oa, is_in_doaj=t2.is_in_doaj, issns=t2.issns, updated_date=t2.updated_date
        from mid.journal t1 
        join mid.zz{date_of_old_release}_journal t2 on t1.journal_id=t2.journal_id
        """)
    q_list.append(f"""
        update mid.author set updated_date=t2.updated_date
        from mid.author t1 
        join mid.zz{date_of_old_release}_author t2 on t1.author_id=t2.author_id
        """)
    q_list.append(f"""
        update mid.location set endpoint_id=t2.endpoint_id,
        evidence=t2.evidence,
        host_type=t2.host_type,
        is_best=t2.is_best,
        license=t2.license,
        oa_date=t2.oa_date,
        pmh_id=t2.pmh_id,
        repository_institution=t2.repository_institution,
        updated=t2.updated,
        url=t2.url,
        url_for_landing_page=t2.url_for_landing_page,
        url_for_pdf=t2.url_for_pdf,
        version=t2.version
        from mid.location t1 
        join mid.zz{date_of_old_release}_location t2 on t1.source_url=t2.source_url and t1.paper_id=t2.paper_id
    """)
    q_list.append(f"""
        update mid.work set updated_date=t2.updated_date,
        genre=t2.genre,
        is_paratext=t2.is_paratext,
        oa_status=t2.oa_status,
        best_url=t2.best_url,
        best_free_url=t2.best_free_url,
        best_free_version=t2.best_free_version,
        doi_lower=t2.doi_lower,
        id=t2.id
        from mid.work t1 
        join mid.zz{date_of_old_release}_work t2 on t1.paper_id=t2.paper_id
        """)

    for q in q_list:
        print(q)
        db.session.execute(q)
        db.session.commit()


# just need to do this for journals that hadn't been added before, which I do by issns is null
update mid.journal set
       display_name=coalesce(t1.display_name, jdb.title),
       normalized_name=coalesce(t1.normalized_name, f_mag_normalize_string(jdb.title)),
       issn=jdb.issn_l,
       issns=jdb.issns_string,
       is_oa=jdb.is_gold_journal,
       is_in_doaj=jdb.is_in_doaj,
       publisher=coalesce(jdb.publisher, t1.publisher),
       updated_date=sysdate
from mid.journal t1
join mid.journalsdb_flat jdb on t1.issn=jdb.issn
where t1.issns is null

 update mid.journal set
        publisher=coalesce(jdb.publisher, t1.publisher)
 from mid.journal t1
 join mid.journalsdb_flat jdb on t1.issn=jdb.issn


# just need to do this for ones with grid_ids we haven't seen before (have grid_id but no ror)
update mid.institution set
       display_name=ror.name,
       ror_id=ror.ror_id,
       official_page=ror.official_page,
       wiki_page=ror.wikipedia_url,
       iso3166_code=ror.country_code,
       latitude=ror.latitude,
       longitude=ror.longitude,
       updated_date=sysdate
from mid.institution t1
join ins.ror_summary ror on t1.grid_id=ror.grid_id
where t1.ror_id is null and t1.grid_id is not null

# make sure this is all set, and do it before the next step
update mid.work set doi_lower=lower(doi) where doi_lower is null and doi is not null

# add unpaywall data to eveything with a doi that doesn't have it yet
update mid.work set
        genre=u.genre,
        is_paratext=u.is_paratext,
        oa_status=u.oa_status,
        best_url=u.doi_url,
        best_free_url=json_extract_path_text(u.best_oa_location, 'url'),
        best_free_version=json_extract_path_text(u.best_oa_location, 'version'),
        updated_date=sysdate
from mid.work t1
join ins.unpaywall_raw u on t1.doi_lower=u.doi
where t1.doi_lower is not null and t1.genre is null


## can't do it with an or, so do it twice, once for each url match
with location_with_paper_id as
    (select work.paper_id, u.* from ins.unpaywall_oa_location_raw u join mid.work work on u.doi=work.doi_lower)
update mid.location set
    endpoint_id=u.endpoint_id,
    evidence=u.evidence,
    host_type=u.host_type,
    is_best=u.is_best,
    license=u.license,
    oa_date=u.oa_date,
    pmh_id=u.pmh_id,
    repository_institution=u.repository_institution,
    updated=u.updated,
    url=u.url,
    url_for_landing_page=u.url_for_landing_page,
    url_for_pdf=u.url_for_pdf,
    version=u.version
from mid.location t1
join location_with_paper_id u on t1.paper_id=u.paper_id
where t1.url is null and
lower(replace(t1.source_url, 'https', 'http')) = lower(replace(u.url_for_landing_page, 'https', 'http'));

with location_with_paper_id as
    (select work.paper_id, u.* from ins.unpaywall_oa_location_raw u join mid.work work on u.doi=work.doi_lower)
update mid.location set
    endpoint_id=u.endpoint_id,
    evidence=u.evidence,
    host_type=u.host_type,
    is_best=u.is_best,
    license=u.license,
    oa_date=u.oa_date,
    pmh_id=u.pmh_id,
    repository_institution=u.repository_institution,
    updated=u.updated,
    url=u.url,
    url_for_landing_page=u.url_for_landing_page,
    url_for_pdf=u.url_for_pdf,
    version=u.version
from mid.location t1
join location_with_paper_id u on t1.paper_id=u.paper_id
where t1.url is null and
lower(replace(t1.source_url, 'https', 'http')) = lower(replace(u.url_for_pdf, 'https', 'http'));


##  also add anything not there already what we've got
insert into mid.location (
    paper_id,
    source_type,
    language_code,
    source_url,
    endpoint_id,
    evidence,
    host_type,
    is_best,
    license,
    oa_date,
    pmh_id,
    repository_institution,
    updated,
    url,
    url_for_landing_page,
    url_for_pdf,
    version)
     (
        select
            work.paper_id,
            null as source_type,
            null as language_code,
            u.url as source_url,
            u.endpoint_id,
            u.evidence,
            u.host_type,
            u.is_best,
            u.license,
            u.oa_date,
            u.pmh_id,
            u.repository_institution,
            u.updated,
            u.url,
            u.url_for_landing_page,
            u.url_for_pdf,
            u.version
            from ins.unpaywall_oa_location_raw u
            join mid.work work on u.doi=work.doi_lower
            where work.paper_id || lower(replace(u.url, 'https', 'http')) not in
                (select loc.paper_id || lower(replace(loc.source_url, 'https', 'http')) from mid.location loc)
        )



# update
#
# update mid.abstract set indexed_abstract=regexp_replace(indexed_abstract, '\\u0000', '') where indexed_abstract ~ '\\u0000';
# update mid.affiliation set original_author=regexp_replace(original_author, '\t', '') where original_author ~ '\t';
# update mid.author set display_name=regexp_replace(display_name, '\t', '') where display_name ~ '\t';
# update mid.author_orcid set orcid=regexp_replace(orcid, 's', '') where orcid ~ 's';
# update mid.institution set display_name=regexp_replace(display_name, '\t', '') where display_name ~ '\t';
# update mid.journal set display_name=regexp_replace(display_name, '\\\\/', '/') where display_name ~ '\\\\/';
# update mid.journal set publisher=regexp_replace(publisher, '\t', '') where publisher ~ '\t';
# update mid.location set source_url=regexp_replace(source_url, '\ten', ''), language_code='en' where source_url ~ '\ten';
# update mid.location set source_url=regexp_replace(source_url, '\tes', ''), language_code='es' where source_url ~ '\tes';
# update mid.location set source_url=regexp_replace(source_url, '\tfr', ''), language_code='fr' where source_url ~ '\tfr';
# update mid.location set source_url=regexp_replace(source_url, '\tsv', ''), language_code='sv' where source_url ~ '\tsv';
# update mid.location set source_url=regexp_replace(source_url, '\tko', ''), language_code='ko' where source_url ~ '\tko';
# update mid.location set source_url=regexp_replace(source_url, '\tpt', ''), language_code='pt' where source_url ~ '\tpt';
# update mid.location set source_url=regexp_replace(source_url, '\tfi', ''), language_code='fi' where source_url ~ '\tfi';
# update mid.location set source_url=regexp_replace(source_url, '\t', '') where source_url ~ '\t';
# update mid.location set url_for_landing_page=regexp_replace(url_for_landing_page, '\n', '') where url_for_landing_page ~ '\n';
# update mid.location set source_url=regexp_replace(source_url, '\n', '') where source_url ~ '\n';
# update mid.location set url=regexp_replace(url, '\n', '') where url ~ '\n';
# update mid.location set url_for_pdf=regexp_replace(url_for_pdf, '\n', '') where url_for_pdf ~ '\n';
#
# update mid.work set original_venue=regexp_replace(original_venue, '\b', '') where original_venue ~ '\b';
# update mid.affiliation set original_affiliation=regexp_replace(original_affiliation, '\b', '') where original_affiliation ~ '\b';
# update legacy.mag_nlp_paper_citation_contexts set citation_context=regexp_replace(citation_context, '\b', '') where citation_context ~ '\b';
#
# update mid.abstract set indexed_abstract=regexp_replace(indexed_abstract, '\\\\:\\[', '":\\[') where indexed_abstract ilike '%\\\\:[%';
#
# update mid.work set original_title=regexp_replace(original_title, '\\\\/', '/') where original_title ~ '\\\\/';
#
#
# select * from mid.author_orcid where orcid not ilike '0%';
# delete from mid.author_orcid where orcid not ilike '0%';
#
# select * from mid.concept where level is null;
# delete from mid.concept where level is null;
#
# select * from mid.affiliation where original_author ~ '\t';
# update mid.affiliation set original_author=regexp_replace(original_author, '\t', '') where original_author ~ '\t';
# select * from mid.work where original_title ~ '\t' none;
# update mid.work set original_title=regexp_replace(original_title, '\t', '') where original_title ~ '\t';
# select * from mid.work where publisher ~ '\t' none;
# update mid.work set publisher=regexp_replace(publisher, '\t', '') where publisher ~ '\t';
#
# select * from mid.journal where display_name ~ '\t';
# update mid.journal set display_name=regexp_replace(display_name, '\t', '') where display_name ~ '\t';
#
#
# select * from legacy.mag_advanced_fields_of_study where display_name ~ '\t'
# select * from mid.concept where display_name ~ '\t'
# update mid.concept set display_name=regexp_replace(display_name, '\t', '') where display_name ~ '\t';
# update legacy.mag_advanced_fields_of_study set display_name=regexp_replace(display_name, '\t', '') where display_name ~ '\t';
#
#
# select * from mid.location where source_url ~ '"' limit 10
# update mid.location set source_url=regexp_replace(source_url, '"', '') where source_url ~ '"';
# select * from mid.affiliation where original_author ~ '"' limit 10
# update mid.affiliation set original_author=regexp_replace(original_author, '"', '') where original_author ~ '"';
# select * from mid.author where display_name ~ '"' limit 10
# update mid.author set display_name=regexp_replace(display_name, '"', '') where display_name ~ '"';
#
# select publisher, count(*) from mid.work where publisher ~ '"' group by publisher
# update mid.work set publisher=regexp_replace(publisher, '"', '*') where publisher ~ '"';
#
#
# select * from mid.abstract where indexed_abstract ~ '\\\\"' limit 100
# update mid.abstract set indexed_abstract=regexp_replace(indexed_abstract, '\\\\"', '') where indexed_abstract ~ '\\\\"';
# select indexed_abstract, regexp_replace(indexed_abstract, '\\\\"', '') from mid.abstract where indexed_abstract ~ '\\\\"' limit 10
#
# update mid.abstract set indexed_abstract=regexp_replace(indexed_abstract, '\\\\:\\[', '":\\[') where indexed_abstract ilike '%\\\\:[%';
#
# update legacy.mag_nlp_abstracts_inverted set inverted_index_json=regexp_replace(inverted_index_json, '\\\\"', '') where inverted_index_json ~ '\\\\"';
# select * from mid.abstract where indexed_abstract ~ '\\\\"' limit 100
# select count(*) from mid.abstract where indexed_abstract ~ '\\\\"'
# select sum(regexp_count(indexed_abstract, '\\\\"')) from mid.abstract
#
#
#
# delete from legacy.mag_advanced_paper_fields_of_study where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_advanced_paper_mesh where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_advanced_paper_recommendations where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_main_paper_author_affiliations where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_main_paper_extended_attributes where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_main_paper_references_id where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_main_paper_urls where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_nlp_abstracts_inverted where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_nlp_paper_citation_contexts where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_main_paper_resources where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from legacy.mag_main_papers where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
#
# delete from legacy.mag_main_affiliations where affiliation_id not in (select affiliation_id from mid.without_patents_affiliation_ids_view without_patents);
# delete from legacy.mag_main_paper_author_affiliations where affiliation_id not in (select affiliation_id from mid.without_patents_affiliation_ids_view without_patents);
#
# delete from legacy.mag_main_author_extended_attributes where author_id not in (select author_id from mid.without_patents_author_ids_view without_patents);
# delete from legacy.mag_main_authors where author_id not in (select author_id from mid.without_patents_author_ids_view without_patents);
# delete from legacy.mag_main_paper_author_affiliations where author_id not in (select author_id from mid.without_patents_author_ids_view without_patents);
#
#
#
# delete from mid.work_concept where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from mid.mesh where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from mid.affiliation where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from mid.work_extra_ids where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from mid.citation where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from mid.work where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from mid.abstract where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
# delete from mid.work where paper_id not in (select paper_id from mid.without_patents_paper_ids_view without_patents);
#
# delete from mid.institution where affiliation_id not in (select affiliation_id from mid.without_patents_affiliation_ids_view without_patents);
# delete from mid.affiliation where affiliation_id not in (select affiliation_id from mid.without_patents_affiliation_ids_view without_patents);
#
# delete from mid.author_orcid where author_id not in (select author_id from mid.without_patents_author_ids_view without_patents);
# delete from mid.author where author_id not in (select author_id from mid.without_patents_author_ids_view without_patents);
# delete from mid.affiliation where author_id not in (select author_id from mid.without_patents_author_ids_view without_patents);
#
#
#
#
# refresh materialized view mid.citation_papers_mv;
# update mid.work set reference_count=v.reference_count, citation_count=v.citation_count, estimated_citation=v.estimated_citation, updated_date=sysdate
# from mid.work t1
# join mid.citation_papers_mv v on t1.paper_id=v.paper_id
# where (v.reference_count != t1.reference_count) or (v.citation_count != t1.citation_count) or (v.estimated_citation != t1.estimated_citation);
# update mid.work set updated_date = created_date::timestamp where updated_date is null;
#
#
# refresh materialized view mid.citation_authors_mv;
# update mid.author set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
# from mid.author t1
# join mid.citation_authors_mv v on t1.author_id=v.author_id
# where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
# update mid.author set updated_date = created_date::timestamp where updated_date is null;
#
#
# refresh materialized view mid.citation_journals_mv;
# update mid.journal set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
# from mid.journal t1
# join mid.citation_journals_mv v on t1.journal_id=v.journal_id
# where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
# update mid.journal set updated_date = created_date::timestamp where updated_date is null;
#
#
# refresh materialized view mid.citation_institutions_mv;
# update mid.institution set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
# from mid.institution t1
# join mid.citation_institutions_mv v on t1.affiliation_id=v.affiliation_id
# where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
# update mid.institution set updated_date = created_date::timestamp where updated_date is null;
#
#
# refresh materialized view mid.citation_concepts_mv;
# update mid.concept set paper_count=v.paper_count, citation_count=v.citation_count, updated_date=sysdate
# from mid.concept t1
# join mid.citation_concepts_mv v on t1.field_of_study_id=v.field_of_study_id
# where (v.paper_count != t1.paper_count) or (v.citation_count != t1.citation_count);
# update mid.concept set updated_date = created_date::timestamp where updated_date is null;
#
#
#
#
# refresh materialized view mid.citation_authors_by_year_mv;
# refresh materialized view mid.citation_journals_by_year_mv;
# refresh materialized view mid.citation_institutions_by_year_mv;
# refresh materialized view mid.citation_concepts_by_year_mv;
# refresh materialized view mid.citation_papers_by_year_mv;
#
# refresh materialized view mid.work_concept_for_api_mv
# refresh materialized view mid.work_concept_for_api_mv
#
# update mid.institution set match_name = f_matching_string(display_name)
# update mid.journal set match_name = f_matching_string(display_name)
#
# update mid.affiliation set match_author = f_matching_author_string(original_author);  -- running now 3830.2
# update mid.affiliation set match_institution_name = f_matching_string(original_affiliation) where original_affiliation is not null; -- took 1500
#
# update mid.work set match_title = f_matching_string(original_title); -- took 1122.8 seconds
#
# update mid.author set match_name = f_matching_author_string(display_name); -- took 1270.5 seconds
#
# update mid.institution set iso3166_code=upper(ror.country_code)
# from mid.institution t1
# join ins.ror_summary_view ror on ror.ror_id=t1.ror_id
# where t1.iso3166_code is null and t1.ror_id is not null
#
# # make sure to do this one, it verifies the abstracts are good to go
# select max(json_extract_path_text(indexed_abstract, 'IndexLength')) from mid.abstract
