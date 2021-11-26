# coding: utf-8
from sqlalchemy import text
from app import db

#  python -m scripts/datadump_table_updates

DATE_FOR_RENAME = "20211011"


# rename mag new_ to date_
if False:
    #   get table names
    q = """
     SELECT pg_table_def.tablename
       FROM pg_table_def
      WHERE pg_table_def.schemaname = 'legacy' AND pg_table_def.tablename ilike 'new_%'
      GROUP BY pg_table_def.schemaname, pg_table_def.tablename
      order by pg_table_def.tablename
    """
    rows = db.session.execute(text(q), {"date_for_rename": DATE_FOR_RENAME}).fetchall()
    new_mag_table_names = [row[0] for row in rows]

    rename_sql = ""
    for table in new_mag_table_names:
        rename_to = table.replace("new_", f"{DATE_FOR_RENAME}_")
        rename_sql += f"""alter table legacy.{table} rename to zz{rename_to}; 
        """
    print(rename_sql)
    db.session.execute(rename_sql)
    db.session.commit()

# rename outs to have a date
if True:
    #   get table names
    q = """SET enable_case_sensitive_identifier=true; """
    q += """
     SELECT pg_table_def.tablename
       FROM pg_table_def
      WHERE pg_table_def.schemaname = 'outs' 
      GROUP BY pg_table_def.schemaname, pg_table_def.tablename
      order by pg_table_def.tablename
    """
    rows = db.session.execute(text(q), {"date_for_rename": DATE_FOR_RENAME}).fetchall()
    outs_table_names = [row[0] for row in rows]
    print(outs_table_names)

    rename_sql = """SET enable_case_sensitive_identifier=true; """
    for table in outs_table_names:
        rename_to = f"{DATE_FOR_RENAME}_{table}"
        rename_sql += f"""alter table outs."{table}" rename to "zz{rename_to}"; 
        """
    print(rename_sql)
    db.session.execute(rename_sql)
    db.session.commit()

