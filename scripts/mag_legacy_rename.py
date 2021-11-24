# coding: utf-8

from sqlalchemy import text

from app import db

DATE_FOR_RENAME = "20211025"

#  python -m scripts.mag_legacy_rename

q = """
 SELECT pg_table_def.tablename
   FROM pg_table_def
  WHERE pg_table_def.schemaname = 'legacy' AND pg_table_def.tablename ilike 'new_%'
  GROUP BY pg_table_def.schemaname, pg_table_def.tablename
  order by pg_table_def.tablename
"""
rows = db.session.execute(text(q), {"date_for_rename": DATE_FOR_RENAME}).fetchall()
table_names = [row[0] for row in rows]

# rename mag new_ to date_
if False:
    rename_sql = ""
    for table in table_names:
        rename_to = table.replace("new_", f"{DATE_FOR_RENAME}_")
        rename_sql += f"""alter table legacy.{table} rename to zz{rename_to};
        """
    print(rename_sql)
    db.session.execute(rename_sql)
    db.session.commit()



