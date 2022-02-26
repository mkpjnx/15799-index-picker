import psycopg

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"

DB_CONN_STRING = (
  f'host=127.0.0.1 port=5432 '
  f'dbname={DEFAULT_DB} user={DEFAULT_USER} password={DEFAULT_PASS} '
  f'sslmode=disable application_name=psql')


def get_col_mappings():
    with psycopg.connect(DB_CONN_STRING) as conn:
        query = f'''
        SELECT
            column_name, tablename
        FROM
            pg_catalog.pg_tables, information_schema.columns 
        WHERE tablename = table_name AND tableowner = '{DEFAULT_USER}';
        /*WHERE tablename = table_name AND tableowner <> 'postgres'*/;
        '''
        cur = conn.cursor()
        records = cur.execute(query)
        rows = [(col.upper(),tab) for (col, tab) in records]
        colnames = [row[0] for row in rows]
        assert len(set(colnames)) == len(colnames), "DUPLICATE COL NAMES MAY CAUSE ISSUES"
        return {row[0]:row[1] for row in rows}

def get_existing_indexes():
    with psycopg.connect(DB_CONN_STRING) as conn:
        query = f'''
        SELECT
        class.relname AS indname, t.relname AS tablename, att.attname,
        rank() OVER (PARTITION BY class.oid ORDER BY ind.indkey)
        FROM
            pg_index AS ind,
            pg_class AS class,
            pg_authid AS auth,
            pg_class AS t,
            pg_attribute AS att
        WHERE class.oid = ind.indexrelid
            AND ind.indrelid = t.oid
            AND t.relkind = 'r'
            AND att.attrelid = t.oid
            AND att.attnum = ANY(ind.indkey)
            AND class.relowner = auth.oid
            AND rolname = '{DEFAULT_USER}';
        '''
        cur = conn.cursor()
        records = cur.execute(query)
        rows = [row for row in records]
        indnames = [row[0] for row in rows]
        #assert len(set(indnames)) == len(indnames), "DUPLICATE COL NAMES MAY CAUSE ISSUES"
        return rows
        return {row[0]:row[1:] for row in rows}