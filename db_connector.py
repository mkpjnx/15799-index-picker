import psycopg

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"

DB_CONN_STRING = (
  f'host=127.0.0.1 port=5432 '
  f'dbname={DEFAULT_DB} user={DEFAULT_USER} password={DEFAULT_PASS} '
  f'sslmode=disable application_name=psql')

def get_conn():
    return psycopg.connect(DB_CONN_STRING)

def _exec(query, conn = None, exec_only = False):
    existed = conn is not None
    conn = conn if conn is not None else get_conn()

    cur = conn.cursor()
    records = cur.execute(query)

    if exec_only:
        if not existed:
            conn.close()
        return cur.ExecStatus

    res = None
    try:
        res = [r for r in records]
    except Exception as e:
        res = [(e)]
    
    if not existed:
        conn.close()
    return res

def get_col_mappings(conn = None):
    query = f'''
    SELECT
        tablename, column_name
    FROM
        pg_catalog.pg_tables, information_schema.columns 
    WHERE tablename = table_name AND tableowner = '{DEFAULT_USER}';
    /*WHERE tablename = table_name AND tableowner <> 'postgres'*/;
    '''
    return _exec(query, conn)

def get_existing_indexes(conn = None):
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
    return _exec(query, conn)


def get_plan(sql, conn = None):
    return _exec(f'''
    EXPLAIN (FORMAT JSON)
    {sql}
    ''', conn)[0][0][0]