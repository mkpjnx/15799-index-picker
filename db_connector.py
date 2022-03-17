import psycopg
import pglast

DEFAULT_DB = "project1db"
DEFAULT_USER = "project1user"
DEFAULT_PASS = "project1pass"

DB_CONN_STRING = (
    f'host=127.0.0.1 port=5432 '
    f'dbname={DEFAULT_DB} user={DEFAULT_USER} password={DEFAULT_PASS} '
    f'sslmode=disable application_name=psql')


def get_conn():
    return psycopg.connect(DB_CONN_STRING)


def _exec(query, conn=None, exec_only=False):
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


def get_col_mappings(conn=None):
    query = f'''
    SELECT
        tablename, column_name
    FROM
        pg_catalog.pg_tables, information_schema.columns 
    WHERE tablename = table_name AND tableowner = '{DEFAULT_USER}';
    /*WHERE tablename = table_name AND tableowner <> 'postgres'*/;
    '''
    return _exec(query, conn)


def get_existing_indexes(conn=None):
    query = f'''
    SELECT i.tablename, i.indexname, i.indexdef, pg_relation_size(c.oid)
    FROM pg_indexes AS i, pg_class AS c
    WHERE i.indexname = c.relname
        AND i.schemaname NOT LIKE 'pg_%'
        AND i.indexdef NOT LIKE '%UNIQUE%';
    '''
    results = _exec(query, conn)
    output = [
        (
            tname, # table name
            iname, # index name
            indexdef, # reconstructed CreateStmt
            [indexParam.name for indexParam in # parsed index refs
             pglast.parse_sql(indexdef)[0].stmt.indexParams],
            size
        )
        for (tname, iname, indexdef, size) in results
    ]
    return output

def get_unused_indexes(conn=None):
    query = '''
    SELECT s.relname AS tablename,
        s.indexrelname AS indexname,
        pg_relation_size(s.indexrelid) AS index_size
    FROM pg_catalog.pg_stat_user_indexes s
    JOIN pg_catalog.pg_index i ON s.indexrelid = i.indexrelid
    WHERE s.idx_scan = 0      -- has never been scanned
        AND 0 <>ALL (i.indkey)  -- no index column is an expression
        AND s.schemaname = 'public'
        AND NOT i.indisunique   -- is not a UNIQUE index
        AND NOT EXISTS          -- does not enforce a constraint
            (SELECT 1 FROM pg_catalog.pg_constraint c
            WHERE c.conindid = s.indexrelid)
    ORDER BY pg_relation_size(s.indexrelid) DESC;
    '''
    return _exec(query, conn)

def get_plan(sql, conn=None):
    return _exec(f'''
    EXPLAIN (FORMAT JSON)
    {sql}
    ''', conn)[0][0][0]
