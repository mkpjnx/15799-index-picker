import re
from typing import List

import pandas as pd
import pglast
from pglast import ast, visitors

_PG_LOG_COLUMNS: List[str] = [
    "log_time",
    "user_name",
    "database_name",
    "process_id",
    "connection_from",
    "session_id",
    "session_line_num",
    "command_tag",
    "session_start_time",
    "virtual_transaction_id",
    "transaction_id",
    "error_severity",
    "sql_state_code",
    "message",
    "detail",
    "hint",
    "internal_query",
    "internal_query_pos",
    "context",
    "query",
    "query_pos",
    "location",
    "application_name",
    "backend_type",
]


def _extract_query(message_series):
    """
    Extract SQL queries from the CSVLOG's message column.

    Parameters
    ----------
    message_series : pd.Series
        A series corresponding to the message column of a CSVLOG file.

    Returns
    -------
    query : pd.Series
        A str-typed series containing the queries from the log.
    """
    simple = r"statement: ((?:DELETE|INSERT|SELECT|UPDATE).*)"
    extended = r"execute .+: ((?:DELETE|INSERT|SELECT|UPDATE).*)"
    regex = f"(?:{simple})|(?:{extended})"
    query = message_series.str.extract(regex, flags=re.IGNORECASE)
    # Combine the capture groups for simple and extended query protocol.
    query = query[0].fillna(query[1])
    query.fillna("", inplace=True)
    return query.astype(str)


def parse_csv_log(file):
    """
    Extract queries from workload csv file and return df with fingerprint
    and corresponding queries
    """
    df = pd.read_csv(
        file, names=_PG_LOG_COLUMNS,
        parse_dates=["log_time", "session_start_time"],
        usecols=[
            "log_time",
            "session_start_time",
            "command_tag",
            "message",
            "detail",
        ],
        header=None,
        index_col=False)

    # filter out empty messages
    df = df[df["message"] != ""]
    df['detail'].fillna("", inplace=True)
    # extract queries and toss commits, sets, etc.
    df['queries'] = _extract_query(df['message'])
    df = df[df['queries'] != ""]
    df['fingerprint'] = df['queries'].apply(pglast.parser.fingerprint)
    return df[['fingerprint', 'queries']]

def find_colrefs(node: pglast.node.Node):
    """
    Find all column refs by scanning through a pglast node
    """
    if node is pglast.Missing:
        return []
    colrefs = []
    for subnode in node.traverse():
        if type(subnode) is pglast.node.Scalar:
            continue
        if type(subnode.ast_node) is ast.ColumnRef:
            colref = tuple([
                n.val.value for n in subnode.fields
                if type(n.ast_node) == ast.String])
            if len(colref) > 0:
                colrefs.append(colref)
    return colrefs


def get_all_colrefs(sql, col_mappings):
    """
    Get all column refs from a sql statement which appear in
    WHERE and GROUP BYs

    Attempt to resolve aliases for table refs
    """

    table_cols = {}
    for t, c in col_mappings:
        if t not in table_cols:
            table_cols[t] = [c]
        else:
            table_cols[t].append(c)

    tree = pglast.parse_sql(sql)

    aliases = {}
    raw_colrefs = []
    referenced_tables = visitors.referenced_relations(tree)

    # mine the AST for aliases and colrefs
    for node in pglast.node.Node(tree).traverse():
        if type(node) is pglast.node.Scalar:
            continue
        if 'whereClause' in node.attribute_names:
            raw_colrefs += find_colrefs(node.whereClause)
        if 'groupClause' in node.attribute_names:
            raw_colrefs += find_colrefs(node.groupClause)
        if 'alias' in node.attribute_names and 'relname' in node.attribute_names:
            if node.alias is pglast.Missing or node.relname is pglast.Missing:
                continue
            if node.alias.aliasname.value in aliases:
                print("UH OH, double alias")
            aliases[node.alias.aliasname.value] = node.relname.value
    # resolve aliases and figure out actual table col refs
    potential_colrefs = []
    for c in raw_colrefs:
        potential_tables = []
        p_col = None
        if len(c) == 1:
            p_col = c[0]
            potential_tables = [
                t for t in referenced_tables
                if t in table_cols
                and p_col in table_cols[t]]
        if len(c) == 2:
            t = c[0]
            p_col = c[1]
            if t not in table_cols and t in aliases:
                t = aliases[t]
            potential_tables = [t]
        potential_colrefs += [
            (p_t, p_col) for p_t in potential_tables if (p_t, p_col) in col_mappings]
    return set(potential_colrefs)

def aggregate_templates(df, col_mappings, percent_threshold=1):
    """
    Aggregate queries into templates based on pglast
    Only retain most common queries up to {percent_threshold} of the workload
    """
    aggregated = df[['queries', 'fingerprint']]\
        .groupby('fingerprint')\
        .agg([pd.DataFrame.sample, "count"])['queries']\
        .sort_values('count', ascending=False)
    aggregated['fraction'] = aggregated['count'] / aggregated['count'].sum()
    aggregated['cumsum'] = aggregated['fraction'].cumsum()
    filtered = pd.DataFrame(
        aggregated[aggregated['cumsum'] <= percent_threshold])

    # get column refs
    filtered['colrefs'] = filtered['sample'].apply(
        get_all_colrefs, args=(col_mappings,))
    
    return filtered[['sample', 'count', 'cumsum', 'colrefs']]
