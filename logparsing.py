import re
from typing import List

import pandas as pd
import pglast

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
    return df[['command_tag', 'queries', 'detail']]

