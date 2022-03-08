from collections import defaultdict
import pandas as pd
import numpy as np
import pglast

import logparsing
import action_generation.index_actions as index_actions

import db_connector


def get_workload_colrefs(filtered):
    # indexes = db_connector.get_existing_indexes()
    table_colrefs_joint_counts = defaultdict(lambda: defaultdict(np.uint64))

    for _, row in filtered.iterrows():
        refs = row['colrefs']
        tables = set([tab for (tab, _) in refs])
        for table in tables:
            cols_for_tabs = [col for (tab, col) in refs if tab == table]
            if len(cols_for_tabs) == 0:
                continue
            joint_ref = tuple(dict.fromkeys(cols_for_tabs))
            table_colrefs_joint_counts[table][joint_ref] += row['count']

    return table_colrefs_joint_counts


def find_indexes(obj):
    if type(obj) is list:
        return sum([find_indexes(x) for x in obj], [])
    if type(obj) is dict:
        res = sum([find_indexes(obj[x]) for x in obj], [])
        res += [obj['Index Name']] if 'Index Name' in obj else []
        return res
    return []


def estimate_cost(parsed, conn, iterations = 10):
    col_mappings = db_connector.get_col_mappings()
    filtered = None
    costs = []
    for i in range(iterations):
        print(f'cost estimate iter: {i}')
        filtered = logparsing.aggregate_templates(parsed, col_mappings, 0.01)

        # See how the plans changed
        filtered['newplan'] = filtered['sample'].apply(
            lambda x: db_connector.get_plan(x, conn))
        filtered['indexes_used'] = filtered['newplan'].apply(find_indexes)

        cost = filtered['newplan'].apply(
            lambda x: x['Plan']['Total Cost'])
        costs.append(cost)

    filtered['cost'] = pd.concat(costs, axis = 1).mean(axis = 1)
    return filtered

def generate_indexes(workload_csv):
    col_mappings = db_connector.get_col_mappings()
    parsed = logparsing.parse_csv_log(workload_csv)
    filtered = logparsing.aggregate_templates(parsed, col_mappings, 0.01)
    colrefs = get_workload_colrefs(filtered)

    # Resulting hypopg indexes and corresponding CREATE INDEX action
    hypoconn = db_connector.get_conn()
    print("Estimating pre-hypothetical costs")
    before = estimate_cost(parsed, hypoconn, iterations = 3)
    hypo_results = []

    # Install hypothetical indexes
    exhaustive = index_actions.ExhaustiveIndexGenerator(colrefs, 2)
    actions = list(exhaustive)
    for action in actions:
        sql_str = str(action)
        print(f"Creating hypoindex {sql_str}")
        hypo_sql = f"SELECT * FROM hypopg_create_index('{sql_str}') ;"
        hypo_results.append((action, hypoconn.execute(hypo_sql).fetchall()))

    print("Estimating workload cost with hypothetical indexes")
    hypo_ests = estimate_cost(parsed, hypoconn, iterations = 3)

    hypo_ests['cost_diff'] = hypo_ests['cost'] - before['cost']
    return hypo_results, hypo_ests

def generate_sql(workload_csv, timeout):
    hypo_results, hypo_ests = generate_indexes(workload_csv)
    
    by_improvement = hypo_ests.loc[
        (hypo_ests['cost_diff']*hypo_ests['count']).sort_values().index]

    print(by_improvement[["sample","cumsum","indexes_used","cost_diff",]])

    ordered_candidates = []
    for _, row in by_improvement.iterrows():
        for ind in row['indexes_used']:
            if ind not in ordered_candidates:
                ordered_candidates.append(ind)

    # reverse lookup of index action from hypopg index name
    action_dict = {ind[0][1]:a for (a, ind) in hypo_results}

    unordered_colref_set = set()
    final_adds = []
    existing_used = []

    for ind in ordered_candidates:
        # if ind in actual_indexes:
        if ind in action_dict:
            a = action_dict[ind]
            ind_cols = tuple(sorted(a.cols))
            # ind_cols = tuple(a.cols)
            canonical = (a.table, ind_cols)
            if canonical not in unordered_colref_set:
                unordered_colref_set.add(canonical)
                final_adds.append(a)
        else:
            existing_used.append(ind)

    with open('actions.sql','w') as f:
        f.writelines([str(a)+'\n' for a in final_adds])