from .action import ActionGenerator, Action
import itertools
import copy
import pglast
from pglast import ast, stream
from pglast.enums.parsenodes import *


class CreateIndexAction(Action):
    def __init__(self, table, cols, using=None):
        Action.__init__(self)
        self.table = table
        self.cols = cols
        self.using = using

    def _to_sql(self):

        colnames = [c.replace("_", "") for c in self.cols]
        index_name = f'idx_{self.table}_{"_".join(colnames)}'

        self.ast = ast.IndexStmt(
            idxname=index_name,
            relation=ast.RangeVar(relname=self.table, inh=True),
            accessMethod='btree' if self.using is None else self.using,
            indexParams=tuple(
                [
                    ast.IndexElem(
                        col,
                        ordering=SortByDir.SORTBY_DEFAULT,
                        nulls_ordering=SortByNulls.SORTBY_NULLS_DEFAULT,
                    ) for col in self.cols]
            ),
            idxcomment=None,
            if_not_exists=True,
        )
        return stream.RawStream(semicolon_after_last_statement=True)(self.ast)


class SimpleIndexGenerator(ActionGenerator):
    '''
    For each query, this action generator produces a CREATE INDEX statement
    for each table's columns which appears together.
    '''

    def __init__(self, joint_refs) -> None:
        ActionGenerator.__init__(self)
        self.joint_refs = joint_refs

    def __iter__(self):
        for tablename, table in self.joint_refs.items():
            for cols in table:
                yield CreateIndexAction(tablename, cols)


class ExhaustiveIndexGenerator(ActionGenerator):
    '''
    For each query, this action generator produces a CREATE INDEX statement
    for each table's columns which appears together.
    '''

    def __init__(self, joint_refs, max_width=1) -> None:
        ActionGenerator.__init__(self)
        self.tables = list(joint_refs.keys())
        self.joint_refs = joint_refs
        self.actions_per_table = [len(joint_refs[t]) for t in self.tables]
        # Prefix sum of actions per table
        self.max_width = max_width

    def _iter_table_widths(self, table, width):
        col_perms = set()
        for cols in self.joint_refs[table]:
            for perm in itertools.permutations(cols, width):
                if perm in col_perms:
                    continue
                col_perms.add(perm)
                yield CreateIndexAction(table, perm)

    def __iter__(self) -> str:
        for table in self.tables:
            for width in range(1, self.max_width+1):
                for action in self._iter_table_widths(table, width):
                    yield action


class TypedIndexGenerator(ActionGenerator):
    '''
    For each query, this action generator produces a CREATE INDEX statement
    for each table's columns which appears together.
    '''

    def __init__(self, upstream: ActionGenerator) -> None:
        ActionGenerator.__init__(self)
        self.upstream = upstream

    def __iter__(self) -> str:
        for orig_action in self.upstream:
            for using in ["HASH", "BRIN"]:
                new_action = copy.deepcopy(orig_action)
                new_action.using = using
                yield new_action
