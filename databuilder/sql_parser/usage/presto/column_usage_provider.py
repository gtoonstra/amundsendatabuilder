import logging
from antlr4 import InputStream, CommonTokenStream, ParseTreeWalker
from typing import Iterable, List  # noqa: F401

from databuilder.sql_parser.usage.column import Column, Table, remove_double_quotes
from databuilder.sql_parser.usage.presto.antlr_generated.SqlBaseLexer import SqlBaseLexer
from databuilder.sql_parser.usage.presto.antlr_generated.SqlBaseListener import SqlBaseListener
from databuilder.sql_parser.usage.presto.antlr_generated.SqlBaseParser import SqlBaseParser


logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)


class ColumnUsageListener(SqlBaseListener):
    """
    ColumnUsageListener that inherits Antlr generated SqlBaseListener so that it can extract column and table usage
    while ColumnUsageListener walks the parsing tree.

    (Method name is from Antlr generated SqlBaseListener where it does not follow python convention)

    Basic idea of column extraction is to look at SELECT statement as two parts.
     1. processing columns: Columns in SELECT clause. (SELECT foo, bar )
     2. processed columns: Columns in FROM clause. (FROM foobar or FROM (SELECT foo, bar from foobar) )

    Overall, we'd like to retrieve processing column. Thus, the problem we need to solve is basically based on
    processed column, finalize processing column by get the necessary info (such as table, schema) from processed
    column.
    """
    def __init__(self):
        # type: () -> None
        self.processed_cols = []  # type: List[Column]
        self._processing_cols = []  # type: List[Column]
        self._current_col = None  # type: Column
        self._stack = []  # type: List[Column]

    def exitColumnReference(self,
                            ctx  # type: SqlBaseParser.ColumnReferenceContext
                            ):
        # type: (...) -> None
        LOGGER.info("exitColumnReference :" + ctx.getText())
        """
        Call back method for column that does not have table indicator
        :param ctx:
        :return:
        """
        self._current_col = Column(ctx.getText())

    def exitDereference(self,
                        ctx  # type: SqlBaseParser.DereferenceContext
                        ):
        # type: (...) -> None
        """
        Call back method for column with table indicator e.g: foo.bar
        :param ctx:
        :return:
        """
        LOGGER.info("exitDereference :" + ctx.getText())
        self._current_col = Column(ctx.identifier().getText(),
                                   table=Table(ctx.base.getText()))

    def exitSelectSingle(self,
                         ctx  # type: SqlBaseParser.SelectSingleContext
                         ):
        # type: (...) -> None
        """
        Call back method for select single column. This is to distinguish
        between columns for SELECT statement and columns for something else
        such as JOIN statement
        :param ctx:
        :return:
        """
        LOGGER.info("exitSelectSingle :" + ctx.getText())
        if not self._current_col:
            return

        if ctx.identifier():
            self._current_col.col_alias = remove_double_quotes(ctx.identifier().getText())

        self._processing_cols.append(self._current_col)
        self._current_col = None

    def exitSelectAll(self,
                      ctx  # type: SqlBaseParser.SelectAllContext
                      ):
        # type: (...) -> None
        """
        Call back method for select ALL column.
        :param ctx:
        :return:
        """
        LOGGER.info("exitSelectAll :" + ctx.getText())
        self._current_col = Column('*')
        if ctx.qualifiedName():
            self._current_col.table = Table(ctx.qualifiedName().getText())

        self._processing_cols.append(self._current_col)
        self._current_col = None

    def exitTableName(self,
                      ctx  # type: SqlBaseParser.TableNameContext
                      ):
        # type: (...) -> None
        """
        Call back method for table name
        :param ctx:
        :return:
        """
        LOGGER.info("exitTableName :" + ctx.getText())
        table_name = ctx.getText()
        table = Table(table_name)
        if '.' in table_name:
            db_tbl = table_name.split('.')
            table = Table(db_tbl[len(db_tbl) - 1],
                          schema=db_tbl[len(db_tbl) - 2])

        self._current_col = Column('*', table=table)

    def exitAliasedRelation(self,
                            ctx  # type: SqlBaseParser.AliasedRelationContext
                            ):
        # type: (...) -> None
        """
        Call back method for table alias
        :param ctx:
        :return:
        """
        LOGGER.info("exitAliasedRelation :" + ctx.getText())
        if not ctx.identifier():
            return

        # Table alias for column
        if self._current_col and self._current_col.table:
            self._current_col.table.alias = remove_double_quotes(ctx.identifier().getText())
            return

        # Table alias for inner SQL
        for col in self.processed_cols:
            col.table.alias = remove_double_quotes(ctx.identifier().getText())

    def exitRelationDefault(self,
                            ctx  # type: SqlBaseParser.RelationDefaultContext
                            ):
        # type: (...) -> None
        """
        Callback method when exiting FROM clause. Here we are moving processing columns to processed
        to processed
        :param ctx:
        :return:
        """
        LOGGER.info("exitRelationDefault :" + ctx.getText())
        if not self._current_col:
            return

        self.processed_cols.append(self._current_col)
        self._current_col = None

    def enterQuerySpecification(self,
                                ctx  # type: SqlBaseParser.QuerySpecificationContext
                                ):
        # type: (...) -> None
        """
        Callback method for Query specification. For nested SELECT
        statement, it will store previous processing column to stack.
        :param ctx:
        :return:
        """
        LOGGER.info("enterQuerySpecification :" + ctx.getText())
        if not self._processing_cols:
            return

        self._stack.append(self._processing_cols)
        self._processing_cols = []

    def exitQuerySpecification(self,
                               ctx  # type: SqlBaseParser.QuerySpecificationContext
                               ):
        # type: (...) -> None
        """
        Call back method for Query specification. It merges processing
        columns with processed column
        :param ctx:
        :return:
        """
        LOGGER.info("exitQuerySpecification :" + ctx.getText())
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug('processing_cols: {}'.format(self._processing_cols))
            LOGGER.debug('processed_cols: {}'.format(self.processed_cols))

        result = []

        for col in self._processing_cols:
            for resolved in Column.resolve(col, self.processed_cols):
                result.append(resolved)

        self.processed_cols = result
        self._processing_cols = []
        if self._stack:
            self._processing_cols = self._stack.pop()

        self._current_col = None

        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug('done processing_cols: {}'.format(self._processing_cols))
            LOGGER.debug('done processed_cols: {}'.format(self.processed_cols))


class ColumnUsageProvider(object):
    def __init__(self):
        # type: () -> None
        pass

    @classmethod
    def get_columns(cls, query):
        # type: (str) -> Iterable[Column]
        """
        Using presto Grammar, instantiate Parsetree, attach ColumnUsageListener to tree and walk the tree.
        Once finished walking the tree, listener will have selected columns and return them.
        :param query:
        :return:
        """

        query = query.rstrip(';').upper() + "\n"
        lexer = SqlBaseLexer(InputStream(query))
        parser = SqlBaseParser(CommonTokenStream(lexer))
        parse_tree = parser.singleStatement()

        listener = ColumnUsageListener()
        walker = ParseTreeWalker()
        walker.walk(listener, parse_tree)

        return listener.processed_cols


if __name__ == '__main__':
    query = """
SELECT cluster AS cluster,
       date_trunc('day', CAST(ds AS TIMESTAMP)) AS __timestamp,
       sum(p90_time) AS sum__p90_time
FROM
  (select ds,
          cluster,
          approx_percentile(latency_mins, .50) as p50_time,
          approx_percentile(latency_mins, .60) as p60_time,
          approx_percentile(latency_mins, .70) as p70_time,
          approx_percentile(latency_mins, .75) as p75_time,
          approx_percentile(latency_mins, .80) as p80_time,
          approx_percentile(latency_mins, .90) as p90_time,
          approx_percentile(latency_mins, .95) as p95_time
   from
     (SELECT ds,
             cluster_name as cluster,
             query_id,
             date_diff('second',query_starttime,query_endtime)/60.0 as latency_mins
      FROM etl.hive_query_logs
      WHERE date(ds) > date_add('day', -60, current_date)
        AND environment = 'production'
        AND operation_name = 'QUERY' )
   group by ds,
            cluster
   order by ds) AS expr_qry
WHERE ds >= '2018-03-30 00:00:00'
  AND ds <= '2018-05-29 23:29:57'
GROUP BY cluster,
         date_trunc('day', CAST(ds AS TIMESTAMP))
ORDER BY sum__p90_time DESC
LIMIT 5000
"""
    actual = ColumnUsageProvider.get_columns(query)
    print(actual)
