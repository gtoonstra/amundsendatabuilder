import logging
from antlr4 import InputStream, CommonTokenStream, ParseTreeWalker
from typing import Iterable, List  # noqa: F401

from databuilder.sql_parser.usage.column import Column, Table, remove_double_quotes
from databuilder.sql_parser.usage.sqlserver.antlr_generated.TSqlLexer import TSqlLexer
from databuilder.sql_parser.usage.sqlserver.antlr_generated.TSqlParserListener import TSqlParserListener
from databuilder.sql_parser.usage.sqlserver.antlr_generated.TSqlParser import TSqlParser


logging.basicConfig(level=logging.DEBUG)
LOGGER = logging.getLogger(__name__)


class ColumnUsageListener(TSqlParserListener):
    """
    ColumnUsageListener that inherits Antlr generated TSqlParserListener so that it can extract column and table usage
    while ColumnUsageListener walks the parsing tree.

    Basic idea of column extraction is to look at SELECT statement as two parts.
     1. processing columns: Columns in SELECT clause. (SELECT foo, bar )
     2. processed columns: Columns in FROM clause. (FROM foobar or FROM (SELECT foo, bar from foobar) )

    We focus on the processed columns (innermost columns)
    """
    def __init__(self):
        # type: () -> None
        self.processed_cols = []  # type: List[Column]
        self._processing_cols = []  # type: List[Column]
        self._current_col = None  # type: Column
        self._stack = []  # type: List[Column]

    def exitColumn_elem(self,
                        ctx  # type: TSqlParser.Column_elemContext
                        ):
        # type: (...) -> None
        """
        Call back method for any type of column.
        """
        LOGGER.debug("exitColumn_elem: " + ctx.column_name.getText())
        column_name = ctx.column_name.getText()

        if ctx.table_name():
            self._current_col = Column(column_name)
            self._current_col.table = Table(ctx.table_name().getText(), schema=None)
        else:
            self._current_col = Column(column_name)

        if ctx.as_column_alias():
            self._current_col.col_alias = remove_double_quotes(ctx.as_column_alias().column_alias().getText())

    def exitAsterisk(self,
                     ctx  # type: TSqlParser.AsteriskContext
                     ):
        # type: (...) -> None
        """
        Call back method for asterisk in select * or select a.*
        """
        LOGGER.debug("exitAsterisk: " + ctx.getText())
        self._current_col = Column('*')
        if ctx.table_name():
            self._current_col.table = Table(ctx.table_name().getText())

        # Asterisk gets called multiple times, so we remove an existing asterisk entry if there is one.
        if len(self._processing_cols) > 0:
            col = self._processing_cols[-1]
            if col.col_name == '*':
                del self._processing_cols[-1]

        self._processing_cols.append(self._current_col)
        self._current_col = None

    def exitSelect_list_elem(self,
                             ctx  # type: TSqlParser.Select_list_elemContext
                             ):
        # type (....) -> None
        """
        Call back method for selecting an element
        """
        LOGGER.debug("exitSelect_list_elem: " + ctx.getText())

        if not self._current_col:
            return

        self._processing_cols.append(self._current_col)
        self._current_col = None

    def exitTable_name(self,
                       ctx  # type: TSqlParser.Table_name_with_hintContext
                       ):
        # type: (...) -> None
        """
        Call back method for table name
        """
        LOGGER.debug("exitTable_name: " + ctx.getText())

        table_name = ctx.getText()
        table = Table(table_name)
        if '.' in table_name:
            db_tbl = table_name.split('.')
            table = Table(db_tbl[-1],
                          schema=db_tbl[-2])

        self._current_col = Column('*', table=table)

    def exitTable_source_item(self,
                              ctx  # type: TSqlParser.Table_source_item_joinedContext
                              ):
        # type: (...) -> None
        """
        Callback method for a data source item, which is usually a table, but could be a subquery
        """
        LOGGER.debug("exitTable_source_item :" + ctx.getText())

        if self._current_col:
            if self._current_col.table and ctx.as_table_alias():
                self._current_col.table.alias = remove_double_quotes(ctx.as_table_alias().table_alias().getText())
            self.processed_cols.append(self._current_col)
            self._current_col = None
            return

        # Table alias for inner SQL
        if ctx.as_table_alias():
            for col in self.processed_cols:
                col.table.alias = remove_double_quotes(ctx.as_table_alias().table_alias().getText())

    def enterQuery_specification(self,
                                 ctx  # type: TSqlParser.Query_specificationContext
                                 ):
        # type: (...) -> None
        """
        Callback method for Query specification. For nested SELECT
        statement, it will store previous processing column to stack.
        :param ctx:
        :return:
        """
        LOGGER.debug("enterQuery_specification: " + ctx.getText())

        if not self._processing_cols:
            return

        self._stack.append(self._processing_cols)
        self._processing_cols = []

    def exitQuery_specification(self,
                                ctx  # type: TSqlParser.Query_specificationContext
                                ):
        # type: (...) -> None
        """
        Call back method for Query specification. It merges processing
        columns with processed column
        """
        LOGGER.debug("exitQuery_specification: " + ctx.getText())

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
        Using t-sql Grammar, instantiate Parsetree, attach ColumnUsageListener to tree and walk the tree.
        Once finished walking the tree, listener will have selected columns and return them.
        """
        query = query.rstrip(';').upper() + "\n"
        lexer = TSqlLexer(InputStream(query))
        parser = TSqlParser(CommonTokenStream(lexer))
        parse_tree = parser.tsql_file()

        listener = ColumnUsageListener()
        walker = ParseTreeWalker()
        walker.walk(listener, parse_tree)

        return listener.processed_cols


if __name__ == '__main__':
    query = """
        SELECT a from my_table;
    """
    actual = ColumnUsageProvider.get_columns(query)
    print(actual)
