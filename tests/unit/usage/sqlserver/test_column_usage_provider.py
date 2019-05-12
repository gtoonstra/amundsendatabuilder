import logging
import unittest

from databuilder.sql_parser.usage.column import Column, Table, OrTable
from databuilder.sql_parser.usage.sqlserver.column_usage_provider import ColumnUsageProvider


class TestColumnUsage(unittest.TestCase):

    def setUp(self):
        # type: () -> None
        logging.basicConfig(level=logging.INFO)

    def test_column_usage(self):
        # type: () -> None
        query = 'SELECT foo, bar FROM foobar;'

        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='FOO', table=Table(name='FOOBAR', schema=None, alias=None), col_alias=None),
                    Column(name='BAR', table=Table(name='FOOBAR', schema=None, alias=None), col_alias=None)]
        self.assertEqual(expected.__repr__(), actual.__repr__())

    def test_with_schema(self):
        # type: () -> None
        query = 'SELECT foo, bar FROM scm.foobar;'

        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='FOO', table=Table(name='FOOBAR', schema='SCM', alias=None), col_alias=None),
                    Column(name='BAR', table=Table(name='FOOBAR', schema='SCM', alias=None), col_alias=None)]
        self.assertEqual(expected.__repr__(), actual.__repr__())

    def test_join(self):
        # type: () -> None
        query = 'SELECT A, B FROM scm.FOO JOIN BAR ON FOO.A = BAR.B'
        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='A', table=OrTable(tables=[Table(name='FOO', schema='SCM', alias=None),
                                                           Table(name='BAR', schema=None, alias=None)]),
                           col_alias=None),
                    Column(name='B', table=OrTable(tables=[Table(name='FOO', schema='SCM', alias=None),
                                                           Table(name='BAR', schema=None, alias=None)]),
                           col_alias=None)]

        self.assertEqual(expected.__repr__(), actual.__repr__())

    def test_join_with_alias(self):
        # type: () -> None
        query = 'SELECT FOO.A, BAR.B FROM FOOTABLE AS FOO JOIN BARTABLE AS BAR ON FOO.A = BAR.A'
        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='A', table=Table(name='FOOTABLE', schema=None, alias='FOO'), col_alias=None),
                    Column(name='B', table=Table(name='BARTABLE', schema=None, alias='BAR'), col_alias=None)]
        self.assertEqual(expected.__repr__(), actual.__repr__())

    def test_inner_sql(self):
        # type: () -> None
        query = 'SELECT TMP1.A, B FROM (SELECT * FROM FOOBAR) AS TMP1'
        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='A', table=Table(name='FOOBAR', schema=None, alias='TMP1'), col_alias=None),
                    Column(name='B', table=Table(name='FOOBAR', schema=None, alias='TMP1'), col_alias=None)]
        self.assertEqual(expected.__repr__(), actual.__repr__())

    def test_inner_sql_col_alias(self):
        # type: () -> None
        query = 'SELECT TMP1.A, F FROM (SELECT A, B AS F, C FROM FOOBAR) AS TMP1'
        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='A', table=Table(name='FOOBAR', schema=None, alias='TMP1'), col_alias=None),
                    Column(name='B', table=Table(name='FOOBAR', schema=None, alias='TMP1'), col_alias='F')]
        self.assertEqual(expected.__repr__(), actual.__repr__())

    def test_table_alias(self):
        # type: () -> None
        query = """
        SELECT A.*  FROM FACT_RIDES A LEFT JOIN DIM_VEHICLES B ON A.VEHICLE_KEY = B.VEHICLE_KEY
        WHERE B.RENTAL_PROVIDER_ID IS NOT NULL  LIMIT 100
        """
        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='*', table=Table(name='FACT_RIDES', schema=None, alias='A'), col_alias=None)]
        self.assertEqual(expected.__repr__(), actual.__repr__())

    def test_inner_sql_table_alias(self):
        # type: () -> None
        query = """
        SELECT col1, temp.col2 FROM (SELECT col1, col2, col3 FROM foobar) as temp
        """
        actual = ColumnUsageProvider.get_columns(query)
        expected = [Column(name='COL1', table=Table(name='FOOBAR', schema=None, alias='TEMP'), col_alias=None),
                    Column(name='COL2', table=Table(name='FOOBAR', schema=None, alias='TEMP'), col_alias=None)]
        self.assertEqual(expected.__repr__(), actual.__repr__())


if __name__ == '__main__':
    unittest.main()
