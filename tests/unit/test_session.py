import unittest
from unittest.mock import Mock, patch, MagicMock
import datetime as dt
from decimal import Decimal

from dbt.adapters.watsonx_spark.session import (
    Cursor,
    Connection,
    SessionConnectionWrapper,
)
from dbt_common.exceptions import DbtRuntimeError
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException


class TestCursor(unittest.TestCase):
    """Test the Cursor class"""
    
    def test_init_default(self):
        """Test Cursor initialization with defaults"""
        cursor = Cursor()
        
        self.assertIsNone(cursor._df)
        self.assertIsNone(cursor._rows)
        self.assertEqual(cursor.server_side_parameters, {})
    
    def test_init_with_parameters(self):
        """Test Cursor initialization with server-side parameters"""
        params = {"spark.sql.shuffle.partitions": "200"}
        cursor = Cursor(server_side_parameters=params)
        
        self.assertEqual(cursor.server_side_parameters, params)
    
    def test_context_manager_enter(self):
        """Test Cursor as context manager - enter"""
        cursor = Cursor()
        
        with cursor as c:
            self.assertIs(c, cursor)
    
    def test_context_manager_exit(self):
        """Test Cursor as context manager - exit"""
        cursor = Cursor()
        cursor._df = Mock()
        cursor._rows = [Mock()]
        
        with cursor:
            pass
        
        # Should be closed after exiting context
        self.assertIsNone(cursor._df)
        self.assertIsNone(cursor._rows)
    
    def test_description_no_dataframe(self):
        """Test description property when no dataframe"""
        cursor = Cursor()
        
        description = cursor.description
        
        self.assertEqual(description, [])
    
    def test_description_with_dataframe(self):
        """Test description property with dataframe"""
        # Create mock dataframe with schema
        mock_field1 = Mock()
        mock_field1.name = "col1"
        mock_field1.dataType.simpleString.return_value = "string"
        mock_field1.nullable = True
        
        mock_field2 = Mock()
        mock_field2.name = "col2"
        mock_field2.dataType.simpleString.return_value = "int"
        mock_field2.nullable = False
        
        mock_df = Mock()
        mock_df.schema.fields = [mock_field1, mock_field2]
        
        cursor = Cursor()
        cursor._df = mock_df
        
        description = cursor.description
        
        self.assertEqual(len(description), 2)
        self.assertEqual(description[0], ("col1", "string", None, None, None, None, True))
        self.assertEqual(description[1], ("col2", "int", None, None, None, None, False))
    
    def test_close(self):
        """Test close method"""
        cursor = Cursor()
        cursor._df = Mock()
        cursor._rows = [Mock()]
        
        cursor.close()
        
        self.assertIsNone(cursor._df)
        self.assertIsNone(cursor._rows)
    
    @patch('dbt.adapters.watsonx_spark.session.SparkSession')
    def test_execute_simple_sql(self, mock_spark_session_class):
        """Test execute with simple SQL"""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df
        
        mock_builder = Mock()
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_spark_session_class.builder = mock_builder
        
        cursor = Cursor()
        cursor.execute("SELECT * FROM table")
        
        self.assertEqual(cursor._df, mock_df)
        mock_spark.sql.assert_called_once_with("SELECT * FROM table")
    
    @patch('dbt.adapters.watsonx_spark.session.SparkSession')
    def test_execute_with_parameters(self, mock_spark_session_class):
        """Test execute with SQL parameters"""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df
        
        mock_builder = Mock()
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_spark_session_class.builder = mock_builder
        
        cursor = Cursor()
        cursor.execute("SELECT * FROM %s WHERE id = %s", "table", "123")
        
        mock_spark.sql.assert_called_once_with("SELECT * FROM table WHERE id = 123")
    
    @patch('dbt.adapters.watsonx_spark.session.SparkSession')
    def test_execute_with_server_side_parameters(self, mock_spark_session_class):
        """Test execute with server-side parameters"""
        mock_spark = Mock()
        mock_df = Mock()
        mock_spark.sql.return_value = mock_df
        
        mock_builder = Mock()
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_spark_session_class.builder = mock_builder
        
        params = {"spark.sql.shuffle.partitions": "200", "spark.executor.memory": "4g"}
        cursor = Cursor(server_side_parameters=params)
        cursor.execute("SELECT 1")
        
        # Verify config was called for each parameter
        self.assertEqual(mock_builder.config.call_count, 2)
    
    @patch('dbt.adapters.watsonx_spark.session.SparkSession')
    def test_execute_analysis_exception(self, mock_spark_session_class):
        """Test execute with AnalysisException"""
        mock_spark = Mock()
        mock_spark.sql.side_effect = AnalysisException("Table not found")
        
        mock_builder = Mock()
        mock_builder.enableHiveSupport.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark
        mock_spark_session_class.builder = mock_builder
        
        cursor = Cursor()
        
        with self.assertRaises(DbtRuntimeError) as cm:
            cursor.execute("SELECT * FROM missing_table")
        self.assertIn("Table not found", str(cm.exception))
    
    def test_fetchall_no_dataframe(self):
        """Test fetchall when no dataframe"""
        cursor = Cursor()
        
        result = cursor.fetchall()
        
        self.assertIsNone(result)
    
    def test_fetchall_with_dataframe(self):
        """Test fetchall with dataframe"""
        mock_df = Mock()
        mock_rows = [Row(id=1, name="test1"), Row(id=2, name="test2")]
        mock_df.collect.return_value = mock_rows
        
        cursor = Cursor()
        cursor._df = mock_df
        
        result = cursor.fetchall()
        
        self.assertEqual(result, mock_rows)
        mock_df.collect.assert_called_once()
    
    def test_fetchall_cached(self):
        """Test fetchall returns cached rows"""
        mock_df = Mock()
        mock_rows = [Row(id=1)]
        mock_df.collect.return_value = mock_rows
        
        cursor = Cursor()
        cursor._df = mock_df
        
        # First call
        result1 = cursor.fetchall()
        # Second call should return cached
        result2 = cursor.fetchall()
        
        self.assertEqual(result1, result2)
        # collect should only be called once
        mock_df.collect.assert_called_once()
    
    def test_fetchone_no_dataframe(self):
        """Test fetchone when no dataframe"""
        cursor = Cursor()
        
        result = cursor.fetchone()
        
        self.assertIsNone(result)
    
    def test_fetchone_with_dataframe(self):
        """Test fetchone with dataframe"""
        mock_df = Mock()
        mock_row = Row(id=1, name="test")
        mock_df.take.return_value = [mock_row]
        
        cursor = Cursor()
        cursor._df = mock_df
        
        result = cursor.fetchone()
        
        self.assertEqual(result, mock_row)
        mock_df.take.assert_called_once_with(1)
    
    def test_fetchone_multiple_calls(self):
        """Test fetchone with multiple calls"""
        mock_df = Mock()
        mock_rows = [Row(id=1), Row(id=2)]
        mock_df.take.return_value = mock_rows
        
        cursor = Cursor()
        cursor._df = mock_df
        
        # First call
        result1 = cursor.fetchone()
        self.assertEqual(result1.id, 1)
        
        # Second call should return second row
        result2 = cursor.fetchone()
        self.assertEqual(result2.id, 2)
        
        # Third call should return None
        result3 = cursor.fetchone()
        self.assertIsNone(result3)
    
    def test_fetchone_empty_result(self):
        """Test fetchone with empty result"""
        mock_df = Mock()
        mock_df.take.return_value = []
        
        cursor = Cursor()
        cursor._df = mock_df
        
        result = cursor.fetchone()
        
        self.assertIsNone(result)


class TestConnection(unittest.TestCase):
    """Test the Connection class"""
    
    def test_init_default(self):
        """Test Connection initialization with defaults"""
        conn = Connection()
        
        self.assertEqual(conn.server_side_parameters, {})
    
    def test_init_with_parameters(self):
        """Test Connection initialization with parameters"""
        params = {"spark.sql.shuffle.partitions": "200"}
        conn = Connection(server_side_parameters=params)
        
        self.assertEqual(conn.server_side_parameters, params)
    
    def test_cursor(self):
        """Test cursor method"""
        params = {"spark.executor.memory": "4g"}
        conn = Connection(server_side_parameters=params)
        
        cursor = conn.cursor()
        
        self.assertIsInstance(cursor, Cursor)
        self.assertEqual(cursor.server_side_parameters, params)


class TestSessionConnectionWrapper(unittest.TestCase):
    """Test the SessionConnectionWrapper class"""
    
    def test_init(self):
        """Test SessionConnectionWrapper initialization"""
        mock_handle = Mock(spec=Connection)
        
        wrapper = SessionConnectionWrapper(mock_handle)
        
        self.assertEqual(wrapper.handle, mock_handle)
        self.assertIsNone(wrapper._cursor)
    
    def test_cursor(self):
        """Test cursor method"""
        mock_cursor = Mock(spec=Cursor)
        mock_handle = Mock(spec=Connection)
        mock_handle.cursor.return_value = mock_cursor
        
        wrapper = SessionConnectionWrapper(mock_handle)
        result = wrapper.cursor()
        
        self.assertEqual(wrapper._cursor, mock_cursor)
        self.assertIs(result, wrapper)
        mock_handle.cursor.assert_called_once()
    
    def test_cancel(self):
        """Test cancel method (no-op)"""
        mock_handle = Mock(spec=Connection)
        wrapper = SessionConnectionWrapper(mock_handle)
        
        # Should not raise
        wrapper.cancel()
    
    def test_close_with_cursor(self):
        """Test close method with cursor"""
        mock_cursor = Mock(spec=Cursor)
        mock_handle = Mock(spec=Connection)
        
        wrapper = SessionConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.close()
        
        mock_cursor.close.assert_called_once()
    
    def test_close_without_cursor(self):
        """Test close method without cursor"""
        mock_handle = Mock(spec=Connection)
        wrapper = SessionConnectionWrapper(mock_handle)
        
        # Should not raise
        wrapper.close()
    
    def test_rollback(self):
        """Test rollback method (no-op)"""
        mock_handle = Mock(spec=Connection)
        wrapper = SessionConnectionWrapper(mock_handle)
        
        # Should not raise
        wrapper.rollback()
        wrapper.rollback(arg1="value", kwarg1="value")
    
    def test_fetchall(self):
        """Test fetchall method"""
        mock_rows = [Row(id=1), Row(id=2)]
        mock_cursor = Mock(spec=Cursor)
        mock_cursor.fetchall.return_value = mock_rows
        mock_handle = Mock(spec=Connection)
        
        wrapper = SessionConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        result = wrapper.fetchall()
        
        self.assertEqual(result, mock_rows)
        mock_cursor.fetchall.assert_called_once()
    
    def test_fetchall_no_cursor(self):
        """Test fetchall without cursor raises assertion"""
        mock_handle = Mock(spec=Connection)
        wrapper = SessionConnectionWrapper(mock_handle)
        
        with self.assertRaises(AssertionError) as cm:
            wrapper.fetchall()
        self.assertIn("Cursor not available", str(cm.exception))
    
    def test_execute_simple_sql(self):
        """Test execute with simple SQL"""
        mock_cursor = Mock(spec=Cursor)
        mock_handle = Mock(spec=Connection)
        
        wrapper = SessionConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.execute("SELECT * FROM table")
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM table")
    
    def test_execute_sql_with_semicolon(self):
        """Test execute strips trailing semicolon"""
        mock_cursor = Mock(spec=Cursor)
        mock_handle = Mock(spec=Connection)
        
        wrapper = SessionConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.execute("SELECT * FROM table;")
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM table")
    
    def test_execute_with_bindings(self):
        """Test execute with bindings"""
        mock_cursor = Mock(spec=Cursor)
        mock_handle = Mock(spec=Connection)
        
        wrapper = SessionConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        wrapper.execute("SELECT * FROM table WHERE id = %s", [123])
        
        mock_cursor.execute.assert_called_once_with("SELECT * FROM table WHERE id = %s", 123.0)
    
    def test_execute_no_cursor(self):
        """Test execute without cursor raises assertion"""
        mock_handle = Mock(spec=Connection)
        wrapper = SessionConnectionWrapper(mock_handle)
        
        with self.assertRaises(AssertionError) as cm:
            wrapper.execute("SELECT 1")
        self.assertIn("Cursor not available", str(cm.exception))
    
    def test_description(self):
        """Test description property"""
        mock_description = [("col1", "string", None, None, None, None, True)]
        mock_cursor = Mock(spec=Cursor)
        mock_cursor.description = mock_description
        mock_handle = Mock(spec=Connection)
        
        wrapper = SessionConnectionWrapper(mock_handle)
        wrapper._cursor = mock_cursor
        
        result = wrapper.description
        
        self.assertEqual(result, mock_description)
    
    def test_description_no_cursor(self):
        """Test description without cursor raises assertion"""
        mock_handle = Mock(spec=Connection)
        wrapper = SessionConnectionWrapper(mock_handle)
        
        with self.assertRaises(AssertionError) as cm:
            _ = wrapper.description
        self.assertIn("Cursor not available", str(cm.exception))
    
    def test_fix_binding_int(self):
        """Test _fix_binding with int"""
        result = SessionConnectionWrapper._fix_binding(123)
        self.assertEqual(result, 123.0)
        self.assertIsInstance(result, float)
    
    def test_fix_binding_float(self):
        """Test _fix_binding with float"""
        result = SessionConnectionWrapper._fix_binding(123.45)
        self.assertEqual(result, 123.45)
        self.assertIsInstance(result, float)
    
    def test_fix_binding_decimal(self):
        """Test _fix_binding with Decimal"""
        result = SessionConnectionWrapper._fix_binding(Decimal("123.45"))
        self.assertEqual(result, 123.45)
        self.assertIsInstance(result, float)
    
    def test_fix_binding_datetime(self):
        """Test _fix_binding with datetime"""
        dt_value = dt.datetime(2024, 1, 15, 10, 30, 45, 123456)
        result = SessionConnectionWrapper._fix_binding(dt_value)
        
        self.assertEqual(result, "'2024-01-15 10:30:45.123'")
        self.assertIsInstance(result, str)
    
    def test_fix_binding_string(self):
        """Test _fix_binding with string"""
        result = SessionConnectionWrapper._fix_binding("test_value")
        self.assertEqual(result, "'test_value'")
    
    def test_fix_binding_other(self):
        """Test _fix_binding with other types (bool is treated as number)"""
        # bool is a subclass of int, so it gets converted to float
        result = SessionConnectionWrapper._fix_binding(True)
        self.assertEqual(result, 1.0)
        
        # Test with actual string
        result = SessionConnectionWrapper._fix_binding("test")
        self.assertEqual(result, "'test'")


if __name__ == '__main__':
    unittest.main()

# Made with Bob