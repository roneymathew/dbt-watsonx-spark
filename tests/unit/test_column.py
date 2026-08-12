import unittest

from dbt.adapters.watsonx_spark import SparkColumn


class TestSparkColumn(unittest.TestCase):
    def test_convert_table_stats_with_no_statistics(self):
        self.assertDictEqual(SparkColumn.convert_table_stats(None), {})

    def test_convert_table_stats_with_bytes(self):
        self.assertDictEqual(
            SparkColumn.convert_table_stats("123456789 bytes"),
            {
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 123456789,
            },
        )

    def test_convert_table_stats_with_bytes_and_rows(self):
        self.assertDictEqual(
            SparkColumn.convert_table_stats("1234567890 bytes, 12345678 rows"),
            {
                "stats:bytes:description": "",
                "stats:bytes:include": True,
                "stats:bytes:label": "bytes",
                "stats:bytes:value": 1234567890,
                "stats:rows:description": "",
                "stats:rows:include": True,
                "stats:rows:label": "rows",
                "stats:rows:value": 12345678,
            },
        )

    def test_translate_type(self):
        """Test that translate_type returns the dtype as-is"""
        self.assertEqual(SparkColumn.translate_type("string"), "string")
        self.assertEqual(SparkColumn.translate_type("int"), "int")
        self.assertEqual(SparkColumn.translate_type("decimal(10,2)"), "decimal(10,2)")

    def test_can_expand_to(self):
        """Test can_expand_to method exists and returns boolean"""
        col1 = SparkColumn(column="col1", dtype="varchar(100)")
        col2 = SparkColumn(column="col2", dtype="varchar(200)")
        
        # Test that method exists and returns a boolean
        result = col1.can_expand_to(col2)
        self.assertIsInstance(result, bool)
        
        # The method checks if both columns are strings using is_string()
        # which is inherited from base Column class

    def test_literal(self):
        """Test literal value casting"""
        col = SparkColumn(column="test_col", dtype="int")
        self.assertEqual(col.literal("123"), "cast(123 as int)")
        
        col_str = SparkColumn(column="test_col", dtype="string")
        self.assertEqual(col_str.literal("'hello'"), "cast('hello' as string)")

    def test_quoted(self):
        """Test quoted column name"""
        col = SparkColumn(column="my_column", dtype="string")
        self.assertEqual(col.quoted, "`my_column`")
        
        col_special = SparkColumn(column="column-with-dash", dtype="int")
        self.assertEqual(col_special.quoted, "`column-with-dash`")

    def test_data_type(self):
        """Test data_type property"""
        col = SparkColumn(column="test", dtype="decimal(10,2)")
        self.assertEqual(col.data_type, "decimal(10,2)")

    def test_numeric_type_without_precision(self):
        """Test numeric_type without precision/scale"""
        result = SparkColumn.numeric_type("decimal", None, None)
        self.assertEqual(result, "decimal")

    def test_numeric_type_with_precision(self):
        """Test numeric_type with precision and scale"""
        result = SparkColumn.numeric_type("decimal", 10, 2)
        self.assertEqual(result, "decimal(10,2)")
        
        result2 = SparkColumn.numeric_type("decimal", 18, 4)
        self.assertEqual(result2, "decimal(18,4)")

    def test_repr(self):
        """Test string representation"""
        col = SparkColumn(column="my_col", dtype="string")
        self.assertEqual(repr(col), "<SparkColumn my_col (string)>")

    def test_to_column_dict_without_stats(self):
        """Test to_column_dict without table stats"""
        col = SparkColumn(
            column="test_col",
            dtype="string",
            table_database="db1",
            table_schema="schema1",
            table_name="table1"
        )
        result = col.to_column_dict()
        
        self.assertEqual(result["column"], "test_col")
        self.assertEqual(result["dtype"], "string")
        self.assertEqual(result["table_database"], "db1")
        self.assertNotIn("table_stats", result)

    def test_to_column_dict_with_stats(self):
        """Test to_column_dict with table stats merged into root"""
        stats = {
            "stats:bytes:value": 1000,
            "stats:bytes:label": "bytes",
            "stats:rows:value": 10,
            "stats:rows:label": "rows"
        }
        col = SparkColumn(
            column="test_col",
            dtype="string",
            table_stats=stats
        )
        result = col.to_column_dict()
        
        # Stats should be merged into root, not nested
        self.assertNotIn("table_stats", result)
        self.assertEqual(result["stats:bytes:value"], 1000)
        self.assertEqual(result["stats:rows:value"], 10)
