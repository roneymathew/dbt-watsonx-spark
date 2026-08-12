import unittest
from dbt.adapters.watsonx_spark import SparkRelation
from dbt_common.exceptions import DbtRuntimeError


class TestSparkRelation(unittest.TestCase):
    """Unit tests for SparkRelation class"""

    def test_quote_policy_defaults(self):
        """Test default quote policy"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table"
        )
        # Database and schema should not be quoted by default
        self.assertFalse(relation.quote_policy.database)
        self.assertFalse(relation.quote_policy.schema)
        # Identifier should be quoted by default
        self.assertTrue(relation.quote_policy.identifier)

    def test_include_policy_defaults(self):
        """Test default include policy"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table"
        )
        # Database should not be included
        self.assertFalse(relation.include_policy.database)
        # Schema and identifier should be included
        self.assertTrue(relation.include_policy.schema)
        self.assertTrue(relation.include_policy.identifier)

    def test_quote_character(self):
        """Test quote character is backtick"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table"
        )
        self.assertEqual(relation.quote_character, "`")

    def test_database_schema_mismatch_raises_error(self):
        """Test that setting database different from schema raises error"""
        with self.assertRaises(DbtRuntimeError) as context:
            SparkRelation.create(
                database="different_db",
                schema="my_schema",
                identifier="my_table"
            )
        self.assertIn("Cannot set database in spark", str(context.exception))

    def test_database_schema_same_allowed(self):
        """Test that database and schema can be the same"""
        # This should not raise an error
        relation = SparkRelation.create(
            database="my_schema",
            schema="my_schema",
            identifier="my_table"
        )
        self.assertEqual(relation.database, relation.schema)

    def test_render_with_both_database_and_schema_raises_error(self):
        """Test that rendering with both database and schema in include policy raises error"""
        # Create a relation with custom include policy
        from dbt.adapters.watsonx_spark.relation import SparkIncludePolicy
        
        # Manually create a policy that includes both (which is invalid)
        invalid_policy = SparkIncludePolicy(database=True, schema=True, identifier=True)
        
        with self.assertRaises(DbtRuntimeError) as context:
            relation = SparkRelation.create(
                schema="my_schema",
                identifier="my_table",
                include_policy=invalid_policy
            )
            relation.render()
        
        self.assertIn("only one can be set", str(context.exception))

    def test_set_location(self):
        """Test set_location method"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table"
        )
        
        # Initially location_root should be None
        self.assertIsNone(relation.location_root)
        
        # Set location
        relation.set_location("s3://my-bucket/path")
        
        # Verify location was set
        self.assertEqual(relation.location_root, "s3://my-bucket/path")

    def test_is_delta_attribute(self):
        """Test is_delta attribute"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            is_delta=True
        )
        self.assertTrue(relation.is_delta)
        
        relation2 = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            is_delta=False
        )
        self.assertFalse(relation2.is_delta)

    def test_is_hudi_attribute(self):
        """Test is_hudi attribute"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            is_hudi=True
        )
        self.assertTrue(relation.is_hudi)

    def test_is_iceberg_attribute(self):
        """Test is_iceberg attribute"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            is_iceberg=True
        )
        self.assertTrue(relation.is_iceberg)

    def test_information_attribute(self):
        """Test information attribute"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            information="some metadata"
        )
        self.assertEqual(relation.information, "some metadata")

    def test_catalog_attribute(self):
        """Test catalog attribute"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            catalog="my_catalog"
        )
        self.assertEqual(relation.catalog, "my_catalog")

    def test_render_basic(self):
        """Test basic render without database"""
        relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table"
        )
        rendered = relation.render()
        # Should include schema and quoted identifier
        self.assertIn("my_schema", rendered)
        self.assertIn("my_table", rendered)

    def test_create_with_type(self):
        """Test creating relation with specific type"""
        table_relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_table",
            type=SparkRelation.Table
        )
        self.assertEqual(table_relation.type, SparkRelation.Table)
        
        view_relation = SparkRelation.create(
            schema="my_schema",
            identifier="my_view",
            type=SparkRelation.View
        )
        self.assertEqual(view_relation.type, SparkRelation.View)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
