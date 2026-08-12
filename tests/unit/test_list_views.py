import unittest
from unittest import mock
from multiprocessing import get_context
import sys
import types

import dbt.flags as flags
from agate import Table
from pyhive import hive
from dbt.adapters.watsonx_spark import WatsonxSparkAdapter, SparkRelation
from .utils import config_from_parts_or_dicts


class _FakeAuthenticator:
    def get_token(self):
        return "dummy-token"

    def get_catlog_details(self, catalog_name):
        return ("", "parquet")


class TestListViews(unittest.TestCase):
    """Unit tests for _list_views_in_schema method"""

    def setUp(self):
        flags.STRICT_MODE = False

        self.auth_patcher = mock.patch(
            "dbt.adapters.watsonx_spark.connections.get_authenticator",
            return_value=_FakeAuthenticator(),
        )
        self.auth_patcher.start()
        self.addCleanup(self.auth_patcher.stop)

        # Provide a stub pyodbc module
        self.pyodbc_stub = types.SimpleNamespace(connect=lambda *args, **kwargs: None)
        sys.modules.setdefault("pyodbc", self.pyodbc_stub)
        import dbt.adapters.watsonx_spark.connections as conn_mod
        conn_mod.pyodbc = self.pyodbc_stub

        # Stub dbt.adapters.spark.* modules
        spark_connections_mod = types.ModuleType("dbt.adapters.spark.connections")
        spark_connections_mod.hive = hive
        spark_connections_mod.pyodbc = self.pyodbc_stub
        spark_pkg_mod = types.ModuleType("dbt.adapters.spark")
        spark_pkg_mod.connections = spark_connections_mod
        sys.modules["dbt.adapters.spark"] = spark_pkg_mod
        sys.modules["dbt.adapters.spark.connections"] = spark_connections_mod
        import dbt.adapters as adapters_pkg
        setattr(adapters_pkg, "spark", spark_pkg_mod)

        self.project_cfg = {
            "name": "X",
            "version": "0.1",
            "profile": "test",
            "project-root": "/tmp/dbt/does-not-exist",
            "quoting": {
                "identifier": False,
                "schema": False,
            },
            "config-version": 2,
        }

    def _get_target_iceberg(self, project):
        """Get config with Iceberg catalog"""
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "watsonx_spark",
                        "method": "http",
                        "schema": "analytics",
                        "host": "myorg.sparkhost.com",
                        "port": 443,
                        "token": "abc123",
                        "catalog": "iceberg_catalog",
                        "organization": "0123456789",
                        "cluster": "01234-23423-coffeetime",
                    }
                },
                "target": "test",
            },
        )

    def _get_target_no_catalog(self, project):
        """Get config without catalog (traditional Hive)"""
        return config_from_parts_or_dicts(
            project,
            {
                "outputs": {
                    "test": {
                        "type": "watsonx_spark",
                        "method": "http",
                        "schema": "analytics",
                        "host": "myorg.sparkhost.com",
                        "port": 443,
                        "token": "abc123",
                        "organization": "0123456789",
                        "cluster": "01234-23423-coffeetime",
                    }
                },
                "target": "test",
            },
        )

    def test_list_views_with_results(self):
        """Test _list_views_in_schema returns views when they exist"""
        config = self._get_target_iceberg(self.project_cfg)
        adapter = WatsonxSparkAdapter(config, get_context("spawn"))

        # Mock the connection execute to return view data
        mock_view_data = [
            ["iceberg_catalog.analytics", "view1", False],
            ["iceberg_catalog.analytics", "view2", False],
            ["iceberg_catalog.analytics", "view3", False],
        ]
        
        with mock.patch.object(adapter.connections, "execute") as mock_execute:
            mock_execute.return_value = (None, mock_view_data)
            
            schema_relation = SparkRelation.create(
                schema="iceberg_catalog.analytics",
                identifier=None
            )
            
            views = adapter._list_views_in_schema(schema_relation)
            
            # Verify execute was called with correct SQL
            mock_execute.assert_called_once_with(
                "show views in iceberg_catalog.analytics",
                auto_begin=False,
                fetch=True
            )
            
            # Verify results
            self.assertEqual(len(views), 3)
            self.assertEqual(views[0].identifier, "view1")
            self.assertEqual(views[1].identifier, "view2")
            self.assertEqual(views[2].identifier, "view3")
            
            # Verify all are views
            for view in views:
                self.assertEqual(view.type, SparkRelation.View)
                self.assertEqual(view.schema, "iceberg_catalog.analytics")

    def test_list_views_empty_result(self):
        """Test _list_views_in_schema returns empty list when no views exist"""
        config = self._get_target_iceberg(self.project_cfg)
        adapter = WatsonxSparkAdapter(config, get_context("spawn"))

        with mock.patch.object(adapter.connections, "execute") as mock_execute:
            mock_execute.return_value = (None, [])
            
            schema_relation = SparkRelation.create(
                schema="iceberg_catalog.analytics",
                identifier=None
            )
            
            views = adapter._list_views_in_schema(schema_relation)
            
            self.assertEqual(len(views), 0)
            self.assertIsInstance(views, list)

    def test_list_views_handles_exception(self):
        """Test _list_views_in_schema handles exceptions gracefully"""
        config = self._get_target_iceberg(self.project_cfg)
        adapter = WatsonxSparkAdapter(config, get_context("spawn"))

        with mock.patch.object(adapter.connections, "execute") as mock_execute:
            mock_execute.side_effect = Exception("SHOW VIEWS not supported")
            
            schema_relation = SparkRelation.create(
                schema="iceberg_catalog.analytics",
                identifier=None
            )
            
            # Should not raise exception, just return empty list
            views = adapter._list_views_in_schema(schema_relation)
            
            self.assertEqual(len(views), 0)
            self.assertIsInstance(views, list)

    def test_list_views_with_single_column_result(self):
        """Test _list_views_in_schema handles single column results"""
        config = self._get_target_iceberg(self.project_cfg)
        adapter = WatsonxSparkAdapter(config, get_context("spawn"))

        # Some Spark versions may return only view name
        mock_view_data = [
            ["view1"],
            ["view2"],
        ]
        
        with mock.patch.object(adapter.connections, "execute") as mock_execute:
            mock_execute.return_value = (None, mock_view_data)
            
            schema_relation = SparkRelation.create(
                schema="iceberg_catalog.analytics",
                identifier=None
            )
            
            views = adapter._list_views_in_schema(schema_relation)
            
            self.assertEqual(len(views), 2)
            self.assertEqual(views[0].identifier, "view1")
            self.assertEqual(views[1].identifier, "view2")

    def test_list_views_preserves_schema_path(self):
        """Test _list_views_in_schema preserves full catalog.schema path"""
        config = self._get_target_iceberg(self.project_cfg)
        adapter = WatsonxSparkAdapter(config, get_context("spawn"))

        mock_view_data = [
            ["iceberg_catalog.analytics", "my_view", False],
        ]
        
        with mock.patch.object(adapter.connections, "execute") as mock_execute:
            mock_execute.return_value = (None, mock_view_data)
            
            schema_relation = SparkRelation.create(
                schema="iceberg_catalog.analytics",
                identifier=None
            )
            
            views = adapter._list_views_in_schema(schema_relation)
            
            # Verify full schema path is preserved
            self.assertEqual(views[0].schema, "iceberg_catalog.analytics")
            self.assertIn("iceberg_catalog", views[0].schema)

    def test_list_views_with_none_result(self):
        """Test _list_views_in_schema handles None result from execute"""
        config = self._get_target_iceberg(self.project_cfg)
        adapter = WatsonxSparkAdapter(config, get_context("spawn"))

        with mock.patch.object(adapter.connections, "execute") as mock_execute:
            mock_execute.return_value = (None, None)
            
            schema_relation = SparkRelation.create(
                schema="iceberg_catalog.analytics",
                identifier=None
            )
            
            views = adapter._list_views_in_schema(schema_relation)
            
            self.assertEqual(len(views), 0)
            self.assertIsInstance(views, list)


if __name__ == "__main__":
    unittest.main()

# Made with Bob
