"""
Tests for IndexConfig and Bloom Filter functionality.

This module tests the configuration system for graph indexing optimization,
including bloom filter activation and graph performance with different configs.
"""

import pytest
import implica


class TestIndexConfig:
    """Tests for IndexConfig creation and validation."""

    def test_default_config(self):
        """Test that default config has bloom filters disabled."""
        config = implica.IndexConfig()

        assert config.bloom_filter_fpr is None
        assert config.estimated_size is None
        assert not config.has_bloom_filters()

    def test_config_with_bloom_enabled(self):
        """Test creating config with bloom filters enabled."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=100_000)

        assert config.bloom_filter_fpr == 0.01
        assert config.estimated_size == 100_000
        assert config.has_bloom_filters()

    def test_config_bloom_only(self):
        """Test config with bloom FPR but no size estimate."""
        config = implica.IndexConfig(bloom_filter_fpr=0.05)

        assert config.bloom_filter_fpr == 0.05
        assert config.estimated_size is None
        assert config.has_bloom_filters()

    def test_config_size_only(self):
        """Test config with size estimate but bloom disabled."""
        config = implica.IndexConfig(estimated_size=50_000)

        assert config.bloom_filter_fpr is None
        assert config.estimated_size == 50_000
        assert not config.has_bloom_filters()

    def test_config_invalid_fpr_too_low(self):
        """Test that FPR <= 0 raises ValueError."""
        with pytest.raises(ValueError, match="must be in range"):
            implica.IndexConfig(bloom_filter_fpr=0.0)

        with pytest.raises(ValueError, match="must be in range"):
            implica.IndexConfig(bloom_filter_fpr=-0.01)

    def test_config_invalid_fpr_too_high(self):
        """Test that FPR >= 1 raises ValueError."""
        with pytest.raises(ValueError, match="must be in range"):
            implica.IndexConfig(bloom_filter_fpr=1.0)

        with pytest.raises(ValueError, match="must be in range"):
            implica.IndexConfig(bloom_filter_fpr=1.5)

    def test_config_str_representation(self):
        """Test string representation of config."""
        config1 = implica.IndexConfig()
        assert "bloom_filters=disabled" in str(config1)

        config2 = implica.IndexConfig(bloom_filter_fpr=0.01)
        assert "bloom_fpr=1.0%" in str(config2)

        config3 = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1_000_000)
        assert "bloom_fpr=1.0%" in str(config3)
        assert "estimated_size=1000000" in str(config3)

    def test_config_repr(self):
        """Test repr is same as str."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01)
        assert repr(config) == str(config)


class TestIndexConfigAutoSizing:
    """Tests for automatic configuration based on graph size."""

    def test_for_graph_size_small(self):
        """Test auto-config for small graphs (<10K types)."""
        config = implica.IndexConfig.for_graph_size(5_000)

        assert config.bloom_filter_fpr is None
        assert not config.has_bloom_filters()

    def test_for_graph_size_tiny(self):
        """Test auto-config for tiny graphs."""
        config = implica.IndexConfig.for_graph_size(100)

        assert not config.has_bloom_filters()

    def test_for_graph_size_medium(self):
        """Test auto-config for medium graphs (10K-100K types)."""
        config = implica.IndexConfig.for_graph_size(50_000)

        assert config.bloom_filter_fpr == 0.001  # 0.1% FPR
        assert config.estimated_size == 50_000
        assert config.has_bloom_filters()

    def test_for_graph_size_large(self):
        """Test auto-config for large graphs (100K-1M types)."""
        config = implica.IndexConfig.for_graph_size(500_000)

        assert config.bloom_filter_fpr == 0.01  # 1% FPR
        assert config.estimated_size == 500_000
        assert config.has_bloom_filters()

    def test_for_graph_size_very_large(self):
        """Test auto-config for very large graphs (>1M types)."""
        config = implica.IndexConfig.for_graph_size(5_000_000)

        assert config.bloom_filter_fpr == 0.05  # 5% FPR
        assert config.estimated_size == 5_000_000
        assert config.has_bloom_filters()

    def test_for_graph_size_boundary_10k(self):
        """Test boundary at 10K types."""
        config_below = implica.IndexConfig.for_graph_size(10_000)
        config_above = implica.IndexConfig.for_graph_size(10_001)

        assert not config_below.has_bloom_filters()
        assert config_above.has_bloom_filters()

    def test_for_graph_size_boundary_100k(self):
        """Test boundary at 100K types."""
        config_below = implica.IndexConfig.for_graph_size(100_000)
        config_above = implica.IndexConfig.for_graph_size(100_001)

        assert config_below.bloom_filter_fpr == 0.001
        assert config_above.bloom_filter_fpr == 0.01

    def test_for_graph_size_boundary_1m(self):
        """Test boundary at 1M types."""
        config_below = implica.IndexConfig.for_graph_size(1_000_000)
        config_above = implica.IndexConfig.for_graph_size(1_000_001)

        assert config_below.bloom_filter_fpr == 0.01
        assert config_above.bloom_filter_fpr == 0.05


class TestGraphWithConfig:
    """Tests for Graph creation with IndexConfig."""

    def test_graph_default_no_config(self):
        """Test creating graph without config (default behavior)."""
        graph = implica.Graph()

        assert graph is not None
        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0

    def test_graph_with_default_config(self):
        """Test creating graph with explicit default config."""
        config = implica.IndexConfig()
        graph = implica.Graph(config)

        assert graph is not None
        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0

    def test_graph_with_bloom_enabled(self):
        """Test creating graph with bloom filters enabled."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=100_000)
        graph = implica.Graph(config)

        assert graph is not None
        assert len(graph.nodes) == 0
        assert len(graph.edges) == 0

    def test_graph_with_auto_config(self):
        """Test creating graph with auto-configured settings."""
        config = implica.IndexConfig.for_graph_size(500_000)
        graph = implica.Graph(config)

        assert graph is not None
        assert len(graph.nodes) == 0


class TestGraphOperationsWithBloom:
    """Tests that graph operations work correctly with bloom filters enabled."""

    def test_add_nodes_with_bloom(self):
        """Test adding nodes to graph with bloom filters."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1000)
        graph = implica.Graph(config)

        # Create and add nodes
        type_a = implica.Variable("A")
        type_b = implica.Variable("B")

        node1 = implica.Node(type_a, properties={"name": "node1"})
        node2 = implica.Node(type_b, properties={"name": "node2"})

        graph.nodes[node1.uid()] = node1
        graph.nodes[node2.uid()] = node2

        assert len(graph.nodes) == 2

    def test_query_with_bloom_enabled(self):
        """Test querying nodes with bloom filter enabled."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1000)
        graph = implica.Graph(config)

        type_a = implica.Variable("A")
        # Use query().create().execute() to properly index the node
        graph.query().create(node="n", type=type_a, properties={"value": 42}).execute()

        # Query should work normally
        q = graph.query()
        results = q.match(node="n", type=type_a).return_("n")

    def test_graph_without_bloom_still_works(self):
        """Test that graphs without bloom filters still work (regression test)."""
        graph = implica.Graph()

        type_a = implica.Variable("A")
        # Use query().create().execute() to properly index the node
        graph.query().create(node="n", type=type_a, properties={"value": 42}).execute()

        q = graph.query()
        results = q.match(node="n", type=type_a).return_("n")

        assert len(results) == 1

    def test_complex_query_with_bloom(self):
        """Test complex queries work with bloom filters."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1000)
        graph = implica.Graph(config)

        # Create types
        type_a = implica.Variable("A")
        type_b = implica.Variable("B")
        type_func = implica.Arrow(type_a, type_b)

        # Create nodes using query().create().execute()
        graph.query().create(node="na", type=type_a).execute()
        graph.query().create(node="nb", type=type_b).execute()
        graph.query().create(node="nf", type=type_func).execute()

        # Get the created nodes
        node_func_results = graph.query().match(node="n", type=type_func).return_("n")
        node_func = (
            node_func_results[0]["n"] if len(node_func_results) > 0 else implica.Node(type_func)
        )

        # Query with type schema
        q = graph.query()
        results = q.match(node="n", type_schema="A -> B").return_("n")

        assert len(results) == 1
        assert results[0]["n"].uid() == node_func.uid()


class TestBloomFilterPerformance:
    """Tests to verify bloom filter behavior (conceptual, not strict benchmarks)."""

    def test_large_graph_creation(self):
        """Test creating a larger graph with bloom filters."""
        config = implica.IndexConfig.for_graph_size(10_000)
        graph = implica.Graph(config)

        # Add many nodes
        for i in range(100):
            type_var = implica.Variable(f"Type{i}")
            node = implica.Node(type_var, properties={"index": i})
            graph.nodes[node.uid()] = node

        assert len(graph.nodes) == 100

    def test_bloom_vs_no_bloom_same_results(self):
        """Test that bloom and non-bloom graphs return same query results."""
        # Create two identical graphs
        graph_no_bloom = implica.Graph()
        config_bloom = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=100)
        graph_bloom = implica.Graph(config_bloom)

        # Add same nodes to both using query().create().execute()
        types = [implica.Variable(f"T{i}") for i in range(10)]
        for i, typ in enumerate(types):
            graph_no_bloom.query().create(node=f"n{i}", type=typ).execute()
            graph_bloom.query().create(node=f"n{i}", type=typ).execute()

        # Query both
        results_no_bloom = graph_no_bloom.query().match(node="n").return_("n")
        results_bloom = graph_bloom.query().match(node="n").return_("n")

        # Should have same number of results
        assert len(results_no_bloom) == len(results_bloom) == 10


class TestBloomFilterEdgeCases:
    """Tests for edge cases and corner scenarios."""

    def test_zero_estimated_size(self):
        """Test config with zero estimated size."""
        # Should not crash, bloom filters just won't be very effective
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=0)
        graph = implica.Graph(config)
        assert graph is not None

    def test_very_low_fpr(self):
        """Test with very low false positive rate (high accuracy)."""
        config = implica.IndexConfig(bloom_filter_fpr=0.0001)  # 0.01%
        graph = implica.Graph(config)

        type_a = implica.Variable("A")
        # Use query().create().execute() to properly index the node
        graph.query().create(node="n", type=type_a).execute()

        results = graph.query().match(node="n", type=type_a).return_("n")
        assert len(results) == 1

    def test_very_high_fpr(self):
        """Test with high false positive rate (memory efficient)."""
        config = implica.IndexConfig(bloom_filter_fpr=0.1)  # 10%
        graph = implica.Graph(config)

        type_a = implica.Variable("A")
        # Use query().create().execute() to properly index the node
        graph.query().create(node="n", type=type_a).execute()

        results = graph.query().match(node="n", type=type_a).return_("n")
        assert len(results) == 1

    def test_empty_graph_with_bloom(self):
        """Test querying empty graph with bloom filters."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1000)
        graph = implica.Graph(config)

        results = graph.query().match(node="n").return_("n")
        assert len(results) == 0

    def test_single_node_graph_bloom(self):
        """Test graph with single node and bloom filters."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01, estimated_size=1)
        graph = implica.Graph(config)

        # Use query().create().execute() to properly index the node
        graph.query().create(node="n", type=implica.Variable("Singleton")).execute()

        results = graph.query().match(node="n").return_("n")
        assert len(results) == 1


class TestConfigMutability:
    """Tests for config property access and modification."""

    def test_config_properties_readable(self):
        """Test that config properties can be read."""
        config = implica.IndexConfig(bloom_filter_fpr=0.02, estimated_size=50_000)

        assert config.bloom_filter_fpr == 0.02
        assert config.estimated_size == 50_000

    def test_config_properties_writable(self):
        """Test that config properties can be modified."""
        config = implica.IndexConfig()

        # Initially disabled
        assert not config.has_bloom_filters()

        # Enable bloom filters
        config.bloom_filter_fpr = 0.01
        config.estimated_size = 100_000

        assert config.has_bloom_filters()
        assert config.bloom_filter_fpr == 0.01
        assert config.estimated_size == 100_000

    def test_config_disable_bloom(self):
        """Test disabling bloom filters by setting to None."""
        config = implica.IndexConfig(bloom_filter_fpr=0.01)
        assert config.has_bloom_filters()

        config.bloom_filter_fpr = None
        assert not config.has_bloom_filters()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
