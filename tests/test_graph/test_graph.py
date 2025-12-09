import pytest
import implica


class TestGraph:
    def test_graph_init(self):
        graph = implica.Graph()
        assert isinstance(graph, implica.Graph)
