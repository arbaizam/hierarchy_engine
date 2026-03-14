from io import StringIO
from unittest.mock import patch

import yaml

from hierarchy_engine.exporter import HierarchyYamlExporter
from hierarchy_engine.models import HierarchyNode
from tests.helpers import build_definition


class NonClosingStringIO(StringIO):
    def close(self):
        self.seek(0)


def test_export_to_yaml():
    definition = build_definition()

    yaml_text = HierarchyYamlExporter().to_yaml(definition)

    payload = yaml.safe_load(yaml_text)
    assert payload["hierarchy"]["hierarchy_id"] == "TEST"
    assert payload["hierarchy"]["nodes"][0]["account_key"] == "10000"
    assert payload["hierarchy"]["nodes"][0]["children"][0]["account_name"] == "Investments"


def test_exporter_to_dict_omits_empty_children():
    definition = build_definition(nodes=[HierarchyNode(account_key="10000", account_name="Assets")])

    payload = HierarchyYamlExporter().to_dict(definition)

    assert "children" not in payload["hierarchy"]["nodes"][0]


def test_write_yaml_writes_serialized_payload():
    definition = build_definition()
    file_buffer = NonClosingStringIO()

    def fake_open(path, mode, encoding=None):
        assert path == "roundtrip.yaml"
        assert mode == "w"
        assert encoding == "utf-8"
        return file_buffer

    with patch("builtins.open", fake_open):
        HierarchyYamlExporter().write_yaml(definition, "roundtrip.yaml")

    payload = yaml.safe_load(file_buffer.getvalue())
    assert payload["hierarchy"]["hierarchy_id"] == definition.metadata.hierarchy_id
    assert payload["hierarchy"]["nodes"][0]["children"][0]["account_key"] == "10100"


def test_exporter_serializes_non_date_values_tolerantly():
    definition = build_definition(
        metadata_overrides={"effective_start_date": "2026-01-01"}
    )

    payload = HierarchyYamlExporter().to_dict(definition)

    assert payload["hierarchy"]["effective_start_date"] == "2026-01-01"
