import yaml

from hierarchy_engine.exporter import HierarchyYamlExporter
from hierarchy_engine.models import HierarchyNode
from tests.helpers import build_definition


def test_export_to_yaml():
    """
    What: Serializes a definition into the canonical root-level YAML payload.
    Why: Exported YAML is the human-facing artifact and must preserve hierarchy metadata and nesting.
    Fails when: Root metadata moves under a wrapper, node keys are omitted, or child nesting is flattened.
    """
    definition = build_definition()

    yaml_text = HierarchyYamlExporter().to_yaml(definition)

    payload = yaml.safe_load(yaml_text)
    assert payload["hierarchy_id"] == "TEST"
    assert payload["nodes"][0]["account_key"] == "10000"
    assert payload["nodes"][0]["children"][0]["account_name"] == "Investments"


def test_exporter_to_dict_omits_empty_children():
    """
    What: Omits the `children` field when a node has no descendants.
    Why: Canonical payloads should stay minimal instead of emitting empty collections into every leaf node.
    Fails when: Leaf nodes serialize noisy empty `children` arrays into persisted or authored payloads.
    """
    definition = build_definition(nodes=[HierarchyNode(account_key="10000", account_name="Assets")])

    payload = HierarchyYamlExporter().to_dict(definition)

    assert "children" not in payload["nodes"][0]


def test_write_yaml_writes_serialized_payload(monkeypatch):
    """
    What: Writes the serialized canonical payload to disk with UTF-8 encoding.
    Why: File export should use the same payload contract as in-memory YAML rendering and not drift at write time.
    Fails when: The exporter writes to the wrong path, uses the wrong encoding, or mutates the serialized payload.
    """
    definition = build_definition()
    captured = {}

    def fake_write_text(self, text, encoding=None):
        assert str(self) == "roundtrip.yaml"
        assert encoding == "utf-8"
        captured["text"] = text
        return len(text)

    monkeypatch.setattr("pathlib.Path.write_text", fake_write_text)

    HierarchyYamlExporter().write_yaml(definition, "roundtrip.yaml")

    payload = yaml.safe_load(captured["text"])
    assert payload["hierarchy_id"] == definition.metadata.hierarchy_id
    assert payload["nodes"][0]["children"][0]["account_key"] == "10100"

