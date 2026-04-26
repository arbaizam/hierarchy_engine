import json

from hierarchy_engine.serializer import HierarchyVersionSerializer
from tests.helpers import build_definition


def test_serializer_writes_canonical_payload_json():
    definition = build_definition()

    row = HierarchyVersionSerializer().serialize_version(
        definition,
        status="published",
        published_by="engineer",
        published_at="2026-04-26T12:00:00Z",
    )

    payload = json.loads(row.payload_json)
    assert row.hierarchy_id == "TEST"
    assert row.version == "V1"
    assert row.status == "published"
    assert payload["hierarchy_id"] == "TEST"
    assert payload["version"] == "V1"
    assert payload["nodes"][0]["children"][0]["account_key"] == "10100"


def test_serializer_round_trips_definition():
    definition = build_definition()
    serializer = HierarchyVersionSerializer()

    row = serializer.serialize_version(definition, status="published")
    reconstructed = serializer.deserialize_version(row)

    assert reconstructed.metadata.hierarchy_id == definition.metadata.hierarchy_id
    assert reconstructed.metadata.version == definition.metadata.version
    assert reconstructed.metadata.owner == definition.metadata.owner
    assert reconstructed.nodes[0].children[0].account_name == "Investments"


def test_serializer_computes_summary_metrics():
    row = HierarchyVersionSerializer().serialize_version(
        build_definition(),
        status="published",
    )

    assert row.node_count == 2
    assert row.leaf_count == 1
    assert row.max_depth == 2
    assert len(row.content_hash) == 64
