import json

from hierarchy_engine.serializer import HierarchyVersionSerializer
from tests.helpers import build_definition


def test_serializer_writes_canonical_payload_json():
    """
    What: Serializes a published hierarchy version row with canonical payload JSON and persisted metadata.
    Why: The authoritative version row has to capture both queryable columns and the exact reconstructable payload.
    Fails when: Payload JSON drops hierarchy fields, lifecycle metadata is wrong, or child nodes stop serializing.
    """
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
    assert row.effective_start_date == "2026-04-26"
    assert row.effective_end_date == "2999-12-31"
    assert payload["hierarchy_id"] == "TEST"
    assert payload["version"] == "V1"
    assert payload["nodes"][0]["children"][0]["account_key"] == "10100"


def test_serializer_round_trips_definition():
    """
    What: Reconstructs a hierarchy definition from a serialized version row.
    Why: `payload_json` is intended to be the authoritative source for rebuild and recovery workflows.
    Fails when: Deserialization loses metadata, child structure, or the exporter/serializer payload contracts diverge.
    """
    definition = build_definition()
    serializer = HierarchyVersionSerializer()

    row = serializer.serialize_version(definition, status="published")
    reconstructed = serializer.deserialize_version(row)

    assert reconstructed.metadata.hierarchy_id == definition.metadata.hierarchy_id
    assert reconstructed.metadata.version == definition.metadata.version
    assert reconstructed.metadata.owner == definition.metadata.owner
    assert reconstructed.nodes[0].children[0].account_name == "Investments"


def test_serializer_computes_summary_metrics():
    """
    What: Computes node count, leaf count, max depth, and content hash during version serialization.
    Why: Persisted summary metrics support quick inspection and downstream validation without re-flattening the payload.
    Fails when: Hierarchy statistics drift from the canonical payload or the content hash stops being generated.
    """
    row = HierarchyVersionSerializer().serialize_version(
        build_definition(),
        status="published",
    )

    assert row.node_count == 2
    assert row.leaf_count == 1
    assert row.max_depth == 2
    assert len(row.content_hash) == 64


def test_serializer_accepts_explicit_effective_dates():
    """
    What: Allows callers to override effective dates while still keeping them non-null.
    Why: Publish-time dating belongs to persistence metadata, not the authored YAML payload.
    Fails when: Explicit effective windows are ignored or written into the canonical payload.
    """
    row = HierarchyVersionSerializer().serialize_version(
        build_definition(),
        status="published",
        published_at="2026-04-26T12:00:00Z",
        effective_start_date="2026-05-01",
        effective_end_date="2026-12-31",
    )

    payload = json.loads(row.payload_json)
    assert row.effective_start_date == "2026-05-01"
    assert row.effective_end_date == "2026-12-31"
    assert "effective_start_date" not in payload
    assert "effective_end_date" not in payload

