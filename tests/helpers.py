from datetime import date

from hierarchy_engine.models import HierarchyDefinition, HierarchyMetadata, HierarchyNode


def build_definition(
    *,
    metadata_overrides=None,
    nodes=None,
):
    metadata_overrides = metadata_overrides or {}
    if nodes is None:
        nodes = [
            HierarchyNode(
                account_key="10000",
                account_name="Assets",
                children=[
                    HierarchyNode(
                        account_key="10100",
                        account_name="Investments",
                    )
                ],
            )
        ]

    metadata_data = {
        "hierarchy_id": "TEST",
        "hierarchy_name": "Test Hierarchy",
        "hierarchy_description": "Test description",
        "owner_team": "Finance",
        "business_domain": "ALM",
        "version_id": "V1",
        "version_name": "Version 1",
        "version_status": "draft",
        "effective_start_date": date(2026, 1, 1),
    }
    metadata_data.update(metadata_overrides)

    metadata = HierarchyMetadata(**metadata_data)
    return HierarchyDefinition(metadata=metadata, nodes=nodes)
