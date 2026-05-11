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
        "version": "V1",
        "owner": "Finance",
        "owner_department": "ALM",
        "description": "Test description",
    }
    metadata_data.update(metadata_overrides)

    metadata = HierarchyMetadata(**metadata_data)
    return HierarchyDefinition(metadata=metadata, nodes=nodes)
