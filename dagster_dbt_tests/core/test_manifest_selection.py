from typing import Any

import pytest
from dagster_dbt.manifest_selection import DbtSelectionError, select_unique_ids_from_manifest_json


def _node(unique_id: str, path: str, depends_on: list[str], **props: Any) -> dict[str, Any]:
    package, name = unique_id.split(".")[1:3]
    return {
        "unique_id": unique_id,
        "resource_type": unique_id.split(".")[0],
        "name": name,
        "package_name": package,
        "fqn": [package, *path.split("/")[1:-1], name],
        "original_file_path": path,
        "tags": props.pop("tags", []),
        "config": {"enabled": True, **props.pop("config", {})},
        "depends_on": {"nodes": depends_on},
        **props,
    }


NODES = [
    _node("seed.shop.raw_orders", "seeds/raw_orders.csv", [], tags=["raw"]),
    _node("model.shop.stg_orders", "models/staging/stg_orders.sql", ["seed.shop.raw_orders"]),
    _node(
        "model.shop.orders",
        "models/marts/orders.sql",
        ["model.shop.stg_orders"],
        config={"materialized": "table"},
    ),
    _node(
        "test.shop.not_null_orders_id",
        "models/marts/schema.yml",
        ["model.shop.orders"],
        test_metadata={"name": "not_null"},
    ),
    _node(
        "test.shop.relationships_orders",
        "models/marts/schema.yml",
        ["model.shop.orders", "model.shop.stg_orders"],
        test_metadata={"name": "relationships"},
    ),
    _node("model.shop.lonely", "models/lonely.sql", []),
]
PARENT_MAP = {node["unique_id"]: node["depends_on"]["nodes"] for node in NODES}
MANIFEST: dict[str, Any] = {
    "metadata": {"project_name": "shop"},
    "nodes": {node["unique_id"]: node for node in NODES},
    "sources": {},
    "parent_map": PARENT_MAP,
    "child_map": {
        unique_id: [child for child, parents in PARENT_MAP.items() if unique_id in parents]
        for unique_id in PARENT_MAP
    },
    "selectors": {
        "marts": {
            "name": "marts",
            "definition": {"union": [{"method": "path", "value": "models/marts"}]},
        }
    },
}


@pytest.mark.parametrize(
    ("select", "exclude", "selector", "expected"),
    [
        ("", "", "", {node["unique_id"] for node in NODES}),
        # Eager indirect selection adds every test that touches a selected node.
        ("stg_orders", "", "", {"model.shop.stg_orders", "test.shop.relationships_orders"}),
        (
            "stg_orders+",
            "",
            "",
            {
                "model.shop.stg_orders",
                "model.shop.orders",
                "test.shop.not_null_orders_id",
                "test.shop.relationships_orders",
            },
        ),
        ("+stg_orders,tag:raw", "", "", {"seed.shop.raw_orders"}),
        (
            "1+orders",
            "",
            "",
            {
                "model.shop.orders",
                "model.shop.stg_orders",
                "test.shop.not_null_orders_id",
                "test.shop.relationships_orders",
            },
        ),
        (
            "path:models/staging",
            "",
            "",
            {"model.shop.stg_orders", "test.shop.relationships_orders"},
        ),
        (
            "config.materialized:table",
            "",
            "",
            {"model.shop.orders", "test.shop.not_null_orders_id", "test.shop.relationships_orders"},
        ),
        ("test_name:not_null", "", "", {"test.shop.not_null_orders_id"}),
        (
            "fqn:*",
            "resource_type:test lonely",
            "",
            {"seed.shop.raw_orders", "model.shop.stg_orders", "model.shop.orders"},
        ),
        (
            "",
            "",
            "marts",
            {"model.shop.orders", "test.shop.not_null_orders_id", "test.shop.relationships_orders"},
        ),
    ],
)
def test_select(select: str, exclude: str, selector: str, expected: set[str]) -> None:
    """Selections resolve as dbt's own node selector resolves them."""
    assert select_unique_ids_from_manifest_json(select, exclude, selector, MANIFEST) == expected


def test_parent_map_stands_in_for_a_missing_child_map() -> None:
    """Early dbt Fusion manifests omit `child_map`, so graph operators read `parent_map`."""
    manifest = {**MANIFEST, "child_map": {}}

    assert select_unique_ids_from_manifest_json("raw_orders+1", "", "", manifest) == {
        "seed.shop.raw_orders",
        "model.shop.stg_orders",
        "test.shop.relationships_orders",
    }


@pytest.mark.parametrize("select", ["state:modified", "result:error", "nope:x"])
def test_selections_a_manifest_cannot_answer_raise(select: str) -> None:
    """Methods that need run artifacts, and unknown methods, raise instead of selecting nothing."""
    with pytest.raises(DbtSelectionError):
        select_unique_ids_from_manifest_json(select, "", "", MANIFEST)


@pytest.mark.parametrize(
    ("select", "selected"), [("path:models/pkg_only", False), ("path:models", True)]
)
def test_path_matches_package_resources_against_root_project_files(
    select: str, selected: bool
) -> None:
    """dbt globs files in the root project, so a package model matches only through a path the
    root project also has, such as the shared `models` directory.
    """
    package_node = {
        **NODES[1],
        "unique_id": "model.pkg.stg_orders",
        "package_name": "pkg",
        "original_file_path": "models/pkg_only/stg_orders.sql",
    }
    manifest = {**MANIFEST, "nodes": {**MANIFEST["nodes"], "model.pkg.stg_orders": package_node}}

    assert (
        "model.pkg.stg_orders" in select_unique_ids_from_manifest_json(select, "", "", manifest)
    ) == selected


@pytest.mark.parametrize(
    ("select", "expected"),
    [
        # globset: `*` crosses `/`, and `**/` matches zero directories.
        ("path:models/*.sql", {"model.shop.stg_orders", "model.shop.orders", "model.shop.lonely"}),
        ("path:models/**/lonely.sql", {"model.shop.lonely"}),
        # A package's files sit under `dbt_packages/<package>` relative to the root project.
        ("path:models/pkg_only", set()),
        ("path:dbt_packages", {"model.pkg.pkg_model"}),
    ],
)
def test_dbt_2_path_semantics(select: str, expected: set[str]) -> None:
    """A dbt 2.0 manifest gets dbt 2.0's path matching, which differs from dbt-core's."""
    package_node = _node("model.pkg.pkg_model", "models/pkg_only/pkg_model.sql", [])
    manifest = {
        **MANIFEST,
        "metadata": {"project_name": "shop", "dbt_version": "2.0.9"},
        "nodes": {**MANIFEST["nodes"], "model.pkg.pkg_model": package_node},
        "parent_map": {**PARENT_MAP, "model.pkg.pkg_model": []},
    }

    selected = select_unique_ids_from_manifest_json(select, "", "", manifest)

    assert {uid for uid in selected if uid.startswith("model.")} == expected
