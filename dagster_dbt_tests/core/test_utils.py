import json
from pathlib import Path
from unittest import mock

import pytest
from dagster_dbt.errors import DagsterDbtCoreNotInstalledError
from dagster_dbt.utils import select_unique_ids

CLOUD_MANIFEST_PATH = Path(__file__).parents[1] / "cloud_v2" / "manifest.json"


def test_select_unique_ids_without_dbt_core() -> None:
    """Without dbt-core and without a project, selection is evaluated against the manifest, and a
    selection the manifest cannot answer raises with a pointer to `DbtProject`.
    """
    manifest = json.loads(CLOUD_MANIFEST_PATH.read_text())

    def _select(select: str, exclude: str = "", selector: str = "") -> set[str]:
        selected = select_unique_ids(
            select=select, exclude=exclude, selector=selector, project=None, manifest_json=manifest
        )
        return {unique_id for unique_id in selected if not unique_id.startswith("test.")}

    with mock.patch("dagster_dbt.utils.DBT_PYTHON_VERSION", None):
        for select in ("fqn:*", ""):
            assert (
                select_unique_ids(
                    select=select, exclude="", selector="", project=None, manifest_json=manifest
                )
                == manifest["nodes"].keys()
            )

        assert _select("stg_customers+") == {
            "model.jaffle_shop.stg_customers",
            "model.jaffle_shop.customers",
        }
        assert _select("fqn:*", exclude="resource_type:seed resource_type:test") == {
            unique_id for unique_id in manifest["nodes"] if unique_id.startswith("model.")
        }
        assert _select("", selector="raw_customer_child_models") == {
            "model.jaffle_shop.stg_customers",
            "model.jaffle_shop.customers",
        }

        for select, selector in (("state:modified", ""), ("", "missing_selector")):
            with pytest.raises(DagsterDbtCoreNotInstalledError, match="Pass a `DbtProject`"):
                select_unique_ids(
                    select=select,
                    exclude="",
                    selector=selector,
                    project=None,
                    manifest_json=manifest,
                )


def test_select_unique_ids_default_matches_dbt_core() -> None:
    """The no-dbt-core fast path must agree with what dbt-core's selector returns."""
    pytest.importorskip("dbt.version")

    manifest = json.loads(CLOUD_MANIFEST_PATH.read_text())
    from_dbt_core = select_unique_ids(
        select="fqn:*", exclude="", selector="", project=None, manifest_json=manifest
    )
    with mock.patch("dagster_dbt.utils.DBT_PYTHON_VERSION", None):
        without_dbt_core = select_unique_ids(
            select="fqn:*", exclude="", selector="", project=None, manifest_json=manifest
        )

    assert from_dbt_core == without_dbt_core


def test_select_all_on_fusion_keeps_isolated_nodes() -> None:
    """A Fusion project's default selection comes from the manifest, not from `dbt list`.

    Fusion leaves a node with neither parents nor children out of the graph `dbt list` walks,
    so shelling out would silently drop it from the asset graph.
    """
    manifest = json.loads(CLOUD_MANIFEST_PATH.read_text())
    manifest["metadata"]["dbt_version"] = "2.0.6"
    isolated = "model.jaffle_shop.isolated"
    manifest["nodes"][isolated] = {
        "unique_id": isolated,
        "name": "isolated",
        "resource_type": "model",
        "config": {"materialized": "table"},
        "depends_on": {"nodes": []},
    }

    project = mock.MagicMock()
    with (
        mock.patch("dagster_dbt.utils.DBT_PYTHON_VERSION", None),
        mock.patch("dagster_dbt.utils._select_unique_ids_from_cli") as from_cli,
    ):
        selected = select_unique_ids(
            select="fqn:*", exclude="", selector="", project=project, manifest_json=manifest
        )

    assert isolated in selected
    assert selected == manifest["nodes"].keys()
    from_cli.assert_not_called()
