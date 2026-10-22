import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from dagster import AssetKey, AssetMaterialization, TableColumn, TableSchema
from dagster_dbt.core.dbt_cli_invocation import _get_profile_and_vars_args
from dagster_dbt.core.dbt_event_iterator import (
    DbtEventIterator,
    _fetch_fusion_column_metadata,
    _fetch_fusion_row_counts,
)
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

MANIFEST = {
    "metadata": {"adapter_type": "duckdb"},
    "nodes": {
        "model.jaffle.orders": {
            "unique_id": "model.jaffle.orders",
            "resource_type": "model",
            "fqn": ["jaffle", "orders"],
            "relation_name": '"db"."main"."orders"',
            "config": {"materialized": "table"},
            "depends_on": {"nodes": ["seed.jaffle.raw_orders"]},
        },
        "model.jaffle.orders_view": {
            "unique_id": "model.jaffle.orders_view",
            "resource_type": "model",
            "fqn": ["jaffle", "orders_view"],
            "relation_name": '"db"."main"."orders_view"',
            "config": {"materialized": "view"},
            "depends_on": {"nodes": ["model.jaffle.orders"]},
        },
        "seed.jaffle.raw_orders": {
            "unique_id": "seed.jaffle.raw_orders",
            "resource_type": "seed",
            "fqn": ["jaffle", "raw_orders"],
            "relation_name": '"db"."main"."raw_orders"',
            "config": {},
            "depends_on": {"nodes": []},
        },
    },
    "sources": {},
}


class _FakeInvocation:
    def __init__(self, target_path: Path, events: list[dict[str, Any]]) -> None:
        self.manifest = MANIFEST
        self.target_path = target_path
        self.dagster_dbt_translator = DagsterDbtTranslator()
        self.project = None
        self.is_fusion = True
        self.commands: list[Sequence[str]] = []
        self._events = events

    def _run_metadata_command(self, args: Sequence[str]) -> tuple[list[dict[str, Any]], Path]:
        self.commands.append(args)
        return self._events, self.target_path


def _show_node(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"info": {"name": "ShowNode"}, "data": {"preview": json.dumps(rows)}}


def test_fusion_row_counts_use_one_show_query(tmp_path: Path) -> None:
    """Row counts for every table come from one `dbt show` union query, and views are skipped."""
    invocation = _FakeInvocation(
        tmp_path, [_show_node([{"row_index": 0, "row_count": 3}, {"row_index": 1, "row_count": 7}])]
    )

    metadata = _fetch_fusion_row_counts(
        invocation,  # ty: ignore[invalid-argument-type]
        ["model.jaffle.orders", "model.jaffle.orders_view", "seed.jaffle.raw_orders"],
    )

    assert {uid: m["dagster/row_count"] for uid, m in metadata.items()} == {
        "model.jaffle.orders": 3,
        "seed.jaffle.raw_orders": 7,
    }
    assert metadata["model.jaffle.orders"]["dagster/storage_kind"] == "duckdb"
    [command] = invocation.commands
    assert command[0] == "show"
    assert command[2] == (
        'select 0 as row_index, count(*) as row_count from "db"."main"."orders" union all'
        ' select 1 as row_index, count(*) as row_count from "db"."main"."raw_orders"'
    )


def test_fusion_column_metadata_reads_the_catalog(tmp_path: Path) -> None:
    """Column schemas come from the catalog that `dbt compile --write-catalog` writes."""
    tmp_path.joinpath("catalog.json").write_text(
        json.dumps(
            {
                "nodes": {
                    "model.jaffle.orders": {
                        "columns": {
                            "id": {"type": "BIGINT", "index": 1},
                            "amount": {"type": "DOUBLE", "index": 2},
                        }
                    }
                },
                "sources": {},
            }
        )
    )
    invocation = _FakeInvocation(tmp_path, [])

    metadata = _fetch_fusion_column_metadata(
        invocation,  # ty: ignore[invalid-argument-type]
        ["model.jaffle.orders"],
        with_column_lineage=False,
    )

    assert metadata["model.jaffle.orders"]["dagster/column_schema"] == TableSchema(
        columns=[TableColumn("id", "BIGINT"), TableColumn("amount", "DOUBLE")]
    )
    assert invocation.commands == [
        ["compile", "--select", "1+fqn:jaffle.orders", "--write-catalog"]
    ]


def test_fusion_metadata_failure_keeps_events(tmp_path: Path) -> None:
    """A failed metadata command leaves the events unchanged instead of failing the step."""

    class _FailingInvocation(_FakeInvocation):
        def _run_metadata_command(self, args: Sequence[str]) -> tuple[list[dict[str, Any]], Path]:
            raise RuntimeError("warehouse unavailable")

    event = AssetMaterialization(
        asset_key=AssetKey("orders"), metadata={"unique_id": "model.jaffle.orders"}
    )
    events = DbtEventIterator(
        iter([event]),
        _FailingInvocation(tmp_path, []),  # ty: ignore[invalid-argument-type]
    )

    assert list(events.fetch_row_counts()) == [event]


def test_profile_and_vars_args_are_kept() -> None:
    """Metadata commands reuse the profile, target and vars of the invocation they follow."""
    args = ["build", "--select", "a", "--profile", "p", "-t", "dev", "--vars={x: 1}", "--fail-fast"]

    assert _get_profile_and_vars_args(args) == ["--profile", "p", "-t", "dev", "--vars={x: 1}"]
