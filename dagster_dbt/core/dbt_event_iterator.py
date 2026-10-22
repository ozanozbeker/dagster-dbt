from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, TypeAlias, cast

import orjson
from dagster import (
    AssetCheckResult,
    AssetMaterialization,
    AssetObservation,
    Output,
    _check as check,
    get_dagster_logger,
)
from dagster._annotations import public
from dagster._core.definitions.asset_checks.asset_check_evaluation import AssetCheckEvaluation
from dagster._core.definitions.metadata import TableMetadataSet, TextMetadataValue
from dagster._core.errors import DagsterInvalidPropertyError
from dagster._core.utils import exhaust_iterator_and_yield_results_with_exception, imap
from dagster._utils import pushd
from typing_extensions import TypeVar

from dagster_dbt.asset_utils import default_metadata_from_dbt_resource_props
from dagster_dbt.core.dbt_cli_event import EventHistoryMetadata, _build_column_lineage_metadata

if TYPE_CHECKING:
    from dagster_dbt.core.dbt_cli_invocation import DbtCliInvocation


logger = get_dagster_logger()

DbtDagsterEventType: TypeAlias = (
    Output | AssetMaterialization | AssetCheckResult | AssetObservation | AssetCheckEvaluation
)


# We define DbtEventIterator as a generic type for the sake of type hinting.
# This is so that users who inspect the type of the return value of `DbtCliInvocation.stream()`
# will be able to see the inner type of the iterator, rather than just `DbtEventIterator`.
T = TypeVar("T", bound=DbtDagsterEventType)


def _get_dbt_resource_props_from_event(
    invocation: "DbtCliInvocation", event: DbtDagsterEventType
) -> dict[str, Any]:
    unique_id = cast("TextMetadataValue", event.metadata["unique_id"]).text
    return check.not_none(invocation.manifest["nodes"].get(unique_id))


def _fetch_column_metadata(
    invocation: "DbtCliInvocation", event: DbtDagsterEventType, with_column_lineage: bool
) -> dict[str, Any] | None:
    """Threaded task which fetches column schema and lineage metadata for dbt models in a dbt
    run once they are built, returning the metadata to be attached.

    First we use the dbt adapter to obtain the column metadata for the built model. Then we
    retrieve the column metadata for the model's parent models, if they exist. Finally, we
    build the column lineage metadata for the model and attach it to the event.

    Args:
        invocation (DbtCliInvocation): The dbt CLI invocation.
        event (DbtDagsterEventType): The dbt event to append column metadata to.
        with_column_lineage (bool): Whether to include column lineage metadata in the event.
    """
    adapter = invocation.adapter
    if adapter is None:
        return {}

    dbt_resource_props = _get_dbt_resource_props_from_event(invocation, event)

    with (
        pushd(str(invocation.project_dir)),
        adapter.connection_named(f"column_metadata_{dbt_resource_props['unique_id']}"),
    ):
        try:
            cols = invocation._get_columns_from_dbt_resource_props(  # noqa: SLF001
                adapter=adapter, dbt_resource_props=dbt_resource_props
            ).columns
        except Exception as e:
            logger.warning(
                "An error occurred while fetching column schema metadata for the dbt resource"
                f" `{dbt_resource_props['original_file_path']}`."
                " Column metadata will not be included in the event.\n\n"
                f"Exception: {e}",
                exc_info=True,
            )
            return {}

        schema_metadata = {}
        try:
            column_schema_data = {col.name: {"data_type": col.data_type} for col in cols}
            col_data = {"columns": column_schema_data}
            schema_metadata = default_metadata_from_dbt_resource_props(col_data)
        except Exception as e:
            logger.warning(
                "An error occurred while building column schema metadata from data"
                f" `{col_data}` for the dbt resource"
                f" `{dbt_resource_props['original_file_path']}`."
                " Column schema metadata will not be included in the event.\n\n"
                f"Exception: {e}",
                exc_info=True,
            )

        lineage_metadata = {}
        if with_column_lineage:
            try:
                parents = {}
                parent_unique_ids = invocation.manifest["parent_map"].get(
                    dbt_resource_props["unique_id"], []
                )
                for parent_unique_id in parent_unique_ids:
                    dbt_parent_resource_props = invocation.manifest["nodes"].get(
                        parent_unique_id
                    ) or invocation.manifest["sources"].get(parent_unique_id)

                    parent_name, parent_columns = invocation._get_columns_from_dbt_resource_props(  # noqa: SLF001
                        adapter=adapter, dbt_resource_props=dbt_parent_resource_props
                    )

                    parents[parent_name] = {
                        col.name: {"data_type": col.data_type} for col in parent_columns
                    }

                lineage_metadata = _build_column_lineage_metadata(
                    event_history_metadata=EventHistoryMetadata(
                        columns=column_schema_data,
                        parents=parents,
                    ),
                    dbt_resource_props=dbt_resource_props,
                    manifest=invocation.manifest,
                    dagster_dbt_translator=invocation.dagster_dbt_translator,
                    target_path=invocation.target_path,
                    project=invocation.project,
                )

            except Exception as e:
                logger.warning(
                    "An error occurred while building column lineage metadata for the dbt resource"
                    f" `{dbt_resource_props['original_file_path']}`."
                    " Lineage metadata will not be included in the event.\n\n"
                    f"Exception: {e}",
                    exc_info=True,
                )

        return {
            **schema_metadata,
            **lineage_metadata,
        }


def _fetch_row_count_metadata(
    invocation: "DbtCliInvocation",
    event: DbtDagsterEventType,
) -> dict[str, Any] | None:
    """Threaded task which fetches row counts for materialized dbt models in a dbt run
    once they are built, and attaches the row count as metadata to the event.
    """
    if not isinstance(event, (AssetMaterialization, Output)):
        return None

    adapter = invocation.adapter
    if adapter is None:
        return None

    dbt_resource_props = _get_dbt_resource_props_from_event(invocation, event)
    is_view = dbt_resource_props["config"]["materialized"] == "view"

    # Avoid counting rows for views, since they may include complex SQL queries
    # that are costly to execute. We can revisit this in the future if there is
    # a demand for it.
    if is_view:
        return None

    unique_id = dbt_resource_props["unique_id"]
    logger.debug("Fetching row count for %s", unique_id)
    relation_name = dbt_resource_props["relation_name"]

    try:
        with (
            pushd(str(invocation.project_dir)),
            adapter.connection_named(f"row_count_{unique_id}"),
        ):
            query_result = adapter.execute(
                f"""
                    SELECT
                    count(*) as row_count
                    FROM
                    {relation_name}
                """,
                fetch=True,
            )
        query_result_table = query_result[1]
        # some adapters do not output the column names, so we need
        # to index by position
        row_count = query_result_table[0][0]
        adapter_type = invocation.manifest.get("metadata", {}).get("adapter_type")
        return {**TableMetadataSet(row_count=row_count, storage_kind=adapter_type)}

    except Exception as e:
        logger.exception(
            f"An error occurred while fetching row count for {unique_id}. Row count metadata"
            " will not be included in the event.\n\n"
            f"Exception: {e}"
        )
        return None


# Keeps each dbt Fusion metadata command line under the 32,767 character limit on Windows.
_FUSION_MAX_COMMAND_CHARS = 8_000


def _batch_by_length(items: Sequence[str], max_chars: int) -> Iterator[list[str]]:
    batch: list[str] = []
    batch_chars = 0
    for item in items:
        if batch and batch_chars + len(item) > max_chars:
            yield batch
            batch, batch_chars = [], 0
        batch.append(item)
        batch_chars += len(item) + 1
    if batch:
        yield batch


def _fetch_fusion_row_counts(
    invocation: "DbtCliInvocation", unique_ids: Sequence[str]
) -> dict[str, dict[str, Any]]:
    """Count the rows of each built relation with `dbt show`, one query per batch of relations.

    Views are skipped for the same reason as `_fetch_row_count_metadata` skips them.
    """
    nodes = invocation.manifest["nodes"]
    counted_unique_ids = [
        unique_id
        for unique_id in unique_ids
        if nodes[unique_id].get("config", {}).get("materialized") != "view"
        and nodes[unique_id].get("relation_name")
    ]
    adapter_type = invocation.manifest.get("metadata", {}).get("adapter_type")

    # Each row carries its position in `selects`, so the result never depends on how the
    # warehouse cases or quotes a unique id.
    selects = [
        f"select {index} as row_index, count(*) as row_count"
        f" from {nodes[unique_id]['relation_name']}"
        for index, unique_id in enumerate(counted_unique_ids)
    ]
    metadata_by_unique_id: dict[str, dict[str, Any]] = {}
    for batch in _batch_by_length(selects, _FUSION_MAX_COMMAND_CHARS):
        events, _ = invocation._run_metadata_command(  # noqa: SLF001
            [
                "show",
                "--inline",
                " union all ".join(batch),
                "--limit",
                str(len(batch)),
                "--output",
                "json",
            ]
        )
        preview = next(
            event["data"]["preview"] for event in events if event["info"]["name"] == "ShowNode"
        )
        for row in orjson.loads(preview):
            row_index, row_count = row.values()
            metadata_by_unique_id[counted_unique_ids[int(row_index)]] = {
                **TableMetadataSet(row_count=int(row_count), storage_kind=adapter_type)
            }

    return metadata_by_unique_id


def _fetch_fusion_column_metadata(
    invocation: "DbtCliInvocation", unique_ids: Sequence[str], with_column_lineage: bool
) -> dict[str, dict[str, Any]]:
    """Read column schemas from the catalog that `dbt compile --write-catalog` writes for the
    built nodes and their parents, then build column lineage as `_fetch_column_metadata` does.
    """
    manifest = invocation.manifest
    selections = [
        f"1+fqn:{'.'.join(manifest['nodes'][unique_id]['fqn'])}" for unique_id in unique_ids
    ]
    catalog_relations: dict[str, Mapping[str, Any]] = {}
    for batch in _batch_by_length(selections, _FUSION_MAX_COMMAND_CHARS):
        _, metadata_target_path = invocation._run_metadata_command(  # noqa: SLF001
            ["compile", "--select", *batch, "--write-catalog"]
        )
        catalog = orjson.loads(metadata_target_path.joinpath("catalog.json").read_bytes())
        catalog_relations.update(catalog.get("nodes", {}))
        catalog_relations.update(catalog.get("sources", {}))

    def _columns(unique_id: str) -> dict[str, dict[str, Any]] | None:
        relation = catalog_relations.get(unique_id)
        if relation is None:
            return None
        return {
            column_name: {"data_type": column["type"]}
            for column_name, column in relation["columns"].items()
        }

    metadata_by_unique_id: dict[str, dict[str, Any]] = {}
    for unique_id in unique_ids:
        column_schema_data = _columns(unique_id)
        if column_schema_data is None:
            continue
        dbt_resource_props = manifest["nodes"][unique_id]
        metadata = dict(default_metadata_from_dbt_resource_props({"columns": column_schema_data}))

        if with_column_lineage:
            parents = {}
            for parent_unique_id in dbt_resource_props.get("depends_on", {}).get("nodes", []):
                parent_props = manifest["nodes"].get(parent_unique_id) or manifest["sources"].get(
                    parent_unique_id
                )
                parent_columns = _columns(parent_unique_id)
                if parent_props and parent_props.get("relation_name") and parent_columns:
                    parents[parent_props["relation_name"]] = parent_columns
            try:
                metadata.update(
                    _build_column_lineage_metadata(
                        event_history_metadata=EventHistoryMetadata(
                            columns=column_schema_data, parents=parents
                        ),
                        dbt_resource_props=dbt_resource_props,
                        manifest=manifest,
                        dagster_dbt_translator=invocation.dagster_dbt_translator,
                        target_path=invocation.target_path,
                        project=invocation.project,
                    )
                )
            except Exception as e:
                logger.warning(
                    "An error occurred while building column lineage metadata for the dbt resource"
                    f" `{dbt_resource_props['original_file_path']}`."
                    " Lineage metadata will not be included in the event.\n\n"
                    f"Exception: {e}",
                    exc_info=True,
                )

        metadata_by_unique_id[unique_id] = metadata

    return metadata_by_unique_id


class DbtEventIterator(Iterator[T]):
    """A wrapper around an iterator of dbt events which contains additional methods for
    post-processing the events, such as fetching row counts for materialized tables.
    """

    def __init__(
        self,
        events: Iterator[T],
        dbt_cli_invocation: "DbtCliInvocation",
    ) -> None:
        self._inner_iterator = events
        self._dbt_cli_invocation = dbt_cli_invocation

    def __next__(self) -> T:
        return next(self._inner_iterator)

    def __iter__(self) -> "DbtEventIterator[T]":
        return self

    @public
    def fetch_row_counts(
        self,
    ) -> "DbtEventIterator[DbtDagsterEventType]":
        """Functionality which will fetch row counts for materialized dbt
        models in a dbt run once they are built. Note that row counts will not be fetched
        for views, since this requires running the view's SQL query which may be costly.

        Returns:
            Iterator[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult, AssetCheckEvaluation]]:
                A set of corresponding Dagster events for dbt models, with row counts attached,
                yielded in the order they are emitted by dbt.
        """
        if self._dbt_cli_invocation.is_fusion:
            return self._attach_fusion_metadata(_fetch_fusion_row_counts, "row count")

        return self._attach_metadata(_fetch_row_count_metadata)

    @public
    def fetch_column_metadata(
        self,
        with_column_lineage: bool = True,
    ) -> "DbtEventIterator[DbtDagsterEventType]":
        """Functionality which will fetch column schema metadata for dbt models in a run
        once they're built. It will also fetch schema information for upstream models and generate
        column lineage metadata using sqlglot, if enabled.

        Args:
            with_column_lineage (bool): Whether to generate column lineage metadata using sqlglot.

        Returns:
            Iterator[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult, AssetCheckEvaluation]]:
                A set of corresponding Dagster events for dbt models, with column metadata attached,
                yielded in the order they are emitted by dbt.
        """
        if self._dbt_cli_invocation.is_fusion:
            return self._attach_fusion_metadata(
                lambda invocation, unique_ids: _fetch_fusion_column_metadata(
                    invocation, unique_ids, with_column_lineage
                ),
                "column",
            )

        fetch_metadata = lambda invocation, event: _fetch_column_metadata(
            invocation, event, with_column_lineage
        )
        return self._attach_metadata(fetch_metadata)

    def _attach_fusion_metadata(
        self,
        fetch: Callable[["DbtCliInvocation", Sequence[str]], dict[str, dict[str, Any]]],
        description: str,
    ) -> "DbtEventIterator[DbtDagsterEventType]":
        """Attach metadata fetched for all materializations at once, after dbt Fusion exits.

        dbt Fusion has no Python adapter to query per event, and each metadata command starts a
        dbt process, so one batch per run costs less than one process per event.
        """

        def _attach() -> Iterator[DbtDagsterEventType]:
            events: list[DbtDagsterEventType] = []
            caught_exception: Exception | None = None
            try:
                events.extend(self)
            except Exception as e:
                caught_exception = e

            unique_ids = list(
                dict.fromkeys(
                    cast("TextMetadataValue", event.metadata["unique_id"]).text
                    for event in events
                    if isinstance(event, (AssetMaterialization, Output))
                    and "unique_id" in event.metadata
                )
            )
            metadata_by_unique_id: dict[str, dict[str, Any]] = {}
            if unique_ids:
                try:
                    metadata_by_unique_id = fetch(self._dbt_cli_invocation, unique_ids)
                except Exception as e:
                    logger.warning(
                        f"An error occurred while fetching {description} metadata with dbt Fusion."
                        f" {description.capitalize()} metadata will not be included in the"
                        f" events.\n\nException: {e}",
                        exc_info=True,
                    )

            for event in events:
                unique_id_value = event.metadata.get("unique_id")
                metadata = (
                    metadata_by_unique_id.get(cast("TextMetadataValue", unique_id_value).text)
                    if isinstance(event, (AssetMaterialization, Output)) and unique_id_value
                    else None
                )
                yield event.with_metadata({**event.metadata, **metadata}) if metadata else event

            if caught_exception:
                raise caught_exception

        return DbtEventIterator(_attach(), dbt_cli_invocation=self._dbt_cli_invocation)

    def _attach_metadata(
        self,
        fn: Callable[["DbtCliInvocation", DbtDagsterEventType], dict[str, Any] | None],
    ) -> "DbtEventIterator[DbtDagsterEventType]":
        """Runs a threaded task to attach metadata to each event in the iterator.

        Args:
            fn (Callable[[DbtCliInvocation, DbtDagsterEventType], Optional[Dict[str, Any]]]):
                A function which takes a DbtCliInvocation and a DbtDagsterEventType and returns
                a dictionary of metadata to attach to the event.

        Returns:
             Iterator[Union[Output, AssetMaterialization, AssetObservation, AssetCheckResult]]:
                A set of corresponding Dagster events for dbt models, with any metadata output
                by the function attached, yielded in the order they are emitted by dbt.
        """

        def _map_fn(event: DbtDagsterEventType) -> DbtDagsterEventType:
            result = fn(self._dbt_cli_invocation, event)
            if result is None:
                return event

            return event.with_metadata({**event.metadata, **result})

        # If the adapter is DuckDB, we need to wait for the dbt CLI process to complete
        # so that the DuckDB lock is released. This is because DuckDB does not allow for
        # opening multiple connections to the same database when a write connection, such
        # as the one dbt uses, is open.
        event_stream = self
        if (
            self._dbt_cli_invocation.adapter
            and self._dbt_cli_invocation.adapter.__class__.__name__ == "DuckDBAdapter"
        ):
            from dbt.adapters.duckdb import DuckDBAdapter

            if isinstance(self._dbt_cli_invocation.adapter, DuckDBAdapter):
                event_stream = exhaust_iterator_and_yield_results_with_exception(self)

        def _threadpool_wrap_map_fn() -> Iterator[
            Output
            | AssetMaterialization
            | AssetObservation
            | AssetCheckResult
            | AssetCheckEvaluation
        ]:
            with ThreadPoolExecutor(
                max_workers=self._dbt_cli_invocation.postprocessing_threadpool_num_threads,
                thread_name_prefix=f"dbt_attach_metadata_{getattr(fn, '__name__', 'fn')}",
            ) as executor:
                yield from imap(
                    executor=executor,
                    iterable=event_stream,
                    func=_map_fn,
                )

        return DbtEventIterator(
            _threadpool_wrap_map_fn(),
            dbt_cli_invocation=self._dbt_cli_invocation,
        )

    @public
    def with_insights(
        self,
        skip_config_check: bool = False,
        record_observation_usage: bool = True,
    ) -> "DbtEventIterator[DbtDagsterEventType]":
        """Associate each warehouse query with the produced asset materializations for use in Dagster
        Plus Insights. Currently supports Snowflake and BigQuery. For any other adapter (e.g.
        DuckDB), this is a no-op: a warning is logged and the dbt events pass through unchanged,
        so the same pipeline can run locally against an unsupported warehouse without failing.

        For more information, see the documentation for
        `dagster_cloud.dagster_insights.dbt_with_snowflake_insights` and
        `dagster_cloud.dagster_insights.dbt_with_bigquery_insights`.

        Args:
            skip_config_check (bool): If true, skips the check that the dbt project config is set up
                correctly. Defaults to False.
            record_observation_usage (bool): If True, associates the usage associated with
                asset observations with that asset. Default is True.

        **Example:**

        .. code-block:: python

            @dbt_assets(manifest=DBT_MANIFEST_PATH)
            def jaffle_shop_dbt_assets(
                context: AssetExecutionContext,
                dbt: DbtCliResource,
            ):
                yield from dbt.cli(["build"], context=context).stream().with_insights()
        """
        adapter_type = self._dbt_cli_invocation.manifest.get("metadata", {}).get("adapter_type")
        if adapter_type == "snowflake":
            try:
                from dagster_cloud.dagster_insights import (  # ty: ignore[unresolved-import]
                    dbt_with_snowflake_insights,
                )
            except ImportError as e:
                raise DagsterInvalidPropertyError(
                    "The `dagster_cloud` library is required to use the `with_insights`"
                    " method. Install the library with `pip install dagster-cloud`."
                ) from e

            return DbtEventIterator(
                events=dbt_with_snowflake_insights(
                    context=self._dbt_cli_invocation.context,
                    dbt_cli_invocation=self._dbt_cli_invocation,
                    dagster_events=self,
                    skip_config_check=skip_config_check,
                    record_observation_usage=record_observation_usage,
                ),
                dbt_cli_invocation=self._dbt_cli_invocation,
            )
        elif adapter_type == "bigquery":
            try:
                from dagster_cloud.dagster_insights import (  # ty: ignore[unresolved-import]
                    dbt_with_bigquery_insights,
                )
            except ImportError as e:
                raise DagsterInvalidPropertyError(
                    "The `dagster_cloud` library is required to use the `with_insights`"
                    " method. Install the library with `pip install dagster-cloud`."
                ) from e

            return DbtEventIterator(
                events=dbt_with_bigquery_insights(
                    context=self._dbt_cli_invocation.context,
                    dbt_cli_invocation=self._dbt_cli_invocation,
                    dagster_events=self,
                    skip_config_check=skip_config_check,
                    record_observation_usage=record_observation_usage,
                ),
                dbt_cli_invocation=self._dbt_cli_invocation,
            )
        else:
            logger.warning(
                "Dagster+ Insights is only supported for the Snowflake and BigQuery dbt"
                f" adapters, but the dbt project uses the `{adapter_type}` adapter. Skipping"
                " insights; dbt events will pass through unchanged."
            )
            return DbtEventIterator(
                events=self,
                dbt_cli_invocation=self._dbt_cli_invocation,
            )
