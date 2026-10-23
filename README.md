# dagster-dbt

This is a short-lived fork of `dagster-dbt` 0.29.26 (dagster 1.13.26) that makes it work fully with dbt 2.0. Once upstream ships these fixes, switch back to the PyPI release.

Install it from the tag, alongside dbt 2.0 and no dbt-core:

```bash
uv add "dagster-dbt @ git+https://github.com/ozanozbeker/dagster-dbt@v0.29.26-fusion.3" dbt
```

The fork fixes these failures on top of upstream:

- A passing dbt unit test no longer fails the step with a `KeyError`. dbt 2.0 reports unit tests and functions as finished nodes, but the manifest keeps them outside `nodes`.
- `python-dateutil` is a declared dependency. Upstream only gets it through dbt-common, so `import dagster_dbt` fails without dbt-core.
- Seeds keep `dagster-dbt/materialization_type`. dbt 2.0 leaves `config.materialized` empty for seeds.
- The dbt project component scaffolder raises when `dbt init` fails, instead of carrying on.
- `@dbt_assets(selector=...)` sends `--selector` alone. Upstream adds the default `--select fqn:*`, and dbt 2.0 exits with code 2 on the pair.
- `pyproject.toml` drops the monorepo's `[tool.uv.sources]`, which broke installs from git.

It also fills these gaps that upstream leaves under dbt 2.0:

- `fetch_row_counts()` and `fetch_column_metadata()` work. dbt 2.0 has no Python adapter, so after dbt exits the fork runs one `dbt show` query for every row count and one `dbt compile --write-catalog` for every column schema. Events therefore arrive at the end of the run, and column types use the warehouse's own names, such as `VARCHAR` where dbt-core's DuckDB adapter reports `character varying(256)`.
- `@dbt_assets` accepts `select`, `exclude` and `selector` without `project=`. `dagster_dbt/manifest_selection.py` evaluates dbt's selection syntax against the manifest. It matched `dbt ls` on all 5,095 selections tried across 20 dbt-core projects, and all 665 tried across three dbt 2.0 projects. The `state`, `result` and `source_status` methods need dbt's run artifacts, so they still need `project=`.
- `DbtCliResource` runs the `dbt` installed next to dagster-dbt before any other `dbt` on `PATH`, so an unactivated virtualenv no longer runs Homebrew's dbt.
- Installing dbt 2.0 next to dbt-core, or any `dbt-<adapter>` package that pulls it in, now warns at import. The two packages overwrite each other's files in the shared `dbt` package, so keep them in separate environments.

The docs for `dagster-dbt` can be found
[here](https://docs.dagster.io/integrations/libraries/dbt/dagster-dbt).
