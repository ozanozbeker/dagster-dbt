# dagster-dbt

This is a short-lived fork of `dagster-dbt` 0.29.26 (dagster 1.13.26) that fixes the failures it hits with dbt 2.0. Once upstream ships these fixes, switch back to the PyPI release.

Install it from the tag, alongside dbt 2.0 and no dbt-core:

```bash
uv add "dagster-dbt @ git+https://github.com/ozanozbeker/dagster-dbt@v0.29.26-fusion.1" dbt
```

The fork changes these things on top of upstream:

- A passing dbt unit test no longer fails the step with a `KeyError`. dbt 2.0 reports unit tests and functions as finished nodes, but the manifest keeps them outside `nodes`.
- `python-dateutil` is a declared dependency. Upstream only gets it through dbt-common, so `import dagster_dbt` fails without dbt-core.
- Seeds keep `dagster-dbt/materialization_type`. dbt 2.0 leaves `config.materialized` empty for seeds.
- The dbt project component scaffolder raises when `dbt init` fails, instead of carrying on.
- `pyproject.toml` drops the monorepo's `[tool.uv.sources]`, which broke installs from git.

These limits remain under dbt 2.0, because dbt 2.0 has no Python adapter:

- `fetch_row_counts()` and `fetch_column_metadata()` log a warning and add nothing. The `dagster` dbt package's `log_column_level_metadata` post-hook still adds column schema and lineage for models.
- `@dbt_assets` needs `project=` when it uses `select`, `exclude` or `selector`, because selection runs `dbt ls`.
- `DbtCliResource` runs the first `dbt` on `PATH`. Set `dbt_executable` if more than one is installed.
- dbt-core, or any `dbt-<adapter>` package that pulls it in, overwrites dbt 2.0's files in the shared `dbt` package. Keep them in separate environments.

The docs for `dagster-dbt` can be found
[here](https://docs.dagster.io/integrations/libraries/dbt/dagster-dbt).
