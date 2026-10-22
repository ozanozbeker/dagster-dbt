import pytest
from dagster._core.errors import DagsterInvalidInvocationError
from dagster_dbt.components.dbt_project.scaffolder import DbtProjectComponentScaffolder


class _FailedResult:
    success = False
    exception = RuntimeError("not a terminal")


class _FailingRunner:
    def invoke(self, args: list[str]) -> _FailedResult:
        return _FailedResult()


def test_failed_dbt_init_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """A failed `dbt init` raises instead of scaffolding a component around no project."""
    monkeypatch.setattr("dbt.cli.main.dbtRunner", _FailingRunner)

    with pytest.raises(DagsterInvalidInvocationError, match="not a terminal"):
        DbtProjectComponentScaffolder()._init_dbt_project()
