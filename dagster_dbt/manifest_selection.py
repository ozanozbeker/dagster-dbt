"""Evaluate dbt node selection against a manifest, without dbt-core.

dbt 2.0 ships no Python selection engine, so `utils.select_unique_ids` uses this module when
dbt-core is absent and there is no project to run `dbt ls` in. It follows the semantics of
dbt-core's `dbt.graph` package; the `state`, `result` and `source_status` methods read dbt run
artifacts that a manifest does not hold, so they raise.
"""

import re
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass
from fnmatch import fnmatch, fnmatchcase
from functools import cached_property
from pathlib import PurePosixPath
from typing import Any, Literal, TypeAlias, cast, get_args

IndirectSelection: TypeAlias = Literal["eager", "cautious", "buildable", "empty"]

_SPEC_PATTERN = re.compile(
    r"\A"
    r"(?P<childrens_parents>(\@))?"
    r"(?P<parents>((?P<parents_depth>(\d*))\+))?"
    r"((?P<method>([\w.]+)):)?(?P<value>(.*?))"
    r"(?P<children>(\+(?P<children_depth>(\d*))))?"
    r"\Z"
)

# Searched in dbt-core's order, which decides the resource a unique id resolves to.
_GRAPH_MEMBER_COLLECTIONS = (
    "nodes",
    "sources",
    "exposures",
    "functions",
    "metrics",
    "semantic_models",
    "unit_tests",
    "saved_queries",
)
_NON_SOURCE_COLLECTIONS = (
    "nodes",
    "exposures",
    "metrics",
    "unit_tests",
    "semantic_models",
    "saved_queries",
    "functions",
)
_NODE_TYPES = frozenset(
    {
        "model",
        "analysis",
        "test",
        "snapshot",
        "operation",
        "seed",
        "rpc",
        "sql_operation",
        "doc",
        "source",
        "macro",
        "exposure",
        "metric",
        "group",
        "saved_query",
        "semantic_model",
        "unit_test",
        "fixture",
        "function",
    }
)
_METHODS = frozenset(
    {
        "fqn",
        "tag",
        "group",
        "access",
        "source",
        "path",
        "file",
        "package",
        "config",
        "test_name",
        "test_type",
        "resource_type",
        "state",
        "exposure",
        "metric",
        "result",
        "source_status",
        "version",
        "semantic_model",
        "saved_query",
        "unit_test",
        "selector",
    }
)
_ARTIFACT_METHODS = frozenset({"state", "result", "source_status"})


class DbtSelectionError(ValueError):
    """Raised for a dbt selection that is invalid or needs more than a manifest to evaluate."""


@dataclass(frozen=True)
class _Criteria:
    raw: Any
    method: str
    arguments: tuple[str, ...]
    value: Any
    childrens_parents: bool
    parents: bool
    parents_depth: int | None
    children: bool
    children_depth: int | None
    indirect_selection: IndirectSelection = "eager"


@dataclass(frozen=True)
class _Group:
    operator: Literal["union", "intersection", "difference"]
    components: tuple["_Spec", ...]


_Spec: TypeAlias = _Criteria | _Group


def _to_depth(raw: Any) -> int | None:
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as e:
        raise DbtSelectionError(f"Invalid node spec - could not handle depth {raw}") from e


def _default_method(value: str) -> str:
    if "/" in value or "\\" in value:
        return "path"
    if value.lower().endswith((".sql", ".py", ".csv")):
        return "file"
    return "fqn"


def _criteria_from_dict(raw: Any, definition: Mapping[str, Any]) -> _Criteria:
    if "value" not in definition:
        raise DbtSelectionError(f'Invalid node spec "{raw}" - no search value!')

    raw_method = definition.get("method")
    if raw_method is None:
        method, arguments = _default_method(str(definition["value"])), ()
    else:
        method, *argument_list = str(raw_method).split(".")
        arguments = tuple(argument_list)
        if method not in _METHODS:
            raise DbtSelectionError(f"'{method}' is not a valid method name")

    indirect_selection = definition.get("indirect_selection", "eager")
    if indirect_selection not in get_args(IndirectSelection):
        raise DbtSelectionError(f"'{indirect_selection}' is not a valid indirect selection")

    criteria = _Criteria(
        raw=raw,
        method=method,
        arguments=arguments,
        value=definition["value"],
        childrens_parents=bool(definition.get("childrens_parents")),
        parents=bool(definition.get("parents")),
        parents_depth=_to_depth(definition.get("parents_depth")),
        children=bool(definition.get("children")),
        children_depth=_to_depth(definition.get("children_depth")),
        indirect_selection=cast("IndirectSelection", indirect_selection),
    )
    if criteria.children and criteria.childrens_parents:
        raise DbtSelectionError(
            f'Invalid node spec {raw} - "@" prefix and "+" suffix are incompatible'
        )
    return criteria


def _parse_single(raw: str) -> _Criteria:
    match = _SPEC_PATTERN.match(raw)
    if match is None:
        raise DbtSelectionError(f'Invalid selector spec "{raw}"')
    return _criteria_from_dict(raw, match.groupdict())


def _parse_union(raw: str) -> _Spec:
    """Parse a `--select` or `--exclude` string: spaces separate a union, commas an intersection."""
    return _Group(
        "union",
        tuple(
            _Group("intersection", tuple(_parse_single(part) for part in spec.split(",")))
            for spec in raw.split(" ")
        ),
    )


def _parse_definition_list(definition: Mapping[str, Any], key: str) -> list[Any]:
    values = definition[key]
    if not isinstance(values, list):
        raise DbtSelectionError(f'Invalid value for key "{key}". Expected a list.')
    return values


def _parse_exclusions(definition: Mapping[str, Any]) -> _Spec | None:
    exclusions = [
        _parse_definition(exclusion) for exclusion in _parse_definition_list(definition, "exclude")
    ]
    if not exclusions:
        return None
    return exclusions[0] if len(exclusions) == 1 else _Group("union", tuple(exclusions))


def _parse_set_definition(
    definition: Mapping[str, Any], operator: Literal["union", "intersection"]
) -> _Spec:
    included: list[_Spec] = []
    excluded: _Spec | None = None
    for part in _parse_definition_list(definition, operator):
        if isinstance(part, dict) and "exclude" in part:
            if excluded is not None:
                raise DbtSelectionError(
                    "You cannot provide multiple exclude arguments to the same selector set"
                    " operator"
                )
            excluded = _parse_exclusions(part)
        else:
            included.append(_parse_definition(part))

    group = _Group(operator, tuple(included))
    return group if excluded is None else _Group("difference", (group, excluded))


def _parse_definition(definition: Any) -> _Spec:
    """Parse one selector definition from `selectors.yml`, as dbt-core's `parse_from_definition`."""
    if isinstance(definition, str):
        return _parse_single(definition)
    if not isinstance(definition, dict):
        raise DbtSelectionError(
            f"Expected to find union, intersection, str or dict, instead found"
            f" {type(definition)}: {definition}"
        )
    if "union" in definition:
        return _parse_set_definition(definition, "union")
    if "intersection" in definition:
        return _parse_set_definition(definition, "intersection")

    excluded: _Spec | None = None
    if len(definition) == 1:
        key, value = next(iter(definition.items()))
        criteria_definition: Mapping[str, Any] = {"method": key, "value": value}
    elif "method" in definition and "value" in definition:
        criteria_definition = {k: v for k, v in definition.items() if k != "exclude"}
        if "exclude" in definition:
            excluded = _parse_exclusions(definition)
    else:
        raise DbtSelectionError(
            f'Expected either 1 key or else "method" and "value" keys, but got {list(definition)}'
        )

    criteria = _criteria_from_dict(definition, criteria_definition)
    return criteria if excluded is None else _Group("difference", (criteria, excluded))


def _is_selected_node(fqn: list[str], node_selector: str, is_versioned: bool) -> bool:
    """Match an fqn against a selector, as dbt-core's `is_selected_node`."""
    if is_versioned:
        flat_node_selector = node_selector.split(".")
        if fqn[-2] == node_selector:
            return True
        if "_".join(fqn[-2:]) == "_".join(flat_node_selector[-2:]):
            return True
    elif fqn[-1] == node_selector:
        return True

    flat_fqn = [item for segment in fqn for item in segment.split(".")]
    selector_parts = node_selector.split(".")
    if len(flat_fqn) < len(selector_parts):
        return False

    slurp_from_index: int | None = None
    for index, selector_part in enumerate(selector_parts):
        if any(wildcard in selector_part for wildcard in ("*", "?", "[", "]")):
            slurp_from_index = index
            break
        if flat_fqn[index] != selector_part:
            return False

    if slurp_from_index is None:
        return True
    if fnmatch(".".join(flat_fqn[slurp_from_index:]), ".".join(selector_parts[slurp_from_index:])):
        return True
    if "." not in node_selector:
        return fnmatch(fqn[-2] if is_versioned else fqn[-1], node_selector)
    return False


def _glob_parts_match(path_parts: tuple[str, ...], pattern_parts: tuple[str, ...]) -> bool:
    if not pattern_parts:
        return not path_parts
    head, *rest = pattern_parts
    if head == "**":
        return any(
            _glob_parts_match(path_parts[index:], tuple(rest))
            for index in range(len(path_parts) + 1)
        )
    return bool(path_parts) and (
        fnmatchcase(path_parts[0], head) and _glob_parts_match(path_parts[1:], tuple(rest))
    )


def _glob_match(path: PurePosixPath, pattern: str) -> bool:
    """Match a project-relative path as `Path.glob(pattern)` run in the project root would."""
    pattern_parts = tuple(part for part in pattern.replace("\\", "/").split("/") if part != ".")
    pattern_parts = tuple(part for part in pattern_parts if part) or (".",)
    if pattern_parts == (".",):
        return path == PurePosixPath(".")
    return _glob_parts_match(path.parts, pattern_parts)


def _globset_regex(pattern: str) -> re.Pattern[str]:
    """Translate a glob as Rust's globset reads it: `*` crosses `/`, and `**/` may match nothing."""
    parts = [part for part in pattern.replace("\\", "/").split("/") if part not in ("", ".")]
    glob = "/".join(parts)
    regex = ""
    index = 0
    while index < len(glob):
        if glob.startswith("**/", index):
            regex += "(?:.*/)?"
            index += 3
        elif glob.startswith("**", index):
            regex += ".*"
            index += 2
        elif glob[index] == "*":
            regex += ".*"
            index += 1
        elif glob[index] == "?":
            regex += "."
            index += 1
        elif glob[index] == "[" and "]" in glob[index + 1 :]:
            end = glob.index("]", index + 1)
            body = glob[index + 1 : end]
            regex += "[" + ("^" + body[1:] if body.startswith("!") else body) + "]"
            index = end + 1
        else:
            regex += re.escape(glob[index])
            index += 1
    return re.compile(regex)


def _path_candidates(resource: Mapping[str, Any]) -> list[PurePosixPath]:
    """Return the paths dbt's `path` method compares: the file, its directories and its patch."""
    path = PurePosixPath(resource["original_file_path"].replace("\\", "/"))
    candidates = [path, *path.parents]
    patch_path = resource.get("patch_path")
    if patch_path and "://" in patch_path:
        candidates.append(PurePosixPath(patch_path.split("://")[1].replace("\\", "/")))
    return candidates


def _parse_version(raw: Any) -> tuple[int, float | str]:
    """Order versions as dbt's `UnparsedVersion` does: numbers numerically, before strings."""
    try:
        return (0, float(raw))
    except (TypeError, ValueError):
        return (1, str(raw))


def _matches_config_value(selector: Any, value: Any, case_insensitive: bool) -> bool:
    def _equals(candidate: Any) -> bool:
        if case_insensitive and isinstance(selector, str) and isinstance(candidate, str):
            return selector.upper() == candidate.upper()
        return selector == candidate

    is_true = isinstance(selector, str) and selector.upper() == "TRUE"
    is_false = isinstance(selector, str) and selector.upper() == "FALSE"
    if isinstance(value, list):
        return (
            any(_equals(item) for item in value)
            or (is_true and True in value)
            or (is_false and False in value)
        )
    return _equals(value) or (is_true and value is True) or (is_false and value is False)


class _ManifestSelector:
    """Evaluates parsed selection specs against one manifest."""

    def __init__(self, manifest: Mapping[str, Any]) -> None:
        self._manifest = manifest
        self._resources: dict[str, Mapping[str, Any]] = {}
        for collection in reversed(_GRAPH_MEMBER_COLLECTIONS):
            self._resources.update(manifest.get(collection) or {})

        members = {
            unique_id
            for unique_id, resource in self._resources.items()
            if (resource.get("config") or {}).get("enabled", True) is not False
        }
        self._children: dict[str, set[str]] = {unique_id: set() for unique_id in members}
        self._parents: dict[str, set[str]] = {unique_id: set() for unique_id in members}

        # dbt Fusion omitted `child_map` in early releases, so `parent_map` fills the gaps.
        edges = {
            (parent, child)
            for parent, children in (manifest.get("child_map") or {}).items()
            for child in children
        } | {
            (parent, child)
            for child, parents in (manifest.get("parent_map") or {}).items()
            for parent in parents
        }
        for parent, child in edges:
            if parent in members and child in members:
                self._children[parent].add(child)
                self._parents[child].add(parent)

        self._selectors: Mapping[str, Any] = manifest.get("selectors") or {}
        # Where dbt 2.0 and dbt-core disagree, follow the engine that wrote the manifest.
        dbt_version = str((manifest.get("metadata") or {}).get("dbt_version", ""))
        self._is_dbt_2 = dbt_version.split(".")[0].isdigit() and int(dbt_version.split(".")[0]) >= 2

    def select(self, spec: _Spec) -> set[str]:
        direct, _ = self._select_recursively(spec)
        return direct

    def _resources_in(
        self, collections: Iterable[str], included: set[str]
    ) -> Iterator[tuple[str, Mapping[str, Any]]]:
        for collection in collections:
            for unique_id, resource in (self._manifest.get(collection) or {}).items():
                if unique_id in included:
                    yield unique_id, resource

    def _walk(self, start: set[str], edges: dict[str, set[str]], depth: int | None) -> set[str]:
        found: set[str] = set()
        layer = start
        steps = 0
        while layer and (depth is None or steps < depth):
            next_layer = {
                neighbor
                for unique_id in layer
                for neighbor in edges.get(unique_id, ())
                if neighbor not in found
            }
            found |= next_layer
            layer = next_layer
            steps += 1
        return found

    def _select_parents(self, selected: set[str], depth: int | None = None) -> set[str]:
        return self._walk(selected, self._parents, depth)

    def _select_children(self, selected: set[str], depth: int | None = None) -> set[str]:
        return self._walk(selected, self._children, depth)

    def _select_recursively(self, spec: _Spec) -> tuple[set[str], set[str]]:
        if isinstance(spec, _Criteria):
            return self._select_criteria(spec)

        bundles = [self._select_recursively(component) for component in spec.components]
        combine: Callable[..., set[str]] = {
            "union": set.union,
            "intersection": set.intersection,
            "difference": set.difference,
        }[spec.operator]
        if not bundles:
            return set(), set()
        direct = combine(*(direct for direct, _ in bundles))
        indirect = combine(*(direct | indirect for direct, indirect in bundles))
        return direct, indirect

    def _select_criteria(self, criteria: _Criteria) -> tuple[set[str], set[str]]:
        included = set(self._children)
        collected = set(self._search(criteria, included))

        selected = set(collected)
        if criteria.childrens_parents:
            descendants = self._select_children(collected) | collected
            selected |= self._select_parents(descendants) | descendants
        if criteria.parents:
            selected |= self._select_parents(collected, criteria.parents_depth)
        if criteria.children:
            selected |= self._select_children(collected, criteria.children_depth)

        if criteria.indirect_selection == "empty" or criteria.method == "selector":
            return selected, set()
        return self._expand_selection(selected, criteria.indirect_selection)

    def _expand_selection(
        self, selected: set[str], indirect_selection: IndirectSelection
    ) -> tuple[set[str], set[str]]:
        """Add the tests attached to the selection, as `NodeSelector.expand_selection` does."""
        direct = set(selected)
        indirect: set[str] = set()
        buildable = (
            selected | self._select_parents(selected) | set(self._manifest.get("sources") or {})
            if indirect_selection == "buildable"
            else set()
        )
        successors = {child for unique_id in selected for child in self._children[unique_id]}
        for unique_id in successors:
            resource = (self._manifest.get("nodes") or {}).get(unique_id) or (
                self._manifest.get("unit_tests") or {}
            ).get(unique_id)
            if resource is None or resource.get("resource_type") not in ("test", "unit_test"):
                continue
            depends_on = set((resource.get("depends_on") or {}).get("nodes") or [])
            if (
                indirect_selection == "eager"
                or depends_on <= selected
                or (indirect_selection == "buildable" and depends_on <= buildable)
            ):
                direct.add(unique_id)
            elif indirect_selection != "empty":
                indirect.add(unique_id)
        return direct, indirect

    def _search(self, criteria: _Criteria, included: set[str]) -> Iterator[str]:
        method, value = criteria.method, criteria.value
        if method in _ARTIFACT_METHODS:
            raise DbtSelectionError(
                f"The `{method}` selection method reads dbt run artifacts, which a manifest does"
                " not include. Evaluate this selection with a dbt project instead."
            )
        if method == "fqn":
            yield from self._search_fqn(str(value), included)
        elif method == "tag":
            for unique_id, resource in self._resources_in(_GRAPH_MEMBER_COLLECTIONS, included):
                if any(fnmatch(tag, value) for tag in resource.get("tags") or []):
                    yield unique_id
        elif method == "group":
            for unique_id, resource in self._resources_in(("nodes", "metrics"), included):
                group = (resource.get("config") or {}).get("group")
                if group and fnmatch(group, value):
                    yield unique_id
        elif method == "access":
            for unique_id, resource in self._resources_in(("nodes",), included):
                if resource.get("resource_type") == "model" and resource.get("access") == value:
                    yield unique_id
        elif method == "source":
            yield from self._search_source(str(value), included)
        elif method in ("exposure", "metric", "semantic_model", "saved_query", "unit_test"):
            yield from self._search_package_and_name(method, str(value), included)
        elif method == "path":
            yield from self._search_path(str(value), included)
        elif method == "file":
            for unique_id, resource in self._resources_in(_GRAPH_MEMBER_COLLECTIONS, included):
                paths = [resource["original_file_path"]]
                # dbt 2.0 also matches a test's generated `path`, such as `not_null_x.sql`.
                if (
                    self._is_dbt_2
                    and resource.get("resource_type") == "test"
                    and resource.get("path")
                ):
                    paths.append(resource["path"])
                if any(
                    fnmatch(path.name, value) or fnmatch(path.stem, value)
                    for path in (PurePosixPath(p.replace("\\", "/")) for p in paths)
                ):
                    yield unique_id
        elif method == "package":
            package = value
            if value == "this" and (self._manifest.get("metadata") or {}).get("project_name"):
                package = self._manifest["metadata"]["project_name"]
            for unique_id, resource in self._resources_in(_GRAPH_MEMBER_COLLECTIONS, included):
                if fnmatch(resource["package_name"], package):
                    yield unique_id
        elif method == "config":
            yield from self._search_config(criteria.arguments, value, included)
        elif method == "resource_type":
            if value not in _NODE_TYPES:
                raise DbtSelectionError(f'Invalid resource_type selector "{value}"')
            for unique_id, resource in self._resources_in(_GRAPH_MEMBER_COLLECTIONS, included):
                if resource.get("resource_type") == value:
                    yield unique_id
        elif method == "test_name":
            for unique_id, resource in self._resources_in(("nodes", "unit_tests"), included):
                if resource.get("resource_type") == "test" and resource.get("test_metadata"):
                    if fnmatch(resource["test_metadata"]["name"], value):
                        yield unique_id
                elif resource.get("resource_type") == "unit_test" and fnmatch(
                    resource["name"], value
                ):
                    yield unique_id
        elif method == "test_type":
            yield from self._search_test_type(str(value), included)
        elif method == "version":
            yield from self._search_version(str(value), included)
        elif method == "selector":
            yield from self._search_selector(str(value), included)

    def _search_fqn(self, value: str, included: set[str]) -> Iterator[str]:
        for unique_id, resource in self._resources_in(_NON_SOURCE_COLLECTIONS, included):
            fqn = resource.get("fqn") or []
            is_versioned = resource.get("version") is not None
            if fqn and (
                _is_selected_node(fqn, value, is_versioned)
                or (len(fqn) > 1 and _is_selected_node(fqn[1:], value, is_versioned))
            ):
                yield unique_id

    def _search_source(self, value: str, included: set[str]) -> Iterator[str]:
        parts = value.split(".")
        if len(parts) == 1:
            package, source, table = "*", parts[0], "*"
        elif len(parts) == 2:
            package, source, table = "*", parts[0], parts[1]
        elif len(parts) == 3:
            package, source, table = parts
        else:
            raise DbtSelectionError(f'Invalid source selector value "{value}"')
        for unique_id, resource in self._resources_in(("sources",), included):
            if (
                fnmatch(resource["package_name"], package)
                and fnmatch(resource["source_name"], source)
                and fnmatch(resource["name"], table)
            ):
                yield unique_id

    def _search_package_and_name(
        self, method: str, value: str, included: set[str]
    ) -> Iterator[str]:
        parts = value.split(".")
        if len(parts) > 2:
            raise DbtSelectionError(f'Invalid {method} selector value "{value}"')
        package, name = parts if len(parts) == 2 else ("*", parts[0])
        collection = {
            "exposure": "exposures",
            "metric": "metrics",
            "semantic_model": "semantic_models",
            "saved_query": "saved_queries",
            "unit_test": "unit_tests",
        }[method]
        for unique_id, resource in self._resources_in((collection,), included):
            if fnmatch(resource["package_name"], package) and fnmatch(resource["name"], name):
                yield unique_id

    def _search_path(self, value: str, included: set[str]) -> Iterator[str]:
        # dbt globs files in the root project, then matches each resource's package-relative
        # paths against them, so a package resource matches only where the root project has a
        # file at the same relative path.
        if self._is_dbt_2:
            yield from self._search_path_dbt_2(value, included)
            return

        root_paths = self._root_project_paths
        for unique_id, resource in self._resources_in(_GRAPH_MEMBER_COLLECTIONS, included):
            if any(
                (root_paths is None or candidate in root_paths) and _glob_match(candidate, value)
                for candidate in _path_candidates(resource)
            ):
                yield unique_id

    def _search_path_dbt_2(self, value: str, included: set[str]) -> Iterator[str]:
        """Match paths as dbt 2.0 does: relative to the root project, with globset patterns.

        dbt 2.0 places a package's files under `dbt_packages/<package>` and a generic test at
        its generated file under `target/generic_tests`, the default install and target paths.
        """
        project_name = (self._manifest.get("metadata") or {}).get("project_name")
        pattern = _globset_regex(value)
        for unique_id, resource in self._resources_in(_GRAPH_MEMBER_COLLECTIONS, included):
            if resource.get("resource_type") == "test" and resource.get("test_metadata"):
                generated = PurePosixPath("target/generic_tests", resource.get("path") or "")
                candidates = [generated, *generated.parents]
            elif project_name and resource["package_name"] != project_name:
                prefix = PurePosixPath("dbt_packages", resource["package_name"])
                candidates = []
                for candidate in _path_candidates(resource):
                    candidates += [prefix / candidate, *(prefix / candidate).parents]
            else:
                candidates = _path_candidates(resource)
            if any(pattern.fullmatch(candidate.as_posix()) for candidate in candidates):
                yield unique_id

    @cached_property
    def _root_project_paths(self) -> set[PurePosixPath] | None:
        project_name = (self._manifest.get("metadata") or {}).get("project_name")
        if not project_name:
            return None
        return {
            candidate
            for collection in (*_GRAPH_MEMBER_COLLECTIONS, "macros", "docs")
            for resource in (self._manifest.get(collection) or {}).values()
            if resource.get("package_name") == project_name and resource.get("original_file_path")
            for candidate in _path_candidates(resource)
        }

    def _search_config(
        self, arguments: tuple[str, ...], value: Any, included: set[str]
    ) -> Iterator[str]:
        for unique_id, resource in self._resources_in(("nodes", "sources"), included):
            config_value: Any = resource.get("config") or {}
            for argument in arguments:
                if not isinstance(config_value, Mapping) or argument not in config_value:
                    break
                config_value = config_value[argument]
            else:
                if _matches_config_value(value, config_value, arguments == ("severity",)):
                    yield unique_id

    def _search_test_type(self, value: str, included: set[str]) -> Iterator[str]:
        if value not in ("generic", "schema", "data", "singular", "unit"):
            raise DbtSelectionError(
                f'Invalid test type selector {value}: expected "generic", "singular", "unit",'
                ' or "data"'
            )
        for unique_id, resource in self._resources_in(("nodes", "unit_tests"), included):
            resource_type = resource.get("resource_type")
            is_generic = resource_type == "test" and bool(resource.get("test_metadata"))
            is_singular = resource_type == "test" and not resource.get("test_metadata")
            if (
                (value == "generic" and is_generic)
                # dbt 2.0 still accepts the legacy `schema` alias but matches nothing with it.
                or (value == "schema" and is_generic and not self._is_dbt_2)
                or (value == "data" and (is_generic or is_singular))
                or (value == "singular" and is_singular)
                or (value == "unit" and resource_type == "unit_test")
            ):
                yield unique_id

    def _search_version(self, value: str, included: set[str]) -> Iterator[str]:
        if value not in ("latest", "prerelease", "old", "none"):
            raise DbtSelectionError(
                f'Invalid version type selector {value}: expected one of: "latest",'
                ' "prerelease", "old", or "none"'
            )
        for unique_id, resource in self._resources_in(("nodes",), included):
            if resource.get("resource_type") != "model":
                continue
            node_version = resource.get("version")
            latest_version = resource.get("latest_version")
            if value == "none":
                matched = node_version is None
            elif value == "latest":
                matched = node_version is not None and node_version == latest_version
            elif node_version is None or latest_version is None:
                matched = False
            elif value == "prerelease":
                matched = _parse_version(node_version) > _parse_version(latest_version)
            else:
                matched = _parse_version(node_version) < _parse_version(latest_version)
            if matched:
                yield unique_id

    def _search_selector(self, value: str, included: set[str]) -> Iterator[str]:
        matched = [
            selector["definition"]
            for name, selector in sorted(self._selectors.items())
            if fnmatch(name, value)
        ]
        if not matched:
            raise DbtSelectionError(
                f"Selector '{value}' did not match any selector in selectors.yml"
            )

        original_limit = sys.getrecursionlimit()
        sys.setrecursionlimit(min(original_limit, 250))
        try:
            for definition in matched:
                yield from self.select(_parse_definition(definition)) & included
        except RecursionError as e:
            raise DbtSelectionError(f"Circular dependency detected in selector: {value}") from e
        finally:
            sys.setrecursionlimit(original_limit)


def select_unique_ids_from_manifest_json(
    select: str, exclude: str, selector: str, manifest: Mapping[str, Any]
) -> set[str]:
    """Return the unique ids that a dbt `--select`, `--exclude` and `--selector` choose.

    Parameters
    ----------
    select
        A dbt `--select` string. An empty string selects every node.
    exclude
        A dbt `--exclude` string, or an empty string.
    selector
        The name of a selector in the manifest's `selectors`, or an empty string.
    manifest
        The parsed `manifest.json`.

    Returns
    -------
    :
        The unique ids of the selected resources, with tests chosen by eager indirect selection.

    Raises
    ------
    DbtSelectionError
        If the selection is invalid, combines a selector with `select` or `exclude`, names a
        missing selector, or uses a method that needs dbt run artifacts.
    """
    if selector and ((select and select != "fqn:*") or exclude):
        raise DbtSelectionError("Cannot provide both a selector and a select/exclude param.")

    if selector:
        selectors = manifest.get("selectors") or {}
        if selector not in selectors:
            raise DbtSelectionError(f"Selector `{selector}` not found in manifest.")
        spec = _parse_definition(selectors[selector]["definition"])
    else:
        spec = _parse_union(select or "fqn:*")

    if exclude:
        spec = _Group("difference", (spec, _parse_union(exclude)))

    return _ManifestSelector(manifest).select(spec)
