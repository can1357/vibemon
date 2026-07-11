"""AST source bundling for remote functions and classes.

Turns a module-level function or class into a standalone source payload:
the target definition plus every module-level import, literal constant,
function, and class it (transitively) references. Decorators bound to the
``vmon`` package are stripped so the shipped source has no SDK dependency;
all other decorators are kept and their dependencies are bundled too.
"""

from __future__ import annotations

import ast
import contextlib
import hashlib
import inspect
import symtable
import textwrap
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import Any

_DEFINITION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
_FUNCTION_NODES = (ast.FunctionDef, ast.AsyncFunctionDef)


@dataclass(frozen=True, slots=True)
class SourceBundle:
    """A self-contained source payload for one remote function or class."""

    source: str
    name: str
    module: str
    sha256: str


def bundle_source(
    target: Callable[..., Any] | type,
    include: Sequence[Callable[..., Any] | type] = (),
) -> SourceBundle:
    """Bundle ``target`` and its module-level dependencies into standalone source.

    ``include`` forces additional module-level functions or classes from the
    target's module into the bundle even when the target never names them
    (e.g. classes that only travel through pickled arguments).

    Raises ``ValueError`` when the target closes over local variables, its
    source file is unreachable, or an ``include`` entry is not a module-level
    definition in the target's module.
    """
    if getattr(target, "__closure__", None):
        raise ValueError("remote functions cannot close over local variables")
    try:
        target_source = textwrap.dedent(inspect.getsource(target))
    except OSError as exc:
        raise ValueError("remote functions must be defined in source files") from exc
    name = target.__name__
    module_name = getattr(target, "__module__", "") or "__main__"

    module_tree = _module_tree(target)
    if module_tree is None:
        if include:
            raise ValueError(
                f"include entry {include[0]!r} is not a module-level function or class"
            )
        return _sealed(_strip_all_decorators(target_source), name, module_name)

    vmon_names = _vmon_bound_names(module_tree)
    is_class = inspect.isclass(target)
    target_kind = (ast.ClassDef,) if is_class else _FUNCTION_NODES
    target_line: int | None = None
    if is_class:
        with contextlib.suppress(OSError, TypeError):
            target_line = inspect.getsourcelines(target)[1]
    else:
        target_line = getattr(getattr(target, "__code__", None), "co_firstlineno", None)

    imports: dict[str, tuple[int, str]] = {}
    constants: dict[str, tuple[int, str]] = {}
    definitions: dict[str, tuple[int, str, set[str]]] = {}
    target_node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef | None = None

    for node in module_tree.body:
        if isinstance(node, ast.Import | ast.ImportFrom):
            source = ast.unparse(node)
            for bound in _import_bound_names(node):
                imports[bound] = (node.lineno, source)
        elif isinstance(node, ast.Assign):
            try:
                ast.literal_eval(node.value)
            except TypeError, ValueError:
                continue
            source = ast.unparse(node)
            for bound in _assignment_bound_names(node.targets):
                constants[bound] = (node.lineno, source)
        elif isinstance(node, ast.AnnAssign):
            if node.value is None:
                continue
            try:
                ast.literal_eval(node.value)
            except TypeError, ValueError:
                continue
            source = ast.unparse(node)
            for bound in _assignment_bound_names([node.target]):
                constants[bound] = (node.lineno, source)
        elif isinstance(node, _DEFINITION_NODES):
            _strip_vmon_decorators(node, vmon_names)
            source = ast.unparse(node)
            definitions[node.name] = (node.lineno, source, _free_names(source))
            if (
                isinstance(node, target_kind)
                and node.name == name
                and (target_line is None or node.lineno == target_line)
            ):
                target_node = node

    if target_node is None:
        for node in module_tree.body:
            if isinstance(node, target_kind) and node.name == name:
                target_node = node
                break

    if target_node is not None:
        target_rendered = ast.unparse(target_node).rstrip() + "\n"
    else:
        # Nested (non-module-level) target: apply the same selective rule to
        # its own source, including decorators on methods of a class body.
        nested = ast.parse(target_source)
        for candidate in nested.body:
            if isinstance(candidate, _DEFINITION_NODES):
                _strip_vmon_decorators(candidate, vmon_names)
                target_rendered = ast.unparse(candidate).rstrip() + "\n"
                break
        else:
            raise ValueError("remote target source must contain a function or class definition")

    worklist = list(_free_names(target_rendered))
    for entry in include:
        entry_name = getattr(entry, "__name__", None)
        if not isinstance(entry_name, str) or entry_name not in definitions:
            raise ValueError(f"include entry {entry!r} is not a module-level function or class")
        if entry_name != name:
            worklist.append(entry_name)

    selected_imports: dict[int, str] = {}
    selected_constants: dict[int, str] = {}
    selected_definitions: dict[int, str] = {}
    seen: set[str] = set()
    while worklist:
        current = worklist.pop()
        if current in seen:
            continue
        seen.add(current)
        if current in imports:
            line, source = imports[current]
            selected_imports.setdefault(line, source)
        elif current in constants:
            line, source = constants[current]
            selected_constants.setdefault(line, source)
        elif current in definitions and current != name:
            line, source, referenced = definitions[current]
            if line not in selected_definitions:
                selected_definitions[line] = source
                worklist.extend(referenced)

    parts = [
        source
        for selected in (selected_imports, selected_constants, selected_definitions)
        for _line, source in sorted(selected.items())
    ]
    parts.append(target_rendered.rstrip())
    return _sealed("\n\n".join(parts) + "\n", name, module_name)


def _sealed(source: str, name: str, module: str) -> SourceBundle:
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()
    return SourceBundle(source=source, name=name, module=module, sha256=digest)


def _module_tree(target: Any) -> ast.Module | None:
    module = inspect.getmodule(target)
    if module is None:
        return None
    try:
        module_source = inspect.getsource(module)
    except OSError, TypeError:
        return None
    try:
        return ast.parse(module_source)
    except SyntaxError:
        return None


def _vmon_bound_names(tree: ast.Module) -> set[str]:
    """Names bound by ``import vmon...`` / ``from vmon... import ...`` statements."""
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".", 1)[0] == "vmon":
                    names.add(alias.asname or "vmon")
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and node.module and node.module.split(".", 1)[0] == "vmon":
                for alias in node.names:
                    if alias.name != "*":
                        names.add(alias.asname or alias.name)
    return names


def _root_name(expr: ast.expr) -> str | None:
    """Leftmost name of a decorator expression (``vmon`` for ``@vmon.method(x=1)``)."""
    node: ast.expr = expr
    while True:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            node = node.value
        elif isinstance(node, ast.Call):
            node = node.func
        elif isinstance(node, ast.Subscript):
            node = node.value
        else:
            return None


def _strip_vmon_decorators(
    node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
    vmon_names: set[str],
) -> None:
    """Remove vmon-bound decorators from ``node`` and every nested definition.

    Recursion matters for classes: shipped ``@vmon.method``/``@vmon.enter``
    tags on methods would otherwise NameError in the guest.
    """
    for child in ast.walk(node):
        if isinstance(child, _DEFINITION_NODES) and child.decorator_list:
            child.decorator_list = [
                decorator
                for decorator in child.decorator_list
                if _root_name(decorator) not in vmon_names
            ]


def _strip_all_decorators(source: str) -> str:
    """Fallback rendering when the target's module tree is unavailable."""
    tree = ast.parse(textwrap.dedent(source))
    for node in tree.body:
        if isinstance(node, _DEFINITION_NODES):
            node.decorator_list = []
            return ast.unparse(node) + "\n"
    raise ValueError("remote target source must contain a function or class definition")


def _import_bound_names(node: ast.Import | ast.ImportFrom) -> list[str]:
    names: list[str] = []
    for alias in node.names:
        if alias.name == "*":
            continue
        if alias.asname:
            names.append(alias.asname)
        else:
            names.append(alias.name.split(".", 1)[0])
    return names


def _assignment_bound_names(targets: Iterable[ast.expr]) -> list[str]:
    names: list[str] = []
    for target in targets:
        if isinstance(target, ast.Name):
            names.append(target.id)
        elif isinstance(target, ast.Tuple | ast.List):
            names.extend(_assignment_bound_names(target.elts))
    return names


def _free_names(source: str) -> set[str]:
    try:
        table = symtable.symtable(source, "<remote-fn>", "exec")
    except SyntaxError:
        return set()
    names: set[str] = set()
    stack = [table]
    while stack:
        scope = stack.pop()
        for symbol in scope.get_symbols():
            if symbol.is_global():
                names.add(symbol.get_name())
        stack.extend(scope.get_children())
    return names
