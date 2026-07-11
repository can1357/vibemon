"""Bundler behavior: transitive dependencies, classes, decorators, include."""

from __future__ import annotations

import sys
import textwrap

import pytest

from vmon._remote_source import bundle_source

MODULE = '''
import functools
import math
import vmon
from vmon import cls, enter, function, method

LIMIT = 3
NAMES = ["a", "b"]


def helper(value):
    return value + LIMIT


def chained(value):
    return helper(value) * 2


@functools.lru_cache(maxsize=8)
def cached(value):
    return chained(value)


@function(image="python:3.14-slim")
def target(value):
    return cached(value) + math.floor(0.5)


@vmon.cls(image="python:3.14-slim")
class Counter:
    """Counts things."""

    def __init__(self, start=0):
        self.n = start

    @enter
    def warm(self):
        self.n += 1

    @method
    @functools.wraps(helper)
    def bump(self):
        self.n += 1
        return self.n


class Standalone:
    tag = NAMES


def unrelated():
    return "unused"
'''


@pytest.fixture(scope="module")
def sample_module(tmp_path_factory):
    directory = tmp_path_factory.mktemp("bundlemod")
    path = directory / "bundle_sample.py"
    path.write_text(textwrap.dedent(MODULE))
    sys.path.insert(0, str(directory))
    try:
        import bundle_sample

        yield bundle_sample
    finally:
        sys.path.remove(str(directory))
        sys.modules.pop("bundle_sample", None)


def test_function_bundle_pulls_transitive_helpers_constants_imports(sample_module) -> None:
    bundle = bundle_source(sample_module.target._function)
    source = bundle.source
    assert "def helper" in source and "def chained" in source and "def cached" in source
    assert "LIMIT = 3" in source
    assert "import math" in source and "import functools" in source
    assert "def unrelated" not in source
    assert bundle.name == "target" and bundle.module == "bundle_sample"
    assert len(bundle.sha256) == 64

    namespace: dict[str, object] = {}
    exec(source, namespace)
    assert namespace["target"](2) == 10  # helper(2)=5, chained doubles it, floor adds 0


def test_vmon_decorators_stripped_but_others_kept(sample_module) -> None:
    source = bundle_source(sample_module.target._function).source
    assert "@function" not in source
    assert "@functools.lru_cache(maxsize=8)" in source
    assert "vmon" not in source.replace("bundle_sample", "")


def test_class_bundle_strips_method_tags_recursively(sample_module) -> None:
    bundle = bundle_source(sample_module.Counter._cls)
    source = bundle.source
    assert source.rstrip().endswith("return self.n")
    assert "@vmon.cls" not in source and "@enter" not in source and "@method" not in source
    assert "@functools.wraps(helper)" in source, "non-vmon method decorators must survive"
    assert "def helper" in source, "decorator dependencies must be bundled"

    namespace: dict[str, object] = {}
    exec(source, namespace)
    counter = namespace["Counter"](5)
    counter.warm()
    assert counter.bump() == 7


def test_include_forces_unreferenced_definitions(sample_module) -> None:
    source = bundle_source(sample_module.target._function).source
    assert "class Standalone" not in source

    source = bundle_source(
        sample_module.target._function, include=(sample_module.Standalone,)
    ).source
    assert "class Standalone" in source
    assert 'NAMES = ["a", "b"]' in source.replace("'", '"'), "include dependencies join too"


def test_include_rejects_non_module_level_entries(sample_module) -> None:
    with pytest.raises(ValueError, match="include entry"):
        bundle_source(sample_module.target._function, include=(dict,))


def test_closures_rejected_with_stable_message() -> None:
    outer = 1

    def closes_over():
        return outer

    with pytest.raises(ValueError, match="cannot close over local variables"):
        bundle_source(closes_over)


def test_nested_function_falls_back_to_own_source() -> None:
    def nested(value):
        return value + 1

    bundle = bundle_source(nested)
    assert bundle.source == "def nested(value):\n    return value + 1\n"
    assert bundle.module == __name__


def test_builtin_targets_are_rejected() -> None:
    with pytest.raises((ValueError, TypeError)):
        bundle_source(len)
