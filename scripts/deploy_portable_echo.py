#!/usr/bin/env python3
"""Deploy the portable echo functions used by the real-daemon SDK smoke tests."""

from __future__ import annotations

import argparse
import importlib
import re
from pathlib import Path

import vmon

_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


def echo(value: object) -> object:
    return value


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--api-token", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--python-image", required=True)
    return parser.parse_args()


def main() -> None:
    args = arguments()
    if not _SAFE_NAME.fullmatch(args.namespace):
        raise SystemExit("namespace must contain only letters, digits, dot, underscore, or hyphen")

    names = {"json": "portable-json-echo", "cbor": "portable-cbor-echo"}
    deployed_echo = importlib.import_module("deploy_portable_echo").echo
    package_root = str(Path(__file__).resolve().parent)
    with vmon.connect(args.server_url, token=args.api_token, discover=False, timeout=30) as client:
        revisions: dict[str, str] = {}
        for codec, name in names.items():
            function = vmon.function(
                deployed_echo,
                client=client,
                namespace=args.namespace,
                name=name,
                image=args.python_image,
                serializer=codec,
                startup_timeout=120,
                execution_timeout=60,
                result_ttl=3600,
                package_root=package_root,
                include=("deploy_portable_echo.py",),
            )
            record = function.activate()
            revisions[codec] = record.current.revision_id

    # Shell-safe by construction: namespace is validated and function/revision IDs are server identifiers.
    values = {
        "VMON_REMOTE_NAMESPACE": args.namespace,
        "VMON_REMOTE_JSON_NAME": names["json"],
        "VMON_REMOTE_JSON_REVISION": revisions["json"],
        "VMON_REMOTE_CBOR_NAME": names["cbor"],
        "VMON_REMOTE_CBOR_REVISION": revisions["cbor"],
    }
    for key, value in values.items():
        if not _SAFE_NAME.fullmatch(value):
            raise RuntimeError(f"server returned unsafe {key}: {value!r}")
        print(f"export {key}={value}")


if __name__ == "__main__":
    main()
