"""
Optional run-context validation using jsonschema.

This module is only required when strict schema validation is requested.
Install the distribution with the `validation` extra.
"""

from __future__ import annotations

import json
from functools import lru_cache
from importlib.resources import files
from typing import Any, Iterable


class RunContextValidationError(ValueError):
    """
    Raised when run-context JSON does not satisfy the expected structure.
    """

    def __init__(self, path: str, message: str) -> None:
        super().__init__(f"Invalid run context at '{path}': {message}")
        self.path = path
        self.message = message


class RunContextValidationUnavailableError(RuntimeError):
    """
    Raised when strict validation is requested but validation dependency is missing.
    """


def _json_path(path: Iterable[Any]) -> str:
    parts = ["$"]
    for item in path:
        if isinstance(item, int):
            parts.append(f"[{item}]")
        else:
            parts.append(f".{item}")
    return "".join(parts)


def _jsonschema_validator():
    # Lazy import so the default runtime stays lightweight and dependency-light.
    # `jsonschema` is only needed when strict validation is explicitly enabled.
    try:
        from jsonschema import Draft202012Validator
    except ModuleNotFoundError as exc:
        raise RunContextValidationUnavailableError(
            "jsonschema is not installed. Install the distribution with the "
            "'validation' extra."
        ) from exc

    return Draft202012Validator


@lru_cache(maxsize=1)
def _load_schema() -> dict[str, Any]:
    schema_path = files("run_context").joinpath("schemas/run-context.schema.json")
    with schema_path.open("r", encoding="utf-8") as fp:
        schema = json.load(fp)
    if not isinstance(schema, dict):
        raise RuntimeError("run-context schema must be a JSON object")
    return schema


def validate_run_context(payload: Any) -> None:
    """
    Validate run-context JSON using Draft 2020-12 schema.

    Raises `RunContextValidationError` on schema violations and
    `RunContextValidationUnavailableError` when strict validation is requested
    but the optional `jsonschema` dependency is not installed.
    """

    Draft202012Validator = _jsonschema_validator()
    validator = Draft202012Validator(_load_schema())
    errors = sorted(validator.iter_errors(payload), key=lambda err: list(err.path))
    if not errors:
        return

    first_error = errors[0]
    raise RunContextValidationError(
        _json_path(first_error.path), first_error.message
    )
