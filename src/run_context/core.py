"""
Minimal run-context library.

It's a proof-of-concept. See: https://github.com/orgs/vantage6/discussions/2556

Algorithms running on (FL) platforms that generate a run_context.json can use
this library to help them parse it. It mainly provides:
- `dispatch()`: to start the algorithm using the requested entrypoint
- `RunContext`: helpers to parse and access run_context.json
"""

from __future__ import annotations

import json
import os
from importlib.metadata import entry_points
from pathlib import Path
from typing import Any, Callable, Mapping

from run_context.validation import validate_run_context

RUN_CONTEXT_ENTRYPOINT_GROUP = "run_context"

class RunContext:
    """
    Raw run-context data plus helper accessors.

    The full JSON object is preserved in `payload`; section properties read
    `entrypoint`, `arguments`, `inputs`, and `outputs` directly from it.
    """

    def __init__(self, source: Path, payload: Mapping[str, Any]) -> None:
        """
        Create a run-context instance from a source path and parsed
        (json-loaded) payload.
        """
        self.source = source
        self.payload = payload

    @classmethod
    def from_path(cls, path: str | Path, *, validate: bool = False) -> "RunContext":
        """
        Load run-context JSON from file.

        If `validate=True`, strict schema validation is performed using the
        optional validation module.
        """
        source = Path(path)
        with open(source, "r", encoding="utf-8") as fp:
            payload = json.load(fp)

        if not isinstance(payload, dict):
            raise ValueError("Run context must be a JSON object")
        if validate:
            validate_run_context(payload)
        return cls(source=source, payload=payload)

    @classmethod
    def from_env(cls, *, validate: bool = False) -> "RunContext":
        """
        Load run-context from the `RUN_CONTEXT_FILE` environment variable.
        """
        run_context_file = os.environ.get("RUN_CONTEXT_FILE")
        if not run_context_file:
            raise RuntimeError("RUN_CONTEXT_FILE is not set")
        return cls.from_path(run_context_file, validate=validate)

    @property
    def entrypoint(self) -> Any:
        """
        Raw `entrypoint` section from run-context payload.
        """
        return self.payload.get("entrypoint")

    @property
    def arguments(self) -> Any:
        """
        Raw `arguments` section from run-context payload.
        """
        return self.payload.get("arguments")

    @property
    def inputs(self) -> Any:
        """
        Raw `inputs` section from run-context payload.

        Example shape:
        [
          {
            "id": "features",
            "uri": "/mnt/data/features.csv",
            "type": "csv",
            "arguments": {"bind": "features"}
          },
          {
            "id": "targets",
            "uri": "/mnt/data/targets.csv",
            "type": "csv",
            "arguments": {"bind": "y"}
          }
        ]
        """
        return self.payload.get("inputs")

    @property
    def outputs(self) -> Any:
        """
        Raw `outputs` section from run-context payload.
        """
        return self.payload.get("outputs")

    def entrypoint_name(self) -> str:
        """
        Return `entrypoint.name` as a non-empty string.
        """
        if not isinstance(self.entrypoint, dict):
            raise ValueError("Run context field 'entrypoint' must be an object")
        name = self.entrypoint.get("name")
        if not isinstance(name, str) or not name:
            raise ValueError(
                "Run context field 'entrypoint.name' must be a non-empty string"
            )
        return name

    def named_args(self) -> dict[str, Any]:
        """
        Return `arguments.named` when present; otherwise return an empty dict.
        """
        if not isinstance(self.arguments, dict):
            return {}
        named = self.arguments.get("named")
        if isinstance(named, dict):
            return named
        return {}

    def _uris(self, field: str) -> list[Path]:
        """
        Return all URI values from `inputs` or `outputs` as Paths.

        `field` selects which section to inspect (`inputs` or `outputs`).
        A ValueError is raised when the section is malformed.
        """
        if field == "inputs":
            items = self.inputs
        elif field == "outputs":
            items = self.outputs
        else:
            raise ValueError("Run context field selector must be 'inputs' or 'outputs'")
        if not isinstance(items, list):
            raise ValueError(f"Run context {field} must be a list")

        uris: list[Path] = []
        for idx, item in enumerate(items):
            if not isinstance(item, dict):
                raise ValueError(f"Run context {field}[{idx}] must be an object")
            if "uri" not in item:
                raise ValueError(f"Run context {field}[{idx}] is missing required field 'uri'")
            uri = item["uri"]
            if not isinstance(uri, str) or not uri:
                raise ValueError(
                    f"Run context {field}[{idx}].uri must be a non-empty string"
                )
            uris.append(Path(uri))
        return uris

    def input_uris(self) -> list[Path]:
        """
        Return all input URIs as Paths.
        """
        return self._uris(field="inputs")

    def output_uris(self) -> list[Path]:
        """
        Return all output URIs as Paths.
        """
        return self._uris(field="outputs")


def dispatch(*, validate: bool = False) -> None:
    """
    Dispatch to the function selected by `entrypoint.name`.

    The callable is resolved from Python entry points in group `run_context`.
    """
    context = RunContext.from_env(validate=validate)
    requested = context.entrypoint_name()
    func = _resolve_entrypoint_callable(requested)
    kwargs: dict[str, Any] = {}
    if getattr(func, "__run_context_needs_context__", False):
        kwargs["run_context"] = context
    func(**kwargs)


def _resolve_entrypoint_callable(name: str) -> Callable[..., Any]:
    """
    Resolve one callable from entry-point group `run_context`.
    """
    available = list(entry_points(group=RUN_CONTEXT_ENTRYPOINT_GROUP))
    matches = [item for item in available if item.name == name]
    if not matches:
        allowed = ", ".join(sorted(item.name for item in available)) or "<none>"
        raise ValueError(
            f"Unsupported run-context entrypoint '{name}'. "
            f"Allowed entrypoints: {allowed}"
        )
    if len(matches) > 1:
        raise ValueError(
            f"Duplicate run-context entrypoint '{name}' in Python entry-point "
            f"group '{RUN_CONTEXT_ENTRYPOINT_GROUP}'"
        )
    return matches[0].load()
