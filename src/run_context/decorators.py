"""
Decorator utilities for run-context entrypoints.
"""

from __future__ import annotations

from functools import wraps
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from run_context.core import RunContext

_NEEDS_CONTEXT_ATTR = "__run_context_needs_context__"


def _normalize_named_arguments(value: str | Iterable[str] | None) -> tuple[str, ...]:
    """
    Normalize argument names (from run_context.json -> arguments.named)
    to a tuple of non-empty strings.
    """
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    names = tuple(item for item in value if isinstance(item, str) and item)
    return names


def _mark_wants_context(func: Callable[..., Any]) -> None:
    setattr(func, _NEEDS_CONTEXT_ATTR, True)


def _input_uri_by_arguments(context: RunContext, selector: Mapping[str, Any]) -> Any:
    """
    Return exactly one input URI Path matching `inputs[*].arguments` selector.

    Example selector shape:
    `{"bind": "features"}` matches an input item like:
    `{"uri": "/mnt/data/features.csv", "arguments": {"bind": "features"}}`.

    When a selector has multiple key/value pairs, all pairs must match on
    `inputs[*].arguments` (logical AND). Example:
    `{"bind": "features", "split": "train"}`.
    """
    inputs = context.inputs
    if not isinstance(inputs, list):
        raise ValueError("Run context inputs must be a list")

    matches = []
    for idx, item in enumerate(inputs):
        if not isinstance(item, dict):
            raise ValueError(f"Run context inputs[{idx}] must be a dict")
        if "uri" not in item:
            raise ValueError(f"Run context inputs[{idx}] is missing required field 'uri'")
        uri = item["uri"]
        if not isinstance(uri, str) or not uri:
            raise ValueError(
                f"Run context inputs[{idx}].uri must be a non-empty string"
            )

        arguments = item.get("arguments")
        if arguments is None:
            continue
        if not isinstance(arguments, Mapping):
            raise ValueError(
                f"Run context inputs[{idx}].arguments must be a mapping when present"
            )
        # A selector could be made up of multiple key=value, we want all to match
        item_matches = all(
            arguments.get(k) == v
            for k, v in selector.items()
        )

        if item_matches:
            matches.append(Path(uri))

    if len(matches) != 1:
        raise ValueError(
            "Expected exactly 1 run-context input URI value matching selector "
            f"{dict(selector)!r}, found {len(matches)}"
        )
    return matches[0]


def _inject_input_uris_from_mapping(
    context: RunContext,
    mapping: Mapping[str, Any],
    kwargs: dict[str, Any],
) -> None:
    """
    Inject configured function args from selector-matched run-context input URIs.

    Mapping shape example:
    ```
    {
      argument_name: selector,
      ...
      "features_path": {
        "bind": "features"
      },
      "y_path": {
        "bind": "y"
      }
    }
    ```
    where each selector is matched against `inputs[*].arguments`.
    If a selector contains multiple key/value pairs, they are combined with
    logical AND.
    """
    for argument_name, selector in mapping.items():
        if not isinstance(argument_name, str) or not argument_name:
            raise TypeError(
                "run_context(input_uris={...}) keys must be non-empty strings"
            )
        if not isinstance(selector, Mapping):
            raise TypeError(
                "run_context(input_uris={...}) values must be mappings like "
                "{'bind': 'features'}"
            )
        if not selector:
            raise TypeError(
                "run_context(input_uris={...}) each mapping value must be a non-empty "
                "mapping of input.arguments key/value selectors, for example "
                "{'bind': 'features'}"
            )
        for selector_key in selector:
            if not isinstance(selector_key, str) or not selector_key:
                raise TypeError(
                    "run_context(input_uris={...}) selector keys must be non-empty "
                    "strings"
                )
        kwargs[argument_name] = _input_uri_by_arguments(context, selector)


def run_context(
    func: Callable[..., Any] | None = None,
    *,
    input_uris: str | Mapping[str, Any] | None = None,
    named_arguments: str | Iterable[str] | None = None,
    output_uris: str | Mapping[str, Any] | None = None,
    run_context: bool = False,
) -> Callable[..., Any]:
    """
    Configure run-context argument injection for an entrypoint function.

    Injected values are sourced from run-context:
    - `input_uris`: single input URI Path when configured as a string, or
      selector-based input URI Paths when configured as a mapping, for example:
      `{"features_path": {"bind": "features"}, "y_path": {"bind": "y"}}`
    - `output_uris`: single output URI Path when configured as a string
    - `named_arguments`: named run arguments

    Can be used as `@run_context` or `@run_context(...)`.

    If `run_context=True`, the `RunContext` object is injected to wrapped
    function as keyword argument `run_context`.
    """

    input_uris_config = input_uris
    output_uris_config = output_uris
    normalized_named_args = _normalize_named_arguments(named_arguments)
    include_context = run_context

    def decorator(inner: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(inner)
        def wrapper(*args, **kwargs):
            context = kwargs.pop("run_context", None)
            if context is None:
                # Direct Python calls bypass run-context injection entirely.
                return inner(*args, **kwargs)
            if not isinstance(context, RunContext):
                raise TypeError("'run_context' must be a RunContext instance")

            if input_uris_config:
                if isinstance(input_uris_config, str):
                    # A single target argument name implies exactly one input URI.
                    input_uris = context.input_uris()
                    if len(input_uris) != 1:
                        raise ValueError(
                            f"Expected exactly 1 run-context inputs URI value(s), "
                            f"found {len(input_uris)}"
                        )
                    kwargs[input_uris_config] = input_uris[0]
                elif isinstance(input_uris_config, Mapping):
                    _inject_input_uris_from_mapping(
                        context,
                        mapping=input_uris_config,
                        kwargs=kwargs,
                    )
                else:
                    raise TypeError(
                        "'input_uris' must be a string, a mapping, or None"
                    )
            if output_uris_config:
                if isinstance(output_uris_config, str):
                    # A single target argument name implies exactly one output URI.
                    output_uris = context.output_uris()
                    if len(output_uris) != 1:
                        raise ValueError(
                            f"Expected exactly 1 run-context outputs URI value(s), "
                            f"found {len(output_uris)}"
                        )
                    kwargs[output_uris_config] = output_uris[0]
                elif isinstance(output_uris_config, Mapping):
                    raise NotImplementedError(
                        "run_context(output_uris={...}) mappings are not implemented yet. "
                        "Planned for future role-based URI mapping."
                    )
                else:
                    raise TypeError(
                        "'output_uris' must be a string, a mapping, or None"
                    )
            if normalized_named_args:
                named = context.named_args()
                for name in normalized_named_args:
                    if name in named:
                        kwargs[name] = named[name]

            if include_context:
                kwargs["run_context"] = context

            return inner(*args, **kwargs)

        _mark_wants_context(wrapper)
        return wrapper

    # Support both styles: @run_context and @run_context(...)
    if func is not None:
        return decorator(func)
    return decorator
