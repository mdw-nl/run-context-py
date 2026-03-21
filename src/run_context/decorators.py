"""
Decorator utilities for run-context entrypoints.
"""

from __future__ import annotations

from functools import wraps
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
    - `input_uris`: single input URI Path when configured as a string
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
            # dispatch() will pass run_context
            context = kwargs.pop("run_context", None)
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
                    raise NotImplementedError(
                        "run_context(input_uris={...}) mappings are not implemented yet. "
                        "Planned for future role-based URI mapping."
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
