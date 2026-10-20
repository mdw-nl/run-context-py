"""
Console entrypoint for run-context dispatch.
"""

from __future__ import annotations

from run_context.core import dispatch, get_env


def _is_truthy(value: str | None) -> bool:
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def main() -> None:
    """
    Dispatch using RUN_CONTEXT_FILE and optional strict validation flag.

    Set RUN_CONTEXT_FILE_VALIDATE to a truthy value to enable strict schema
    validation before dispatch. This implies you installed the extra
    [validation] dependencies.
    """
    validate = _is_truthy(get_env("RUN_CONTEXT_FILE_VALIDATE"))
    dispatch(validate=validate)
