# run-context-py

Minimal Python helpers to run algorithms from a `run_context.json` file.

Context/background discussion:
https://github.com/orgs/vantage6/discussions/2556

The goal is to keep algorithm code platform-agnostic: if a platform can produce
`run_context.json`, your algorithm can use this package to parse it and dispatch
the requested function.

## What This Package Provides

- `RunContext`: load and query `run_context.json`
- `dispatch()`: resolve and call function selected by `entrypoint.name`
- `run-context-dispatch`: console script that calls `dispatch()`
- `@run_context(...)`: inject values from run context into function args
- optional strict schema validation via `validate=True`

## Quick Example

```python
from pathlib import Path
from run_context import dispatch, run_context

@run_context(
    input_uris="dataset_path",
    named_arguments="column_name",
    output_uris="output_path",
)
def partial_mean(
    dataset_path: Path,
    output_path: Path,
    column_name: str,
) -> None:
    # your algorithm logic here
    ...

if __name__ == "__main__":
    dispatch()
```

`input_uris="dataset_path"` and `output_uris="output_path"` each require
exactly one entry in their corresponding run-context section.

## Register Python Entry Points

`dispatch()` resolves callables from Python entry-point group `"run_context"`.
By default it requires those entrypoints to come from exactly one installed
distribution (installed package).

```toml
[project.entry-points."run_context"]
partial_mean = "myalgo.algos:partial_mean"
```

Then if `run_context.json` contains:

```json
{
  "entrypoint": { "name": "partial_mean" },
  "arguments": { "named": { "column_name": "age" } },
  "inputs": [{ "uri": "/mnt/data/input.csv" }],
  "outputs": [{ "uri": "/mnt/data/output.json" }]
}
```

`dispatch()` will call the `partial_mean` function and inject those values.

## Console Script

This package also provides a CLI entrypoint:

```bash
run-context-dispatch
```

It behaves like:

```python
from run_context import dispatch
dispatch()
```

## What Algorithm Developers Need To Do

Minimal checklist:

1. Implement your algorithm function and decorate it with `@run_context(...)`.
2. Register that function in Python entry points group `"run_context"` in your
   `pyproject.toml`.
3. Ensure your runtime/container has both your algorithm package and
   `run-context-py` installed.
4. Run either:
   - your own bootstrap code that calls `dispatch()`, or
   - the provided script `run-context-dispatch`.
5. Ensure the platform provides `RUN_CONTEXT_FILE` pointing to a valid
   `run_context.json`.

At runtime, `dispatch()` selects and calls the function named in
`run_context.json -> entrypoint.name`.

## Strict `run_context.json` Validation

By default, run-context loading is lightweight: fields are checked when used
(for example when resolving `entrypoint.name`, reading URIs, or injecting named
arguments).

If you want strict upfront schema validation of the full `run_context.json`,
enable it by setting:

```bash
RUN_CONTEXT_FILE_VALIDATE=1
```

When enabled, `run-context-dispatch` calls `dispatch(validate=True)`, which
validates the payload against the JSON Schema before dispatch.

Note: strict validation requires the optional dependency (`jsonschema`)
installed via the package's `validation` extra.

## Docker CMD/ENTRYPOINT vs Python Entry Points vs Run-Context Entrypoint

These are different things:

- Docker `ENTRYPOINT`/`CMD`:
  process command inside container (for example `python -m myalgo`)
- Python entry points (`[project.entry-points."run_context"]`):
  mapping from string name -> import path for callable
- Run-context `entrypoint.name`:
  runtime selection of which registered callable to execute

Typical container flow:
1. Docker starts your process (ENTRYPOINT/CMD)
2. Your process calls `dispatch()` (directly or via `run-context-dispatch`)
3. `dispatch()` reads `RUN_CONTEXT_FILE`, gets `entrypoint.name`
4. It resolves that name through Python entry points group `"run_context"`
5. It calls the selected function

## Notes

- `RunContext.input_uris()` / `RunContext.output_uris()` return all URI values.
- Mapping-based decorator configs like `input_uris={...}` / `output_uris={...}`
  are reserved for future attribute-based mapping and currently raise
  `NotImplementedError`.
