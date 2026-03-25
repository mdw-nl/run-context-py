import json

import pytest

from run_context import RunContext, dispatch, run_context
from run_context.validation import RunContextValidationUnavailableError


def _write_json(path, payload):
    path.write_text(json.dumps(payload), encoding="utf-8")


def _valid_payload(input_uri: str, output_uri: str, *, name: str = "demo", named=None):
    return {
        "entrypoint": {"name": name},
        "arguments": {"named": named or {}},
        "inputs": [{"uri": input_uri}],
        "outputs": [{"uri": output_uri}],
    }


class _FakeEntryPoint:
    def __init__(self, name, func):
        self.name = name
        self._func = func

    def load(self):
        return self._func


def test_run_context_helpers(tmp_path, monkeypatch):
    in_path = tmp_path / "input.csv"
    out_path = tmp_path / "out.json"
    ctx_path = tmp_path / "run_context.json"
    _write_json(ctx_path, _valid_payload(str(in_path), str(out_path), named={"x": 1}))

    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))
    context = RunContext.from_env()

    assert context.entrypoint_name() == "demo"
    assert context.named_args() == {"x": 1}
    assert context.input_uris() == [in_path]
    assert context.output_uris() == [out_path]


def test_dispatch_injects_arguments(tmp_path, monkeypatch):
    in_path = tmp_path / "input.csv"
    out_path = tmp_path / "out.json"
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        _valid_payload(
            str(in_path),
            str(out_path),
            name="dispatch_demo",
            named={"model_name": "simple"},
        ),
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    captured = {}

    @run_context(
        input_uris="dataset_path",
        named_arguments="model_name",
        output_uris="output_path",
    )
    def dispatch_demo(dataset_path, output_path, model_name=None):
        captured["dataset_path"] = dataset_path
        captured["output_path"] = output_path
        captured["model_name"] = model_name

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [_FakeEntryPoint("dispatch_demo", dispatch_demo)],
    )
    dispatch()

    assert captured["dataset_path"] == in_path
    assert captured["output_path"] == out_path
    assert captured["model_name"] == "simple"


def test_input_uris_string_requires_single_input(tmp_path, monkeypatch):
    out_path = tmp_path / "out.json"
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        {
            "entrypoint": {"name": "single_input_demo"},
            "arguments": {"named": {}},
            "inputs": [{"uri": "a.csv"}, {"uri": "b.csv"}],
            "outputs": [{"uri": str(out_path)}],
        },
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    @run_context(input_uris="dataset_path", output_uris="output_path")
    def single_input_demo(dataset_path, output_path):
        raise AssertionError("should not be reached")

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [_FakeEntryPoint("single_input_demo", single_input_demo)],
    )
    with pytest.raises(ValueError, match="exactly 1 run-context inputs URI value\\(s\\)"):
        dispatch()


def test_output_uris_string_requires_single_output(tmp_path, monkeypatch):
    in_path = tmp_path / "input.csv"
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        {
            "entrypoint": {"name": "single_output_demo"},
            "arguments": {"named": {}},
            "inputs": [{"uri": str(in_path)}],
            "outputs": [{"uri": "a.json"}, {"uri": "b.json"}],
        },
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    @run_context(input_uris="dataset_path", output_uris="output_path")
    def single_output_demo(dataset_path, output_path):
        raise AssertionError("should not be reached")

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [_FakeEntryPoint("single_output_demo", single_output_demo)],
    )
    with pytest.raises(ValueError, match="exactly 1 run-context outputs URI value\\(s\\)"):
        dispatch()


def test_input_uris_mapping_injects_selector_matches(tmp_path, monkeypatch):
    dataset_path = tmp_path / "input.csv"
    config_path = tmp_path / "config.json"
    out_path = tmp_path / "out.json"
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        {
            "entrypoint": {"name": "mapping_demo"},
            "arguments": {"named": {}},
            "inputs": [
                {
                    "id": "default",
                    "uri": str(dataset_path),
                    "type": "csv",
                    "arguments": {"kind": "dataset"},
                },
                {
                    "id": "config",
                    "uri": str(config_path),
                    "type": "json",
                    "arguments": {"kind": "config"},
                },
            ],
            "outputs": [{"uri": str(out_path)}],
        },
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    captured = {}

    @run_context(
        input_uris={
            "dataset_path": {"kind": "dataset"},
            "config_path": {"kind": "config"},
        },
        output_uris="output_path",
    )
    def mapping_demo(dataset_path, config_path, output_path):
        captured["dataset_path"] = dataset_path
        captured["config_path"] = config_path
        captured["output_path"] = output_path

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [_FakeEntryPoint("mapping_demo", mapping_demo)],
    )
    dispatch()

    assert captured["dataset_path"] == dataset_path
    assert captured["config_path"] == config_path
    assert captured["output_path"] == out_path


def test_input_uris_mapping_raises_when_no_match(tmp_path, monkeypatch):
    in_path = tmp_path / "input.csv"
    out_path = tmp_path / "out.json"
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        {
            "entrypoint": {"name": "mapping_no_match_demo"},
            "arguments": {"named": {}},
            "inputs": [{"uri": str(in_path), "arguments": {"bind": "dataset"}}],
            "outputs": [{"uri": str(out_path)}],
        },
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    @run_context(input_uris={"dataset_path": {"bind": "config"}}, output_uris="output_path")
    def mapping_no_match_demo(dataset_path, output_path):
        raise AssertionError("should not be reached")

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [_FakeEntryPoint("mapping_no_match_demo", mapping_no_match_demo)],
    )
    with pytest.raises(
        ValueError,
        match="Expected exactly 1 run-context input URI value matching selector",
    ):
        dispatch()


def test_input_uris_mapping_raises_when_multiple_match(tmp_path, monkeypatch):
    out_path = tmp_path / "out.json"
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        {
            "entrypoint": {"name": "mapping_multi_match_demo"},
            "arguments": {"named": {}},
            "inputs": [
                {"uri": "a.csv", "arguments": {"bind": "dataset"}},
                {"uri": "b.csv", "arguments": {"bind": "dataset"}},
            ],
            "outputs": [{"uri": str(out_path)}],
        },
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    @run_context(input_uris={"dataset_path": {"bind": "dataset"}}, output_uris="output_path")
    def mapping_multi_match_demo(dataset_path, output_path):
        raise AssertionError("should not be reached")

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [
            _FakeEntryPoint("mapping_multi_match_demo", mapping_multi_match_demo)
        ],
    )
    with pytest.raises(
        ValueError,
        match="Expected exactly 1 run-context input URI value matching selector",
    ):
        dispatch()


def test_dispatch_raises_on_unknown_entrypoint(tmp_path, monkeypatch):
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        _valid_payload("input.csv", "output.json", name="does_not_exist"),
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [_FakeEntryPoint("some_other_name", lambda: None)],
    )
    with pytest.raises(ValueError, match="Unsupported run-context entrypoint"):
        dispatch()


def test_validation_missing_dependency_raises_custom_error(monkeypatch):
    from run_context import validation as validation_module

    def _raise_missing_dependency():
        raise RunContextValidationUnavailableError("missing jsonschema")

    monkeypatch.setattr(
        validation_module,
        "_jsonschema_validator",
        _raise_missing_dependency,
    )

    with pytest.raises(RunContextValidationUnavailableError, match="jsonschema"):
        validation_module.validate_run_context({})


def test_run_context_decorator_injects_context(tmp_path, monkeypatch):
    in_path = tmp_path / "input.csv"
    out_path = tmp_path / "out.json"
    ctx_path = tmp_path / "run_context.json"
    _write_json(
        ctx_path,
        _valid_payload(str(in_path), str(out_path), name="with_context"),
    )
    monkeypatch.setenv("RUN_CONTEXT_FILE", str(ctx_path))

    captured = {}

    @run_context(run_context=True)
    def with_context(run_context):
        captured["entrypoint_name"] = run_context.entrypoint_name()

    monkeypatch.setattr(
        "run_context.core.entry_points",
        lambda *, group: [_FakeEntryPoint("with_context", with_context)],
    )
    dispatch()
    assert captured["entrypoint_name"] == "with_context"


def test_cli_main_dispatches_with_validate_false_by_default(monkeypatch):
    from run_context import cli

    monkeypatch.delenv("RUN_CONTEXT_FILE_VALIDATE", raising=False)
    captured = {}

    def _capture_dispatch(*, validate=False):
        captured["validate"] = validate

    monkeypatch.setattr("run_context.cli.dispatch", _capture_dispatch)
    cli.main()
    assert captured == {"validate": False}


def test_cli_main_dispatches_with_validate_true_when_env_enabled(monkeypatch):
    from run_context import cli

    monkeypatch.setenv("RUN_CONTEXT_FILE_VALIDATE", "true")
    captured = {}

    def _capture_dispatch(*, validate=False):
        captured["validate"] = validate

    monkeypatch.setattr("run_context.cli.dispatch", _capture_dispatch)
    cli.main()
    assert captured == {"validate": True}
