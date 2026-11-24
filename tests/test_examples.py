from __future__ import annotations

import runpy
from pathlib import Path

import pytest


EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
EXAMPLE_FILES = sorted(path for path in EXAMPLES_DIR.glob("*.py") if path.is_file())


@pytest.mark.parametrize("example_path", EXAMPLE_FILES, ids=lambda path: path.stem)
def test_examples_execute_and_print_step_annotations(
    capsys: pytest.CaptureFixture[str],
    example_path: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Two examples keep their cache in the working directory. In an empty one
    # every run starts from the same state, as on a fresh checkout, and leaves
    # nothing behind for the next run to load.
    monkeypatch.chdir(tmp_path)
    runpy.run_path(str(example_path), run_name="__main__")
    output = capsys.readouterr().out
    assert "Step " in output
    assert "Example complete." in output
