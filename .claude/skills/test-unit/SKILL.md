---
name: test-unit
description: Run the sql-data-guard unit tests (test/*_unit.py) with pytest
disable-model-invocation: true
argument-hint: "[pytest -k expression]"
allowed-tools: Bash(PYTHONPATH=src venv/bin/python -m pytest*)
---

# Unit tests

Run the unit tests of sql-data-guard.

## Usage

```
/test-unit                 # all unit tests
/test-unit mask            # only tests whose name matches "mask" (pytest -k)
```

## What it does

1. Runs every `test/*_unit.py` file with pytest and colored output. The files are named
   `*_unit.py`, so they must be passed explicitly; `test_sql_guard_llm.py` needs AWS Bedrock
   credentials and is manual-only, so it is not included.
2. Puts `src` on `PYTHONPATH`; the tests import the package from there.

## Command

```sh
PYTHONPATH=src venv/bin/python -m pytest --color=yes test/*_unit.py
```

With an argument, add `-k "<argument>"`.

## Prerequisites

Dependencies in the repo's `venv/` (or any environment with the same packages):

```sh
python -m venv venv && source venv/bin/activate && pip install -r requirements.txt -r test/test.requirements.txt
```

CI also runs `ruff check src test` and `mypy`, and tests the oldest and newest allowed `sqlglot`
(see `CLAUDE.md`); this skill runs only the unit tests with whatever `sqlglot` is installed.
