---
name: test-unit
description: Run all unit tests for bedrock-observability using pytest
disable-model-invocation: true
allowed-tools: Bash(PYTHONPATH=src:tests venv/bin/python -m pytest*)
---

# Unit Tests

Run all unit tests for the bedrock-observability project.

## Usage

```
/test-unit
```

## What it does

1. Runs all unit tests in the `test/` directory ending with `_unit.py`
2. Uses pytest with colored output for better readability
3. Sets PYTHONPATH to include `src` and `test` directories

## Prerequisites

Ensure dependencies are installed:
```sh
python -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt --upgrade && pip install -r requirements-dev.txt --upgrade
```

## Command

```sh
PYTHONPATH=src:tests venv/bin/python -m pytest --color=yes tests/*_unit.py
```