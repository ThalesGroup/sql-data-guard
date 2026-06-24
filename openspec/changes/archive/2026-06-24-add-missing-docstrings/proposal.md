## Why

The project currently has low public API docstring coverage (13.7%). Since Sphinx is used to auto-extract API documentation from codebase docstrings, the lack of docstrings results in incomplete API documentation. Adding these missing docstrings is essential for accurate, complete, and high-quality Sphinx documentation.

## What Changes

- Add explicit, Sphinx-compatible docstrings to modules, classes, and functions identified as missing in the codebase quality report.
- Detail arguments, return types, exceptions, and overall purposes in the docstrings so Sphinx can generate comprehensive API reference docs.
- Increase the docstring coverage metric as evaluated by the quality reporting script.

## Capabilities

### New Capabilities

<!-- None -->

### Modified Capabilities

- `sphinx-documentation`: Enforce that all public modules, classes, and methods in the core library must have explicit docstrings compatible with Sphinx documentation extraction to ensure complete API reference generation.

## Impact

- Codebase files within `src/sql_data_guard/` will be updated with Python docstrings.
- Sphinx-generated HTML documentation will automatically include the newly documented APIs.
- The `QUALITY_REPORT.md` docstring coverage metric will be greatly improved.
- No behavior or runtime changes to the library execution or other modules.
