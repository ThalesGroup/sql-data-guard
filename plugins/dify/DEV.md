## Local Packaging
To test the plugin locally install the Dify CLI and run:
```sh
dify plugin package ../dify --output_path sql_data_guard.difypkg
```

## Version

`version` in `manifest.yaml` is the plugin version: raise it for every change that is published.
`meta.version` is the version of the manifest format and stays as it is.
`requirements.txt` pins the `sql_data_guard` release the plugin uses: raise it after a new library
release (the plugin works with older releases too, but only newer ones have the latest checks).

## Tests

`test/test_dify_plugin_unit.py` in the repository root tests the tool with a stub of the Dify SDK:

```sh
PYTHONPATH=src python -m pytest test/test_dify_plugin_unit.py
```

