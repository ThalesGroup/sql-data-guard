# Contributing guidelines

We welcome contributions from everyone. To become a contributor, follow these steps:

1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Make your changes.
4. Submit a pull request.

### Contributing code

When contributing code, please ensure that you follow our coding standards and guidelines. This helps maintain the quality and consistency of the codebase.

## Pull Request Checklist

Before submitting a pull request, please ensure that you have completed the following:

- [ ] Followed the coding style guidelines.
- [ ] Written tests for your changes.
- [ ] Run all tests and ensured they pass.
- [ ] Updated documentation if necessary.

### License

By contributing to this project, you agree that your contributions will be licensed under the project's open-source license.

### Coding style

### Testing

All contributions must be accompanied by tests to ensure that the code works as expected and does not introduce regressions.

#### Running unit tests
To run all the unit tests locally, use the following command:
```sh
sql-data-guard % PYTHONPATH=src python -m pytest --color=yes test/*_unit.py
```
Unit tests also run automatically on every push using a dedicated workflow.

### Version publication

The versions of the projects are managed using git tags. To publish a new version, make sure the main branch is up-to-date and create a new tag with the version number:
```sh
git tag -a v0.1.0 -m "Release 0.1.0"
git push --tags
```
Workflow will automatically publish the new version to PyPI and to the Docker repository under github container registry.

### Issues management

If you find a bug or have a feature request, please create an issue in the GitHub repository. Provide as much detail as possible to help us understand and address the issue.

We will review your issue and respond as soon as possible. Thank you for helping us improve the project!
