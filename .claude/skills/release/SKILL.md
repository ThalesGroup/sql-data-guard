---
name: release
description: Release sql-data-guard - date the CHANGELOG, then tag vX.Y.Z on main and push the tag, which publishes to TestPyPI and PyPI. Only from main.
disable-model-invocation: true
argument-hint: "[patch | minor | major | X.Y.Z]"
allowed-tools: Bash(git fetch *) Bash(git status *) Bash(git branch *) Bash(git rev-parse *) Bash(git log *) Bash(git diff *) Bash(git tag --list *) Bash(PYTHONPATH=src venv/bin/python -m pytest*)
---

# Release

Pushing a tag `vX.Y.Z` runs `.github/workflows/pypi-publish.yaml`, which publishes to TestPyPI
and then PyPI. **A published PyPI version can never be reused or replaced**, so every check below
must pass before anything is pushed. `git push` is deliberately not in `allowed-tools`: you will
be asked to approve each push.

## Usage

```
/release            # next patch version, you confirm it
/release minor      # or major
/release 0.2.0      # an explicit version
```

## Steps

Stop at the first failed check, say what is wrong, and do not try to work around it.

1. **Fetch.** `git fetch --tags origin`.
2. **Releases come from `main` only.**
   - `git branch --show-current` must be `main`. If it is not, stop. Do not switch branches or
     merge `dev` yourself: tell the user to get `dev` merged into `main` first.
   - `git status --porcelain` must be empty.
   - `git rev-parse HEAD origin/main` must give the same commit (nothing unpushed, nothing to pull).
3. **Latest version.** `git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname` - the first line.
   If there is none, the first release is `v0.0.1`.
4. **Choose the new version.**
   - An argument decides it: `patch` (default), `minor`, `major`, or an explicit `X.Y.Z`.
   - It must be greater than the latest version, and its tag must not already exist.
   - Read the `## Unreleased` section of `CHANGELOG.md`. It must not be empty. If it has `Added`
     or breaking changes and the argument was only a patch bump (or absent), say that a minor bump
     looks more appropriate and ask which to use before continuing.
5. **Tests must pass.** `PYTHONPATH=src venv/bin/python -m pytest --color=yes test/*_unit.py`.
   Also run `ruff check src test` and `mypy` if they are installed (CI runs both).
6. **Changelog.** Rename `## Unreleased` to `## X.Y.Z - YYYY-MM-DD` (today's date) and add an
   empty `## Unreleased` above it. Show the diff.
7. **Confirm.** State the version, the commit, and that the tag will publish to PyPI. Wait for an
   explicit yes.
8. **Commit and push `main`.** Commit only `CHANGELOG.md` with the message `Release X.Y.Z`
   (follow the session's commit attribution rules), then `git push origin main`. If the push is
   rejected (for example branch protection), stop and report it.
9. **Tag and push.**
   ```sh
   git tag -a "vX.Y.Z" -m "Release X.Y.Z"
   git push origin "vX.Y.Z"
   ```
10. **Report.** The tag and commit, and that the `pypi-publish`, Docker and Dify-plugin workflows
    now run in GitHub Actions. Do not wait for them or poll.

If anything fails after the tag was pushed, do not delete or move the tag: report it and let the
user decide, because the workflow may already have published.
