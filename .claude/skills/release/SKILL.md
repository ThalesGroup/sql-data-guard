---
name: release
description: Create a new release with git tag and push to GitHub. Fetches latest version from git tags and creates next patch version by default.
allowed-tools: Bash(git tag *) Bash(git push *)
---

# Release Version Management

Automates the release workflow for keep-up.

## Usage

Create a new release tag and push to GitHub:

```
/release
```

## What it does

1. **Fetches the latest version** from local git tags: `git tag --list | sort -V | tail -1`
2. **Calculates next version** by incrementing patch version (e.g. v0.0.8 → v0.0.9)
3. **Creates annotated git tag** with release message
4. **Pushes tag** to GitHub

## Implementation

```bash
# Get the latest version from local git tags
LATEST_TAG=$(git tag --list | sort -V | tail -1)

if [ -z "$LATEST_TAG" ]; then
    echo "No tags found. Creating first release v0.0.1"
    NEW_VERSION="0.0.1"
else
    # Remove 'v' prefix and extract version parts
    VERSION=${LATEST_TAG#v}
    IFS='.' read -r MAJOR MINOR PATCH <<< "$VERSION"

    # Increment patch version
    PATCH=$((PATCH + 1))
    NEW_VERSION="$MAJOR.$MINOR.$PATCH"
fi

echo "Creating release v$NEW_VERSION"
git tag -a "v$NEW_VERSION" -m "Release v$NEW_VERSION"
git push origin "v$NEW_VERSION"
```