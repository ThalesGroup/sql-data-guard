# Security Policy

## Reporting a vulnerability

Please **do not open a public issue** for a security problem. Report it privately to
security@opensource.thalesgroup.com, including the SQL query, the configuration and the dialect that reproduce it
(a bypass is any query that `verify_sql` allows, or "fixes", but that still reads data the configuration does not permit).

## Supported versions

Security fixes are made on the latest release. Older releases are not patched; please upgrade.

## What sql-data-guard does and does not protect against

- It checks a query against the configuration and can repair it. It does not run queries and does not replace database permissions: use it as an additional layer.
- The configuration is trusted. The REST service and the MCP wrapper take or load the policy themselves; do not expose them to callers who should not be able to choose the policy, and do not expose the REST service without authentication.
- Function blocking is a deny-list. Add functions of your database that read files, run commands or reach other systems to `blocked_functions`, and give the database account only the permissions it needs.
- Results depend on how [sqlglot](https://github.com/tobymao/sqlglot) parses the query. Pass the dialect of the database that runs the query, and keep the supported sqlglot range from `pyproject.toml`.

## Testing

`test/test_bypass_unit.py` holds adversarial scenarios that are executed against seeded data and fail if forbidden data comes back. New bypass attempts are welcome as pull requests.

## Good practices

**Never store credentials in source code or configuration files in the repository.** Use environment variables for secrets in CI/CD (for example GitHub Secrets) and a secret manager in production.
