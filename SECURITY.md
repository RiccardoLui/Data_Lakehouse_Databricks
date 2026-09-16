# Security Policy

This project is a learning/portfolio Databricks lakehouse. Security controls are implemented primarily with Unity Catalog and GitHub CI/CD practices.

## Rules

- Never commit Databricks PATs, cloud keys, passwords, OAuth client secrets, or private keys.
- Keep deployment credentials in GitHub Actions Secrets or, in a production Databricks account, use workload identity federation/service principals.
- Use least-privilege Unity Catalog grants. Analysts should consume Gold tables/views and should not receive Bronze volume write access.
- Prefer governed views, column masks, row filters, or ABAC policies for sensitive data instead of duplicating unmasked extracts.
- Keep production deployment paths out of `/Workspace/Shared`.
- Review changes through pull requests before deployment.

## Free Edition

Databricks Free Edition is designed for learning and has administrative limitations. The runnable governance notebook therefore focuses on Unity Catalog metadata, tags, secure views, and inventory. `Governance/rbac_enterprise_template.sql` is a template for a full Databricks account where account-level groups and service principals are available.

## Reporting

Do not open a public issue containing credentials or private datasets. Revoke/rotate any accidentally exposed credential before removing it from Git history.
