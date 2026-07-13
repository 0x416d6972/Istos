# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |
| < 0.1   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in Istos, please report it responsibly:

1. **Do not** open a public GitHub issue for security vulnerabilities.
2. Email **corvology@gmail.com** with:
   - A description of the vulnerability
   - Steps to reproduce
   - Potential impact assessment
3. You will receive an acknowledgment within 48 hours.
4. We will work with you to understand and fix the issue before public disclosure.

## Security Features

Istos supports transport-level security via Zenoh:

- TLS and mTLS certificate configuration
- Username/password authentication
- Raw PEM injection for secret managers (Vault, AWS Secrets Manager)

See the [Security Guide](docs/user-guide/security.md) for configuration details.

## Best Practices

- Always use TLS in production deployments
- Never commit certificates or credentials to version control
- Use environment variables or secret managers for sensitive configuration
- Run Zenoh routers with authentication enabled in multi-tenant environments
- Keep `eclipse-zenoh` updated to the latest stable release
