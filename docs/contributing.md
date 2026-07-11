# Contributing to Istos

Thank you for considering contributing to Istos! This document provides guidelines and instructions for contributing to the project.

## Code of Conduct

Please be respectful and considerate of others when contributing to this project. We aim to foster an inclusive and welcoming community.

## How to Contribute

There are many ways to contribute to Istos:

1. **Report bugs**: If you find a bug, please create an issue on GitHub with a detailed description of the problem, including steps to reproduce it.

2. **Suggest features**: If you have an idea for a new feature or improvement, please create an issue on GitHub to discuss it.

3. **Contribute code**: If you want to contribute code, please follow the steps below.

## Development Setup

1. Fork the repository on GitHub.

2. Clone your fork locally:
   ```bash
   git clone https://github.com/A111ir/Istos.git
   cd Istos
   ```

3. Create a virtual environment and install development dependencies:
   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install -e ".[dev]"
   ```

4. Create a branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

## Development Guidelines

### Code Style

We follow the [PEP 8](https://www.python.org/dev/peps/pep-0008/) style guide for Python code. We use the following tools to enforce code style:

- **mypy**: For static type checking
- **pylint**: For linting

You can run these tools with:
```bash
mypy src/istos
pylint src/istos
```

### Documentation

- All functions, classes, and modules should have docstrings following the Google docstring format.
- Update the documentation when adding or modifying features.
- Run the documentation locally to check your changes:
  ```bash
  mkdocs serve
  ```

### Testing

- Write tests for all new features and bug fixes.
- Make sure all tests pass before submitting a pull request:
  ```bash
  pytest tests/
  ```

## Pull Request Process

1. Update the documentation with details of changes to the interface, if applicable.
2. Update the tests to cover your changes.
3. Make sure all tests pass.
4. Submit a pull request to the `main` branch.
5. The pull request will be reviewed by maintainers, who may request changes or improvements.
6. Once approved, your pull request will be merged.

## Adding New Decorators or Core Features

If you want to add a new decorator or core feature:

1. Add the implementation to the appropriate module under `src/istos/core/`.
2. Write comprehensive docstrings with parameters, return values, and examples.
3. Add tests for the new functionality under `tests/`.
4. Update the documentation to include the new feature.
5. Register the decorator in `Istos.py` and expose it via `__init__.py`.

## License

By contributing to Istos, you agree that your contributions will be licensed under the project's Apache License 2.0.
