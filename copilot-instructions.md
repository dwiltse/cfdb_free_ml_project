## Python Dependency Management

**Single Source of Truth:**
- All Python dependencies should be managed in `pyproject.toml` at the project root.
- Avoid using separate `requirements.txt` files for submodules (e.g., `mcp_server/requirements.txt`).
- Use `[project.optional-dependencies]` in `pyproject.toml` to define extras for submodules or special use cases (e.g., `[project.optional-dependencies.mcp_server]`).

**Why use `pyproject.toml`?**
- It is the modern, standardized configuration file for Python projects (PEP 518/PEP 621).
- Reduces duplication and risk of version conflicts by having a single source of truth.
- Supports optional dependencies, dev dependencies, and version constraints in a structured way.
- Compatible with all major Python tools (pip, uv, poetry, etc.).
- Also stores project metadata and build system requirements.

**Migration Steps:**
1. Move all dependencies from `requirements.txt` files into the appropriate section of `pyproject.toml`.
2. Remove the old `requirements.txt` files or leave a note pointing to `pyproject.toml`.
3. Update documentation to instruct users to install with, e.g., `pip install .[mcp_server]`.

---
applyTo: "**"
---

# Copilot Instructions for CFDB Free ML Pipeline

These guidelines help ensure code quality and consistency for the CFDB Free ML Pipeline project.

## Project Context

College football analytics using Databricks Asset Bundles, PySpark DLT, and ML predictions with EPA metrics.

## Core Patterns by File Type

### DLT Files (`**/bronze/*.py`, `**/silver/*.py`)
- Always parameterize catalog: `spark.conf.get("catalog", "cfdb_free_dev")`
- Include FBS filtering: `(home_classification = 'fbs' OR away_classification = 'fbs')`
- Add audit fields: `_ingest_timestamp`, `_source_file`

### Asset Bundle (`*.yml`)
- Reference individual files in libraries (never directories)
- Use variables: `${var.catalog}`, `${var.warehouse_id}`

### ML Files (`**/ml/*.py`)
- Use EPA-based features for predictions
- Track experiments with MLflow
- Engineer features using differentials

## Data Domain Rules

- **Time Conversion**: Convert MM:SS to seconds for game stats
- **Team Focus**: Include FBS teams and games vs lower divisions
- **Analytics**: Use EPA, success rate, and explosiveness metrics
- **Sources**: Use S3 paths like `s3://ncaadata/{entity}/`

## Never Do

- Hardcode credentials or catalog names
- Reference directories in DAB libraries
- Skip FBS filtering on games/stats
- Commit secrets or tokens

## Example Commands (PowerShell)

```powershell
# Deploy Databricks bundle (dev target)
databricks bundle deploy --target dev

# Run unit tests
pytest tests/unit/

# Format code with Black
black src/ --line-length 100

# Lint code with Ruff
ruff check src/
```
