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
