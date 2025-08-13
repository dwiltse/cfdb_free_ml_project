# CFDB Free ML Pipeline

A modern college football analytics platform built with Databricks Delta Live Tables, designed for machine learning applications and conversational data access.

## 🏗️ Architecture

```
S3 Raw Data → Bronze Layer → Silver Layer → Gold Layer → ML Models
                    ↓
            MCP Server ← Claude Desktop
```

**Tech Stack:**
- **Data Platform**: Databricks Delta Live Tables (DLT) with Python transformations  
- **Data Architecture**: Medallion (Bronze → Silver → Gold)
- **ML Pipeline**: MLflow integration with Databricks
- **Analytics Interface**: Custom MCP server for conversational data access
- **Deployment**: Databricks Asset Bundles with CI/CD ready structure

## 🚀 Quick Start

### Prerequisites

- Access to Databricks workspace
- Personal Access Token (PAT) for authentication
- Python 3.9+ for local development
- Unity Catalog permissions for `cfdb_free_dev` catalog

### 1. Environment Setup

```bash
# Clone and navigate to project
cd /path/to/cfdb_free_ml_project

# Copy environment template
cp .env.example .env

# Edit .env with your credentials
# DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
# DATABRICKS_ACCESS_TOKEN=your_pat_token
# DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your_warehouse_id
```

### 2. Install Dependencies

```bash
# Install project dependencies
pip install -e ".[dev]"

# Or using uv (recommended)
uv sync --extra dev
```

### 3. Setup Unity Catalog

```bash
# Run the catalog setup script in your Databricks workspace
# Copy and execute: scripts/unity_catalog_setup.sql
```

### 4. Deploy Asset Bundle

```bash
# Validate bundle configuration
databricks bundle validate

# Deploy to development environment
databricks bundle deploy --target dev

# Run the DLT pipeline
databricks bundle run cfdb_data_refresh --target dev
```

## 📊 Data Pipeline

### Bronze Layer (Raw Ingestion)
- `teams_bronze` - FBS team master data
- `games_bronze` - Game results and metadata  
- `season_stats_bronze` - Traditional season statistics
- `game_stats_bronze` - Game-level team performance
- `game_drives_bronze` - Drive-level data
- `plays_bronze` - Play-by-play data
- `advanced_season_stats_bronze` - EPA and advanced season metrics
- `advanced_game_stats_bronze` - EPA and advanced game metrics

### Silver Layer (Business Logic)
- `fact_games_silver` - FBS games with calculated metrics
- `fact_season_stats_silver` - Season stats with efficiency calculations
- `fact_game_stats_silver` - Game stats with time normalization
- `fact_drives_silver` - Drive efficiency and field position analysis
- `fact_advanced_season_stats_silver` - EPA, explosiveness, success rates
- `fact_advanced_game_stats_silver` - Game-level advanced analytics

### Gold Layer (Analytics-Ready)
- `fact_game_predictions_gold` - Game prediction features with EPA differentials
- `dim_team_season_performance_gold` - Complete team performance profiles

## 🤖 MCP Integration

### Setup MCP Server

```bash
cd mcp_server

# Install MCP dependencies
pip install -r requirements.txt

# Configure environment
cp ../.env .env

# Test server
python test_server.py
```

### Claude Desktop Configuration

Add to your Claude Desktop config:

```json
{
  "mcpServers": {
    "cfdb-data": {
      "command": "python",
      "args": ["/path/to/cfdb_free_ml_project/mcp_server/server.py"],
      "env": {
        "DATABRICKS_SERVER_HOSTNAME": "${DATABRICKS_SERVER_HOSTNAME}",
        "DATABRICKS_ACCESS_TOKEN": "${DATABRICKS_ACCESS_TOKEN}"
      }
    }
  }
}
```

## 🔧 Development

### Project Structure

```
cfdb_free_ml_project/
├── databricks.yml              # Asset Bundle configuration
├── pyproject.toml              # Python project configuration
├── src/
│   ├── dlt_pipeline/
│   │   ├── transformations/    # DLT transformations (bronze/silver/gold)
│   │   ├── explorations/       # Data analysis SQL/notebooks
│   │   └── utilities/          # Shared utilities
│   ├── ml/                     # Machine learning components
│   │   ├── training/           # Model training pipelines
│   │   ├── inference/          # Model serving and prediction
│   │   └── features/           # Feature engineering utilities
│   ├── notebooks/              # Jupyter notebooks for analysis
│   └── dashboards/             # Databricks dashboard definitions
├── resources/                  # Bundle resource definitions
├── tests/                      # Unit and integration tests
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   ├── fixtures/               # Test data fixtures
│   └── conftest.py             # Pytest configuration
├── mcp_server/                 # MCP integration
├── docs/                       # Documentation
└── scripts/                    # Deployment scripts
```

### Testing

```bash
# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/

# Run all tests
pytest
```

### Code Quality

```bash
# Format code
black src/ tests/

# Lint code
ruff check src/ tests/

# Type checking (if mypy is installed)
mypy src/
```

## 📈 Analytics Features

### Current Capabilities

- **Team Performance Analysis**: Comprehensive season and game-level metrics
- **EPA Analytics**: Expected Points Added for offensive/defensive efficiency
- **Advanced Metrics**: Explosiveness, success rates, line yards
- **Game Predictions**: Feature engineering for ML models
- **Conversational Analysis**: Natural language queries via MCP

### Available Metrics

- **Traditional Stats**: Yards, points, turnovers, time of possession
- **Efficiency Metrics**: EPA per play, success rate, explosiveness
- **Situational Analytics**: Third down, red zone, late-game performance
- **Field Position**: Starting field position, drive efficiency
- **Game Context**: Competitiveness, conference games, bowl games

## 🚀 Deployment

### Asset Bundle Targets

- **dev**: Development environment (`cfdb_free_dev` catalog)
- **prod**: Production environment (`cfdb_free_prod` catalog)

### Environment Variables

Set via `BUNDLE_VAR_*` environment variables:

```bash
export BUNDLE_VAR_catalog=cfdb_free_dev
export BUNDLE_VAR_warehouse_id=your_warehouse_id
```

### CI/CD Integration

The project is structured for CI/CD deployment:

```yaml
# Example GitHub Actions workflow
- name: Deploy Asset Bundle
  run: databricks bundle deploy --target prod
```

## 📚 Documentation

- [Data Guide](docs/cfdb_data_guide.md) - Comprehensive data dictionary
- [Development Guidelines](CLAUDE.md) - Development best practices
- [DLT Explorations](src/dlt_pipeline/explorations/) - Data analysis examples

## 🔐 Security

- Environment variables for all credentials
- No secrets in version control
- Unity Catalog permissions model
- Asset Bundle security best practices

## 🤝 Contributing

1. Create feature branch from main
2. Make changes following code style guidelines
3. Add tests for new functionality
4. Update documentation as needed
5. Submit pull request

## 📄 License

This project is for educational and analytical purposes. Please respect college football data usage policies.

---

**Built with ❤️ for college football analytics**