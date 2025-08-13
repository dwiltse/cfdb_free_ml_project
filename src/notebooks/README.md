# Analysis Notebooks

This directory contains Jupyter notebooks for exploratory data analysis and research.

## Notebook Categories

### Data Exploration
- `01_data_exploration.ipynb` - Initial data quality assessment
- `02_schema_analysis.ipynb` - Data schema and relationship exploration
- `03_statistical_summaries.ipynb` - Descriptive statistics and distributions

### Feature Analysis
- `10_epa_analysis.ipynb` - Expected Points Added deep dive
- `11_team_performance_metrics.ipynb` - Team rating system analysis
- `12_game_context_features.ipynb` - Situational factor analysis

### Model Development
- `20_baseline_models.ipynb` - Simple prediction baselines
- `21_feature_engineering.ipynb` - Advanced feature creation
- `22_model_comparison.ipynb` - Algorithm performance comparison

### Business Intelligence
- `30_conference_analysis.ipynb` - Conference strength comparisons
- `31_playoff_analysis.ipynb` - College Football Playoff insights
- `32_historical_trends.ipynb` - Multi-year trend analysis

## Development Guidelines

### Notebook Structure
```
# Title and Objective
## Data Loading
## Exploratory Analysis
## Key Findings
## Next Steps
```

### Best Practices
- Clear markdown documentation
- Reproducible code cells
- Data source citations
- Export key insights to presentations

### Databricks Integration
- Use Databricks Connect for local development
- Leverage Unity Catalog for data access
- Export findings to Delta tables when appropriate

## Usage

### Local Development
```bash
# Install Jupyter
pip install jupyter

# Start notebook server
jupyter notebook src/notebooks/
```

### Databricks Workspace
- Upload notebooks to `/Shared/CFDB-dev/notebooks/`
- Attach to appropriate cluster
- Use collaborative features for team analysis