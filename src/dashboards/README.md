# Databricks Dashboards

This directory contains dashboard definitions for CFDB analytics.

## Dashboard Types

### Team Analytics Dashboard
- **File**: `team_analytics.lvdash.json`
- **Purpose**: Comprehensive team performance analysis
- **Features**:
  - EPA trends over time
  - Offensive/Defensive efficiency comparisons
  - Conference standings and rankings
  - Game-by-game performance breakdown

### Game Prediction Dashboard
- **File**: `game_predictions.lvdash.json`
- **Purpose**: Model performance and prediction monitoring
- **Features**:
  - Weekly prediction accuracy
  - Model confidence distribution
  - Prediction vs actual outcomes
  - Feature importance analysis

### Season Overview Dashboard
- **File**: `season_overview.lvdash.json`
- **Purpose**: High-level season analytics
- **Features**:
  - Conference power rankings
  - Playoff probability tracking
  - Strength of schedule analysis
  - Historical trend comparisons

## Asset Bundle Integration

Dashboards are defined in the Asset Bundle configuration:

```yaml
# resources/dashboards.yml
resources:
  dashboards:
    team_analytics:
      display_name: "CFDB Team Analytics"
      file_path: ./src/dashboards/team_analytics.lvdash.json
      warehouse_id: ${var.warehouse_id}
```

## Development Workflow

1. Create dashboard in Databricks UI
2. Export as `.lvdash.json` file
3. Place in this directory
4. Add to Asset Bundle configuration
5. Deploy via `databricks bundle deploy`

## Best Practices

- Use parameterized queries for dynamic filtering
- Include data freshness indicators
- Add contextual help text for metrics
- Optimize query performance for real-time usage