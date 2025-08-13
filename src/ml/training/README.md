# ML Training Pipeline

This directory contains training pipelines for college football prediction models.

## Model Types

### Game Outcome Prediction
- Binary classification (win/loss)
- Multi-class outcome prediction
- Point spread prediction

### Performance Forecasting
- Team rating systems
- Season win total prediction
- Conference championship probability

## Training Framework

### MLflow Integration
- Experiment tracking
- Model versioning
- Parameter optimization

### Databricks ML
- AutoML integration
- Feature store utilization
- Model serving deployment

## Usage

```python
from cfdb_ml.training import GameOutcomeTrainer

trainer = GameOutcomeTrainer(
    catalog="cfdb_free_dev",
    experiment_name="/Shared/CFDB-dev/game-prediction"
)

model = trainer.train(
    features_table="gold.fact_game_predictions",
    target_column="home_team_win"
)
```