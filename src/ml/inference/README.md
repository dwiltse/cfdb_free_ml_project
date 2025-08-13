# ML Inference Pipeline

This directory contains model serving and inference utilities.

## Inference Types

### Real-time Prediction
- Single game prediction API
- Batch prediction for weekly schedules
- Model serving endpoints

### Batch Analytics
- Season simulation
- Tournament bracket prediction
- Historical model evaluation

## Deployment Options

### Databricks Model Serving
- Auto-scaling endpoints
- A/B testing capabilities
- Model performance monitoring

### Custom APIs
- FastAPI integration
- Authentication and rate limiting
- Response caching

## Usage

### Real-time Inference
```python
from cfdb_ml.inference import GamePredictor

predictor = GamePredictor(model_name="cfdb_game_outcome_v1")
prediction = predictor.predict(
    home_team="Nebraska",
    away_team="Iowa",
    game_date="2024-11-29"
)
```

### Batch Inference
```python
from cfdb_ml.inference import WeeklyPredictor

predictor = WeeklyPredictor()
predictions = predictor.predict_week(
    season=2024,
    week=12
)
```