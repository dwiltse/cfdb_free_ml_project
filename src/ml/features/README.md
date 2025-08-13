# ML Feature Engineering

This directory contains feature engineering utilities for college football ML models.

## Planned Features

### Team Performance Features
- EPA (Expected Points Added) metrics
- Offensive/Defensive efficiency ratings
- Recent performance trends
- Strength of schedule adjustments

### Game Context Features
- Home field advantage
- Weather conditions (future)
- Betting line integration (future)
- Rivalry game indicators

### Temporal Features
- Season progression effects
- Rest days between games
- Conference championship implications

## Usage

```python
from cfdb_ml.features import GameFeatureExtractor

extractor = GameFeatureExtractor(catalog="cfdb_free_dev")
features = extractor.extract_game_features(
    home_team="Nebraska", 
    away_team="Iowa", 
    season=2024
)
```