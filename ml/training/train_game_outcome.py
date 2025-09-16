"""
  Training script for Game Outcome Model

  This script:
  1. Loads data from BigQuery using your dbt model
  2. Trains the game outcome model
  3. Evaluates performance
  4. Saves the trained model
  """

import pandas as pd
import logging
from google.cloud import bigquery
from ml.models.game_outcomes import GameOutcomeModel

  # Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_training_data() -> pd.DataFrame:
      """
      Load training data from BigQuery using your dbt model.
      """
      client = bigquery.Client()

      # Query your training dataset
      query = """
      SELECT *
      FROM `nfl-analytics-472221.nfl_data.game_training_dataset`
      WHERE season BETWEEN 2020 AND 2023
      """

      logger.info("Loading training data from BigQuery...")
      df = client.query(query).to_dataframe()
      logger.info(f"Loaded {len(df)} training samples")

      return df

def main():
      """Main training pipeline."""

      # Load data
      df = load_training_data()

      # Check data quality
      logger.info(f"Data shape: {df.shape}")
      logger.info(f"Missing target values: {df['home_win'].isna().sum()}")
      logger.info(f"Home team win rate: {df['home_win'].mean():.3f}")

      # Train Random Forest model
      logger.info("\n=== Training Random Forest Model ===")
      rf_model = GameOutcomeModel(model_type='random_forest')
      rf_metrics = rf_model.train(df)

      # Show feature importance
      importance = rf_model.get_feature_importance()
      logger.info("\nTop 10 Most Important Features:")
      logger.info(importance.head(10).to_string(index=False))

      # Save model
      rf_model.save_model('models/game_outcome_rf.pkl')

      # Train Logistic Regression model for comparison
      logger.info("\n=== Training Logistic Regression Model ===")
      lr_model = GameOutcomeModel(model_type='logistic_regression')
      lr_metrics = lr_model.train(df)
      lr_model.save_model('models/game_outcome_lr.pkl')

      # Compare models
      logger.info("\n=== Model Comparison ===")
      logger.info(f"Random Forest - Accuracy: {rf_metrics['accuracy']:.3f}, ROC-AUC: {rf_metrics['roc_auc']:.3f}")
      logger.info(f"Logistic Regression - Accuracy: {lr_metrics['accuracy']:.3f}, ROC-AUC: {lr_metrics['roc_auc']:.3f}")

      # Test prediction on a sample
      logger.info("\n=== Sample Predictions ===")
      sample_games = df.head(5)
      predictions = rf_model.predict(sample_games)

      for _, game in predictions.iterrows():
          logger.info(f"{game['away_team']} @ {game['home_team']}: "
                     f"{game['predicted_winner']} ({game['home_win_probability']:.3f})")

if __name__ == "__main__":
      main()