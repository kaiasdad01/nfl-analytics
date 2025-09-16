import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
from sklearn.preprocessing import StandardScaler
import joblib
from typing import Optional, Tuple, Dict
import logging

class GameOutcomeModel:
    """ 
    Predicts game outcomes
    """

    def __init__(self, model_type: str = 'random_forest'):
        """ 
        initialize the model
        Args: 
            model_type: 'random_forest' or 'logistic_regression'
        """

        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = None
        self.is_trained = False

        # Initialize the model
        if model_type == 'random_forest':
            self.model = RandomForestClassifier(
                n_estimators=100,
                max_depth=10,
                random_state=42,
                class_weight='balanced'
            )

        elif model_type == 'logistic_regression':
            self.model = LogisticRegression(
                max_iter=1000,
                random_state=42,
                class_weight='balanced'
            )

        else:
            raise ValueError(f"Invalid model type: {model_type}")
    
    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        prepare features for training

        Args:
            df: Dataframe from game_training_dataset
        
        Returns:
            Feature matrix
        """ 

        feature_cols = [
            'home_avg_yards_l3', 'home_avg_epa_l3', 'home_third_down_rate_l3', 
            'home_turnovers_l3', 'home_top_secs_l3',

            'away_avg_yards_l3', 'away_avg_epa_l3', 'away_third_down_rate_l3', 
            'away_turnovers_l3', 'away_top_secs_l3',

            'home_total_yards_wm1', 'home_total_epa_wm1', 'home_third_down_rate_wm1', 
            'home_turnovers_wm1', 'home_top_secs_wm1', 'home_rz_first_downs_wm1',

            'away_total_yards_wm1', 'away_total_epa_wm1', 'away_third_down_rate_wm1', 
            'away_turnovers_wm1', 'away_top_secs_wm1', 'away_rz_first_downs_wm1'
        ]

        if self.feature_columns is None:
            self.feature_columns = feature_cols
        
        features = df[feature_cols].copy()
        for col in features.columns:
            features[col] = pd.to_numeric(features[col], errors='coerce')
        
        for col in features.columns:
            if 'rate' in col.lower():
                features[col] = features[col].fillna(0.5)
            elif 'turnovers' in col.lower():
                features[col] = features[col].fillna(1.0)
            elif 'epa' in col.lower():
                features[col] = features[col].fillna(0)
            else:
                # Convert to float first to handle median calculation properly
                features[col] = features[col].astype('float64')
                median_val = features[col].median()
                features[col] = features[col].fillna(median_val)


        return features 
    
    def train(self, df: pd.DataFrame) -> Dict:
        """
        Train the model on game data.
          
        Args:
            df: Training data from game_training_dataset
              
        Returns:
            Dictionary with training metrics
        """
        logging.info(f"Training {self.model_type} model...")

        X = self.prepare_features(df)
        y = df['home_win'].copy()

        mask = y.notna()
        X = X[mask]
        y = y[mask]

        logging.info(f"Training on {len(X)} games")

        X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_val_scaled = self.scaler.transform(X_val)

        self.model.fit(X_train_scaled, y_train)
        self.is_trained = True

        y_pred = self.model.predict(X_val_scaled)
        y_pred_proba = self.model.predict_proba(X_val_scaled)[:, 1]

        metrics = {
            'accuracy': accuracy_score(y_val, y_pred),
            'roc_auc': roc_auc_score(y_val, y_pred_proba),
            'training_samples': len(X_train),
            'validation_samples': len(X_val)
        }
        logging.info(f"Validation Accuracy: {metrics['accuracy']:.3f}")
        logging.info(f"Validation ROC-AUC: {metrics['roc_auc']:.3f}")
        
        return metrics
        
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Make prediction on new games.

        Args:
            df: Games to predict (same format as training data)
        
        Returns:
            dataframe with predictions
        """

        if not self.is_trained:
            raise ValueError("Model must be trained before predictions can be made")
        
        X = self.prepare_features(df)
        X_scaled = self.scaler.transform(X)

        predictions = self.model.predict_proba(X_scaled)[:, 1] # Home Win Probs

        result_df = df.copy() 
        result_df['home_win_probability'] = predictions
        result_df['predicted_winner'] = np.where(predictions > 0.5,
                                              result_df['home_team'],
                                              result_df['away_team'])
        
        return result_df
    
    def get_feature_importance(self) -> pd.DataFrame:
        """ 
        Get feature importance for the model (RandomForest Only)

        Returns:
            dataframe with feature importance scores
        """

        if not self.is_trained:
            raise ValueError("Model must be trained before feature importance can be calculated")

        if self.model_type != 'random_forest':
            raise ValueError("Feature importance is only available for RandomForest models")
        
        importance_df = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': self.model.feature_importances_
        })
        importance_df = importance_df.sort_values(by='importance', ascending=False)

        return importance_df
    
    def save_model(self, path: str):
        """
        Save the model to a file
        """
        if not self.is_trained:
            raise ValueError("Model must be trained before saving")
        
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'feature_columns': self.feature_columns,
            'model_type': self.model_type
        }

        joblib.dump(model_data, path)
        logging.info(f"Model saved to {path}")
    
    def load_model(self, path: str):
        """
        Load the model from a file
        """
        model_data = joblib.load(path)
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.feature_columns = model_data['feature_columns']
        self.model_type = model_data['model_type']
        self.is_trained = True
        logging.info(f"Model loaded from {path}")