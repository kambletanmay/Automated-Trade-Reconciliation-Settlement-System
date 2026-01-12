from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
import numpy as np
import pickle
from typing import List, Dict
import pandas as pd

class MLMatchingEnhancer:
    """Use ML to improve matching accuracy and learn from resolved breaks"""
    
    def __init__(self):
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.scaler = StandardScaler()
        self.is_trained = False
        
    def extract_features(self, internal: Dict, external: Dict) -> np.array:
        """Extract features for ML model"""
        features = [
            # Price features
            abs(internal['price'] - external['price']),
            abs(internal['price'] - external['price']) / internal['price'],
            
            # Quantity features
            abs(internal['quantity'] - external['quantity']),
            abs(internal['quantity'] - external['quantity']) / internal['quantity'],
            
            # Time features
            abs((internal['trade_date'] - external['trade_date']).total_seconds() / 3600),
            
            # String similarity
            fuzz.ratio(str(internal['counterparty']), str(external['counterparty'])) / 100,
            fuzz.ratio(str(internal['instrument_name'] or ''), str(external['instrument_name'] or '')) / 100,
            
            # Exact matches (binary)
            int(internal['instrument_id'] == external['instrument_id']),
            int(internal['currency'] == external['currency']),
            int(internal['account'] == external.get('account', '')),
            
            # Derived features
            internal['price'] * internal['quantity'],  # Notional
            abs((internal['price'] * internal['quantity']) - 
                (external['price'] * external['quantity'])),  # Notional diff
        ]
        
        return np.array(features).reshape(1, -1)
    
    def train(self, training_data: List[Dict]):
        """
        Train model on historical matches
        training_data format: [{'internal': {...}, 'external': {...}, 'is_match': True/False}]
        """
        X = []
        y = []
        
        for example in training_data:
            features = self.extract_features(example['internal'], example['external'])
            X.append(features[0])
            y.append(1 if example['is_match'] else 0)
        
        X = np.array(X)
        y = np.array(y)
        
        # Scale features
        X_scaled = self.scaler.fit_transform(X)
        
        # Train model
        self.model.fit(X_scaled, y)
        self.is_trained = True
        
        # Print feature importance
        feature_names = ['price_diff', 'price_diff_pct', 'qty_diff', 'qty_diff_pct',
                        'time_diff', 'counterparty_sim', 'instrument_sim', 
                        'instrument_match', 'currency_match', 'account_match',
                        'notional', 'notional_diff']
        
        importances = self.model.feature_importances_
        for name, importance in sorted(zip(feature_names, importances), 
                                      key=lambda x: x[1], reverse=True):
            print(f"{name}: {importance:.4f}")
    
    def predict_match_probability(self, internal: Dict, external: Dict) -> float:
        """Predict probability that two trades match"""
        if not self.is_trained:
            raise ValueError("Model not trained yet")
        
        features = self.extract_features(internal, external)
        features_scaled = self.scaler.transform(features)
        
        # Return probability of positive class
        prob = self.model.predict_proba(features_scaled)[0][1]
        return prob
    
    def save_model(self, filepath: str):
        """Save trained model"""
        with open(filepath, 'wb') as f:
            pickle.dump({
                'model': self.model,
                'scaler': self.scaler,
                'is_trained': self.is_trained
            }, f)
    
    def load_model(self, filepath: str):
        """Load trained model"""
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
            self.model = data['model']
            self.scaler = data['scaler']
            self.is_trained = data['is_trained']
