import os
import pickle
import glob
from typing import Dict, Any, List

class RiskModel:
    """
    Dynamic Multi-Model Inference Engine.
    Automatically loads all 'model_*.pkl' files and runs predictions.
    """
    
    def __init__(self, model_dir: str = None):
        self.model_dir = model_dir or os.getenv('ML_MODEL_DIR', 'ml_engine')
        self.models = {}
        self.load_models()

    def load_models(self):
        """Scan directory and load all model pickles."""
        # 确保目录存在
        if not os.path.exists(self.model_dir):
            print(f"⚠️  Model directory {self.model_dir} does not exist. Creating...")
            os.makedirs(self.model_dir, exist_ok=True)
        
        pattern = os.path.join(self.model_dir, "model_*.pkl")
        files = glob.glob(pattern)
        
        print(f"Scanning for models in {pattern}...")
        for f_path in files:
            try:
                # Filename format: model_{RISK_TYPE}.pkl
                basename = os.path.basename(f_path)
                risk_type = basename.replace("model_", "").replace(".pkl", "")
                
                with open(f_path, 'rb') as f:
                    self.models[risk_type] = pickle.load(f)
                
                print(f"✅ Loaded model for: {risk_type}")
            except Exception as e:
                print(f"❌ Failed to load {f_path}: {e}")
        
        if not self.models:
            print("⚠️  No trained models found. Falling back to rule-based logic (Legacy).")

    def predict_all(self, features_df) -> List[Dict[str, float]]:
        """
        Run inference for ALL loaded models.
        Returns a dict of {risk_type: probability} for each input row.
        """
        results = []
        
        # Prepare Feature Columns (Must match training)
        # We assume the dataframe passed here has the same columns as training
        feat_cols = [c for c in features_df.columns if c.startswith('feat_')]
        
        if not self.models:
            # Fallback: Use legacy rule-based for demo if no models exist
            for _, row in features_df.iterrows():
                results.append({"legacy_rule": self._legacy_predict(row)})
            return results

        # Run each model
        # We initialize all results to 0
        n_samples = len(features_df)
        all_probs = {k: [0.0]*n_samples for k in self.models.keys()}
        
        for risk_type, model in self.models.items():
            try:
                # predict_proba returns [prob_class_0, prob_class_1]
                probs = model.predict_proba(features_df[feat_cols])[:, 1]
                all_probs[risk_type] = probs
            except Exception as e:
                print(f"Inference error for {risk_type}: {e}")
        
        # Reformat to list of dicts
        for i in range(n_samples):
            row_res = {}
            for risk_type in self.models.keys():
                prob = all_probs[risk_type][i]
                if prob > 0.5: # Only report significant risks
                    row_res[risk_type] = round(prob * 100, 1)
            results.append(row_res)
            
        return results

    def _legacy_predict(self, features: Dict[str, Any]) -> float:
        """Original hardcoded rule (Fallback)"""
        score = 0.0
        if features.get('feat_deposit_abnormal', 0) == 1: score += 40
        if features.get('feat_yq_count', 0) > 0: score += 30
        cov = features.get('feat_balance_coverage', 999)
        if cov < 1.0: score += 25
        elif cov < 3.0: score += 15
        
        return min(max(score, 0), 100)
