import sqlite3
import pandas as pd
import numpy as np
import pickle
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from features import FeatureExtractor

class ModelTrainer:
    def __init__(self, db_path: str = None, model_dir: str = None):
        self.db_path = db_path or os.getenv('DB_PATH', 'housing_provident_fund.db')
        self.model_dir = model_dir or os.getenv('ML_MODEL_DIR', 'ml_engine')
        # 确保模型目录存在
        os.makedirs(self.model_dir, exist_ok=True)
        self.feature_extractor = FeatureExtractor(self.db_path)
        
    def discover_risk_types(self):
        """Dynamic Discovery: Find all distinct risk types reported by Skills"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            # 获取所有已存在的风险类型
            cursor.execute("SELECT DISTINCT FXLB FROM FX_SJ_JL WHERE FXLB IS NOT NULL")
            types = [row[0] for row in cursor.fetchall()]
            conn.close()
            return types
        except Exception as e:
            print(f"Error discovering risk types: {e}")
            return []

    def get_labels_for_risk_type(self, features_df, risk_type):
        """
        Generate labels (1/0) for a specific risk type.
        1 = User has a record in FX_SJ_JL for this risk_type.
        0 = User does not.
        """
        conn = sqlite3.connect(self.db_path)
        # 找到所有打上该标签的人 (ZTID)
        sql = f"SELECT DISTINCT ZTID FROM FX_SJ_JL WHERE FXLB = '{risk_type}'"
        positives = set(pd.read_sql_query(sql, conn)['ZTID'])
        conn.close()
        
        # DataFrame 的索引假设是 'GRZH' (Person ID)
        # 如果不是，我们需要在 FeatureExtractor 里把 ID 带出来
        labels = features_df['GRZH'].apply(lambda x: 1 if x in positives else 0)
        return labels

    def train_all(self):
        """Main training loop."""
        
        # 1. Load Features (Context) for ALL active users
        print("Extracting features...")
        raw_features = self.feature_extractor.load_data()
        if not raw_features:
            print("No features extracted. Aborting.")
            return

        df = pd.DataFrame(raw_features)
        
        # Feature Columns (Exclude IDs and raw DB fields)
        feat_cols = [c for c in df.columns if c.startswith('feat_')]
        print(f"Features used: {feat_cols}")
        
        # 2. Dynamic Discovery
        risk_types = self.discover_risk_types()
        print(f"Discovered Risk Types: {risk_types}")
        
        if not risk_types:
            print("No risk types found in DB. Nothing to train.")
            return

        # 3. Train a model for EACH risk type
        for r_type in risk_types:
            print(f"\n--- Training Model for: {r_type} ---")
            
            # Labeling
            y = self.get_labels_for_risk_type(df, r_type)
            X = df[feat_cols]
            
            # Check balance
            pos_count = sum(y)
            if pos_count < 5:
                print(f"⚠️ Skipped: Too few positive samples ({pos_count}) for {r_type}")
                continue
                
            # Train/Test Split
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
            
            # Model (Random Forest is more robust for tabular data)
            model = RandomForestClassifier(n_estimators=100, class_weight='balanced', random_state=42)
            model.fit(X_train, y_train)
            
            # Evaluate
            preds = model.predict(X_test)
            acc = accuracy_score(y_test, preds)
            print(f"Active Features: {sum(y)}")
            print(f"Accuracy: {acc:.2f}")
            
            # Feature Importance
            importance = dict(zip(feat_cols, model.feature_importances_))
            print(f"Feature Importance: {importance}")
            
            # Save
            filename = os.path.join(self.model_dir, f"model_{r_type}.pkl")
            with open(filename, 'wb') as f:
                pickle.dump(model, f)
            print(f"✅ Saved model to {filename}")

if __name__ == "__main__":
    trainer = ModelTrainer(model_dir='ml_engine')
    trainer.train_all()
