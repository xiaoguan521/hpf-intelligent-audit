import sqlite3
import sys
import os
import pandas as pd

# Ensure we can import local modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ml_engine.features import FeatureExtractor
from ml_engine.model import RiskModel

# 从环境变量获取数据库路径
DB_PATH = os.getenv('DB_PATH', './housing_provident_fund.db')

def run_prediction_pipeline():
    print("🚀 Starting Risk Prediction ML Pipeline...")
    
    # 1. Initialize (自动从环境变量读取配置)
    extractor = FeatureExtractor()  # 会自动使用 DB_PATH 环境变量
    model = RiskModel()  # 会自动使用 ML_MODEL_DIR 环境变量
    
    # 2. Extract Features
    print("📊 Extracting behavior features from DB...")
    data_list = extractor.load_data()
    print(f"   -> Loaded {len(data_list)} active loan profiles.")
    
    if not data_list:
        return

    # Convert to DataFrame for easier handling
    df = pd.DataFrame(data_list)

    # 3. Predict & Filter (Inference)
    print("🔮 Running Dynamic Inference on all loaded models...")
    predictions = model.predict_all(df) # Returns list of dicts: [{"risk_a": 85.0}, ...]
    
    high_risk_cases = []
    
    # Iterate through results
    for idx, probs in enumerate(predictions):
        person_id = df.iloc[idx]['GRZH']
        loan_id = df.iloc[idx]['DKZH']
        
        # probs is like {"malicious_arrears": 85.0, "high_dti": 60.5}
        for risk_type, score in probs.items():
            # Threshold Check (Global default for now 50%, specific models might differ)
            if score >= 50:
                 high_risk_cases.append({
                    "ZTID": person_id,
                    "ZTLX": "Individual",
                    "FXLB": risk_type, # Dynamic Risk Type
                    "FXFZ": int(score),
                    "YJZY": f"ML Model Predicted: High probability ({score}%) of {risk_type}",
                    "BGLJ": f"loan:{loan_id}"
                })
            
    print(f"⚠️  Identified {len(high_risk_cases)} risk predictions.")
    
    # 4. Write Back to DB
    if high_risk_cases:
        print("💾 Writing predictions to FX_SJ_JL...")
        save_predictions(high_risk_cases)
        print("✅ Done.")
    else:
        print("✨ No high-risk cases found.")

def save_predictions(cases):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    sql = """
    INSERT INTO FX_SJ_JL (ZTID, ZTLX, FXLB, FXFZ, YJZY, CLZT, BGLJ)
    VALUES (?, ?, ?, ?, ?, 'Pending', ?)
    """
    
    count = 0
    for case in cases:
        # Deduplication check: Don't insert if same subject+type+result exists in Pending
        check_sql = "SELECT ID FROM FX_SJ_JL WHERE ZTID=? AND FXLB=? AND CLZT='Pending'"
        cursor.execute(check_sql, (case['ZTID'], case['FXLB']))
        if cursor.fetchone():
            print(f"   -> Skipping duplicate for {case['ZTID']}")
            continue
            
        cursor.execute(sql, (
            case['ZTID'], 
            case['ZTLX'], 
            case['FXLB'], 
            case['FXFZ'], 
            case['YJZY'], 
            case['BGLJ']
        ))
        count += 1
        
    conn.commit()
    conn.close()
    print(f"   -> Inserted {count} new records.")

if __name__ == "__main__":
    run_prediction_pipeline()
