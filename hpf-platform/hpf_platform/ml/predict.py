"""
预测服务 - 逾期风险预测
"""
import joblib
import pandas as pd
from pathlib import Path
from typing import Dict, Union, List


class OverduePredictor:
    """逾期风险预测器"""
    
    def __init__(self, model_path: str = None):
        """
        初始化预测器
        
        Args:
            model_path: 模型文件路径。如果为 None,使用默认路径
        """
        if model_path is None:
            model_path = Path(__file__).parent / "models" / "overdue_model.pkl"
        
        self.model_path = Path(model_path)
        
        if not self.model_path.exists():
            raise FileNotFoundError(
                f"模型文件不存在: {self.model_path}\n"
                f"请先运行训练脚本: python -m hpf_platform.ml.train"
            )
        
        self.model = joblib.load(self.model_path)
        print(f"✅ 模型已加载: {self.model_path}")
    
    def predict(self, features: Union[Dict, pd.DataFrame]) -> Dict:
        """
        预测单条或多条数据
        
        Args:
            features: 特征字典或 DataFrame
        
        Returns:
            预测结果字典,包含:
            - is_overdue: 预测结果 (0/1)
            - probability: 逾期概率
        
        Example:
            >>> predictor = OverduePredictor()
            >>> result = predictor.predict({
            ...     "loan_amount": 500000,
            ...     "loan_term_months": 240,
            ...     ...
            ... })
            >>> print(result)
        """
        # 转换为 DataFrame
        if isinstance(features, dict):
            df = pd.DataFrame([features])
        else:
            df = features
        
        # 预测
        pred = self.model.predict(df)[0]
        proba = self.model.predict_proba(df)[0]
        
        # 逾期概率 (类别 1 的概率)
        overdue_proba = proba[1] if len(proba) > 1 else proba[0]
        
        return {
            "is_overdue": int(pred),
            "probability": float(overdue_proba),
            "confidence": float(max(proba))
        }
    
    def predict_batch(self, features_list: List[Dict]) -> List[Dict]:
        """
        批量预测
        
        Args:
            features_list: 特征字典列表
        
        Returns:
            预测结果列表
        """
        df = pd.DataFrame(features_list)
        preds = self.model.predict(df)
        probas = self.model.predict_proba(df)
        
        results = []
        for pred, proba in zip(preds, probas):
            overdue_proba = proba[1] if len(proba) > 1 else proba[0]
            results.append({
                "is_overdue": int(pred),
                "probability": float(overdue_proba),
                "confidence": float(max(proba))
            })
        
        return results


# ============================================================================
# FastAPI 集成示例
# ============================================================================

"""
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()
predictor = OverduePredictor()


class PredictionRequest(BaseModel):
    loan_amount: float
    loan_term_months: int
    # ... 其他特征


@app.post("/predict")
async def predict(request: PredictionRequest):
    features = request.dict()
    result = predictor.predict(features)
    return result


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
"""


if __name__ == "__main__":
    # 测试示例
    print("🔍 测试预测服务...\n")
    
    try:
        predictor = OverduePredictor()
        
        # 示例数据
        test_features = {
            "loan_amount": 800000,
            "loan_term_months": 240,
            "issue_year": 2023,
            "issue_month": 6,
            "loan_amount_category": "high"
        }
        
        print(f"📊 输入特征: {test_features}\n")
        result = predictor.predict(test_features)
        
        print("📊 预测结果:")
        print(f"  - 是否逾期: {'是' if result['is_overdue'] else '否'}")
        print(f"  - 逾期概率: {result['probability']:.2%}")
        print(f"  - 置信度: {result['confidence']:.2%}")
        
    except Exception as e:
        print(f"❌ 预测失败: {e}")
        import traceback
        traceback.print_exc()
