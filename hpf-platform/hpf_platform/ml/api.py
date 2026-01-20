"""
违约预测 FastAPI 服务

提供完整的 REST API 用于贷款违约预测
"""

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field, validator
from typing import List, Optional
import joblib
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import sys

# 添加父目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from hpf_platform.ml.features import preprocess_features

# ==========================================
# 日志配置
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/predictions.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ==========================================
# FastAPI 应用
# ==========================================
app = FastAPI(
    title="违约预测API",
    description="住房公积金贷款违约风险预测服务",
    version="1.0.0"
)

# ==========================================
# 模型加载
# ==========================================
MODEL_PATH = Path(__file__).parent / "models" / "overdue_model_latest.pkl"
model = None

def load_model():
    """加载模型"""
    global model
    try:
        if MODEL_PATH.exists():
            model = joblib.load(MODEL_PATH)
            logger.info(f"✅ Model loaded from {MODEL_PATH}")
        else:
            logger.warning(f"⚠️  Model file not found: {MODEL_PATH}")
            model = None
    except Exception as e:
        logger.error(f"❌ Failed to load model: {str(e)}")
        model = None

# 启动时加载模型
load_model()

# ==========================================
# 数据模型
# ==========================================
class LoanApplication(BaseModel):
    """贷款申请输入"""
    age: int = Field(..., ge=18, le=70, description="年龄（18-70）")
    gender: str = Field(..., description="性别（M/F）")
    monthly_income: float = Field(..., gt=0, description="月收入（元）")
    occupation: str = Field(..., description="职业")
    city_tier: int = Field(..., ge=1, le=3, description="城市层级（1/2/3）")
    credit_score: int = Field(..., ge=300, le=850, description="信用评分（300-850）")
    loan_amount: float = Field(..., gt=0, description="贷款金额（元）")
    loan_period_months: int = Field(..., gt=0, description="贷款期限（月）")
    
    @validator('gender')
    def validate_gender(cls, v):
        if v not in ['M', 'F']:
            raise ValueError('性别必须是 M 或 F')
        return v
    
    @validator('occupation')
    def validate_occupation(cls, v):
        valid_occupations = [
            'civil_servant', 'teacher', 'doctor', 'engineer',
            'business_owner', 'freelancer', 'worker'
        ]
        if v not in valid_occupations:
            raise ValueError(f'职业必须是: {", ".join(valid_occupations)}')
        return v

class PredictionResponse(BaseModel):
    """预测结果"""
    default_probability: float = Field(..., description="违约概率（0-1）")
    risk_level: str = Field(..., description="风险等级（低/中/高）")
    decision: str = Field(..., description="审批建议（批准/拒绝/人工审核）")
    explanation: str = Field(..., description="风险分析说明")
    prediction_time: datetime = Field(..., description="预测时间")
    model_version: str = Field(..., description="模型版本")

class BatchPredictionRequest(BaseModel):
    """批量预测请求"""
    applications: List[LoanApplication]

class BatchPredictionResponse(BaseModel):
    """批量预测结果"""
    total: int
    predictions: List[PredictionResponse]
    processing_time_ms: float

# ==========================================
# 辅助函数
# ==========================================
def prepare_features(application: LoanApplication) -> pd.DataFrame:
    """准备特征（应用特征工程）"""
    try:
        # 转换为DataFrame
        data = {
            'age': [application.age],
            'gender': [application.gender],
            'monthly_income': [application.monthly_income],
            'occupation': [application.occupation],
            'city_tier': [application.city_tier],
            'credit_score': [application.credit_score],
            'loan_amount': [application.loan_amount],
            'loan_period_months': [application.loan_period_months]
        }
        df = pd.DataFrame(data)
        
        # 应用特征工程（复用features.py的逻辑）
        # 这里需要复制特征工程的核心逻辑
        
        # 1. 性别编码
        df['gender'] = df['gender'].map({'M': 0, 'F': 1}).fillna(2)
        
        # 2. 职业编码
        occupation_map = {
            'civil_servant': 0,
            'teacher': 1,
            'doctor': 2,
            'engineer': 3,
            'worker': 4,
            'business_owner': 5,
            'freelancer': 6
        }
        df['occupation'] = df['occupation'].map(occupation_map).fillna(4)
        
        # 3. DTI 比率
        df['dti_ratio'] = df['loan_amount'] / (df['monthly_income'] + 1.0)
        
        # 4. 年龄×DTI交叉
        df['age_dti_interaction'] = df['age'] * df['dti_ratio']
        
        # 5. Log收入
        df['log_income'] = np.log1p(df['monthly_income'])
        
        # 6. 新增特征（Phase 2优化）
        df['income_loan_ratio'] = df['monthly_income'] / (df['loan_amount'] + 1)
        df['credit_score_norm'] = (df['credit_score'] - 300) / 550
        df['dti_credit_risk'] = df['dti_ratio'] * (1 - df['credit_score_norm'])
        df['monthly_payment'] = df['loan_amount'] / (df['loan_period_months'] + 1)
        df['payment_income_ratio'] = df['monthly_payment'] / (df['monthly_income'] + 1)
        df['age_credit_interaction'] = df['age'] * df['credit_score_norm']
        df['occupation_risk'] = df['occupation'] / 6.0
        df['city_risk'] = (4 - df['city_tier']) / 3.0
        
        # 填充NaN
        df = df.fillna(0)
        
        return df
        
    except Exception as e:
        logger.error(f"特征准备失败: {str(e)}")
        raise

def generate_explanation(application: LoanApplication, probability: float) -> str:
    """生成风险解释"""
    reasons = []
    
    # DTI分析
    dti = application.loan_amount / (application.monthly_income * 12)
    if dti > 3:
        reasons.append(f"DTI比率过高({dti:.2f})")
    elif dti > 2:
        reasons.append(f"DTI比率偏高({dti:.2f})")
    
    # 信用分析
    if application.credit_score < 580:
        reasons.append(f"信用评分很低({application.credit_score})")
    elif application.credit_score < 650:
        reasons.append(f"信用评分偏低({application.credit_score})")
    
    # 收入分析
    if application.monthly_income < 5000:
        reasons.append("月收入较低")
    
    # 贷款期限
    if application.loan_period_months > 240:
        reasons.append("贷款期限过长")
    
    if reasons:
        return f"违约概率 {probability:.1%}。主要风险因素：" + "、".join(reasons)
    else:
        return f"违约概率 {probability:.1%}，各项指标良好，风险可控"

# ==========================================
# API 端点
# ==========================================
@app.get("/")
async def root():
    """根路径"""
    return {
        "service": "违约预测API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": {
            "health": "/health",
            "predict": "/api/v1/predict",
            "batch": "/api/v1/predict/batch",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health_check():
    """健康检查"""
    return {
        "status": "healthy" if model is not None else "degraded",
        "model_loaded": model is not None,
        "model_path": str(MODEL_PATH),
        "timestamp": datetime.now().isoformat()
    }

@app.post("/api/v1/predict", response_model=PredictionResponse)
async def predict_single(application: LoanApplication):
    """
    单次预测
    
    预测单个贷款申请的违约风险
    """
    if model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请稍后重试")
    
    try:
        start_time = datetime.now()
        
        # 准备特征
        features_df = prepare_features(application)
        
        # 预测
        probability = float(model.predict_proba(features_df)[0][1])
        
        # 风险分级和决策
        if probability < 0.3:
            risk_level = "低"
            decision = "批准"
        elif probability < 0.7:
            risk_level = "中"
            decision = "人工审核"
        else:
            risk_level = "高"
            decision = "拒绝"
        
        # 生成解释
        explanation = generate_explanation(application, probability)
        
        # 记录日志
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        logger.info(
            f"Prediction: age={application.age}, income={application.monthly_income}, "
            f"loan={application.loan_amount} -> prob={probability:.4f}, "
            f"decision={decision}, time={processing_time:.2f}ms"
        )
        
        return PredictionResponse(
            default_probability=round(probability, 4),
            risk_level=risk_level,
            decision=decision,
            explanation=explanation,
            prediction_time=datetime.now(),
            model_version="v1.0"
        )
    
    except Exception as e:
        logger.error(f"预测失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"预测失败: {str(e)}")

@app.post("/api/v1/predict/batch", response_model=BatchPredictionResponse)
async def predict_batch(request: BatchPredictionRequest):
    """
    批量预测
    
    一次预测多个贷款申请
    """
    if model is None:
        raise HTTPException(status_code=503, detail="模型未加载，请稍后重试")
    
    start_time = datetime.now()
    predictions = []
    
    try:
        for app in request.applications:
            result = await predict_single(app)
            predictions.append(result)
        
        processing_time = (datetime.now() - start_time).total_seconds() * 1000
        
        logger.info(f"批量预测完成: {len(predictions)} 条记录, 耗时 {processing_time:.2f}ms")
        
        return BatchPredictionResponse(
            total=len(predictions),
            predictions=predictions,
            processing_time_ms=round(processing_time, 2)
        )
    
    except Exception as e:
        logger.error(f"批量预测失败: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"批量预测失败: {str(e)}")

@app.post("/api/v1/model/reload")
async def reload_model():
    """
    重新加载模型
    
    用于模型更新后重新加载最新版本
    """
    try:
        load_model()
        return {
            "status": "success",
            "message": "模型重新加载成功" if model is not None else "模型加载失败",
            "model_loaded": model is not None,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"模型重新加载失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"模型重新加载失败: {str(e)}")

# ==========================================
# 启动配置
# ==========================================
if __name__ == "__main__":
    import uvicorn
    
    # 确保日志目录存在
    Path("logs").mkdir(exist_ok=True)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
