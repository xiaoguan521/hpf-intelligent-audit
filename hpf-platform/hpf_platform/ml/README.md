# ML Pipeline

机器学习预测模块,基于 dbt Gold 层宽表进行模型训练和预测。

## 模块结构

```
hpf_platform/ml/
├── __init__.py
├── features.py         # 特征工程
├── train.py            # 模型训练
├── predict.py          # 预测服务
└── models/             # 保存的模型文件
```

## 数据流

```
dbt Gold 层 (fct_loan_features)
        ↓
features.py (加载特征)
        ↓
train.py (训练模型)
        ↓
models/overdue_model.pkl
        ↓
predict.py (预测服务)
```

## 使用方法

### 1. 训练模型

```bash
# 从项目根目录运行
python3 -m hpf_platform.ml.train data/warehouse.duckdb

# 或直接运行
cd hpf_platform/ml
python3 train.py ../../data/warehouse.duckdb
```

### 2. 使用预测服务

```python
from hpf_platform.ml.predict import OverduePredictor

predictor = OverduePredictor()

result = predictor.predict({
    "loan_amount": 500000,
    "loan_term_months": 240,
    "issue_year": 2023,
    "issue_month": 6,
    "loan_amount_category": "medium"
})

print(result)
# {'is_overdue': 0, 'probability': 0.15, 'confidence': 0.85}
```

### 3. 集成到 FastAPI

参见 `predict.py` 中的示例代码。

## 特征说明

模型依赖 dbt Gold 层的 `fct_loan_features` 表,需包含以下特征:
- `loan_amount`: 贷款金额
- `loan_term_months`: 贷款期限(月)
- `issue_year`: 发放年份
- `issue_month`: 发放月份
- `loan_amount_category`: 贷款金额分类

目标变量:
- `is_overdue`: 是否逾期 (0/1)

## 模型配置

- **算法**: RandomForestClassifier
- **参数**: n_estimators=100
- **评估指标**: Precision, Recall, F1-Score

## 依赖

```bash
pip install scikit-learn joblib pandas duckdb
```
