# 模型预测使用指南

## 🎯 预测时需要的数据

### 必需字段（8个）

所有字段都是**贷款申请时就能获取的信息**：

| 字段名 | 类型 | 说明 | 示例 | 来源 |
|--------|------|------|------|------|
| `age` | int | 年龄 | 35 | 身份证 |
| `gender` | str | 性别 | "M"/"F"/"U" | 身份证 |
| `monthly_income` | float | 月收入(元) | 15000 | 收入证明 |
| `occupation` | str | 职业 | "engineer" | 申请表 |
| `city_tier` | int | 城市层级 | 1-4 | 地址 |
| `credit_score` | int | 信用评分 | 720 | 征信报告 |
| `loan_amount` | float | 贷款金额(元) | 500000 | 申请表 |
| `loan_period_months` | int | 贷款期限(月) | 240 | 申请表 |

### 职业类型枚举

```python
occupation_options = [
    "civil_servant",    # 公务员
    "teacher",          # 教师
    "doctor",           # 医生
    "engineer",         # 工程师
    "worker",           # 工人
    "business_owner",   # 企业主
    "freelancer"        # 自由职业
]
```

---

## 🚀 使用方法

### 方式1：Python 直接调用

```python
from hpf_platform.ml.predict import OverduePredictor

# 加载模型
predictor = OverduePredictor()

# 准备数据
application = {
    "age": 35,
    "gender": "M",
    "monthly_income": 15000,
    "occupation": "engineer",
    "city_tier": 1,
    "credit_score": 720,
    "loan_amount": 500000,
    "loan_period_months": 240
}

# 预测
result = predictor.predict(application)

print(f"违约概率: {result['probability']:.2%}")
# 输出: 违约概率: 23.45%
```

---

### 方式2：FastAPI 接口（推荐）

#### 启动服务

```bash
# 在容器内或本地
cd hpf-platform
python -m uvicorn hpf_platform.ml.api:app --host 0.0.0.0 --port 8000
```

#### 调用 API

```bash
curl -X POST http://localhost:8000/predict \
  -H "Content-Type: application/json" \
  -d '{
    "age": 35,
    "gender": "M",
    "monthly_income": 15000,
    "occupation": "engineer",
    "city_tier": 1,
    "credit_score": 720,
    "loan_amount": 500000,
    "loan_period_months": 240
  }'
```

**响应示例**：
```json
{
  "default_probability": 0.2345,
  "risk_level": "low",
  "recommendation": "✅ 建议批准：低风险客户",
  "feature_importance": {
    "occupation": 0.2956,
    "credit_score": 0.2537,
    "monthly_income": 0.0820,
    "dti_ratio": 0.0579,
    "age": 0.0387
  }
}
```

---

## 🔄 数据流程

```
用户申请表单（8个字段）
    ↓
特征工程（自动计算7个衍生特征）
    ↓
模型预测（15个特征）
    ↓
返回结果（概率 + 风险等级 + 建议）
```

---

## ⚠️ 重要说明

### ✅ 优点：无数据泄漏
所有特征都是**申请时刻**就能获取的，不包含：
- ❌ 未来信息（还款记录）
- ❌ 结果信息（是否逾期）
- ❌ 隐私信息（详细消费记录）

### ✅ 符合监管要求
- 所有数据都有合法来源
- 用户知情同意
- 可解释性强（特征重要性）

---

## 🎯 风险等级判定

| 违约概率 | 风险等级 | 审批建议 |
|----------|---------|---------|
| < 30% | 低风险 | ✅ 建议批准 |
| 30-60% | 中风险 | ⚠️ 人工审核 |
| > 60% | 高风险 | ❌ 建议拒绝 |

---

## 📊 批量预测示例

```python
# 批量处理多个申请
applications = [
    {"age": 35, "gender": "M", ...},
    {"age": 28, "gender": "F", ...},
    {"age": 42, "gender": "M", ...}
]

results = predictor.predict_batch(applications)

for i, result in enumerate(results):
    print(f"申请 {i+1}: 违约概率 {result['probability']:.2%}")
```

---

## 🔧 前端集成示例

```javascript
// 前端表单提交
async function checkLoanRisk() {
    const formData = {
        age: parseInt(document.getElementById('age').value),
        gender: document.getElementById('gender').value,
        monthly_income: parseFloat(document.getElementById('income').value),
        occupation: document.getElementById('occupation').value,
        city_tier: parseInt(document.getElementById('city').value),
        credit_score: parseInt(document.getElementById('credit').value),
        loan_amount: parseFloat(document.getElementById('loan_amount').value),
        loan_period_months: parseInt(document.getElementById('period').value)
    };
    
    const response = await fetch('/api/predict', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify(formData)
    });
    
    const result = await response.json();
    
    // 显示结果
    document.getElementById('risk-probability').textContent = 
        `${(result.default_probability * 100).toFixed(2)}%`;
    document.getElementById('recommendation').textContent = 
        result.recommendation;
}
```

---

## 📌 常见问题

### Q: 如果某些字段缺失怎么办？
A: 所有8个字段都是必需的。如果缺失，建议：
- 使用默认值（如平均值）
- 要求用户补充
- 使用更简单的规则模型

### Q: 能否只提供部分字段？
A: 不行。模型训练时使用了所有15个特征（8个原始+7个衍生），预测时必须完全一致。

### Q: 特征顺序重要吗？
A: 在 `predict.py` 中已经自动处理了顺序，用户提供字典即可。

### Q: 如何更新模型？
A: 重新训练后，替换 `models/overdue_model_latest.pkl` 即可，无需修改代码。
