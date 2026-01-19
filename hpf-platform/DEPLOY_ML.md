# Linux 服务器部署指南

## 🐳 Docker 部署模型训练

### 1. 准备工作

```bash
# 克隆代码到服务器
git clone <your-repo> hpf-project
cd hpf-project/hpf-platform

# 确保有 Docker 和 Docker Compose
docker --version
docker-compose --version
```

### 2. 构建镜像

```bash
# 从项目根目录构建
cd /path/to/hpf-project
docker-compose -f hpf-platform/docker-compose.ml.yml build
```

**镜像包含**：
- ✅ Python 3.9
- ✅ XGBoost (Linux 原生支持)
- ✅ LightGBM (Linux 原生支持)
- ✅ CatBoost
- ✅ dbt-core + dbt-duckdb
- ✅ 所有数据生成工具

### 3. 运行训练

#### 方式 A：一键运行完整 Pipeline
```bash
docker-compose -f hpf-platform/docker-compose.ml.yml up
```

**流程**：
1. 生成 10万条模拟数据
2. 运行 dbt 构建数仓
3. 训练 4个模型（RF/XGB/CatBoost/LR）
4. 输出最佳模型到 `./hpf_platform/ml/models/`

#### 方式 B：分步执行（推荐用于调试）
```bash
# 启动容器但不执行训练
docker-compose -f hpf-platform/docker-compose.ml.yml run --rm ml-trainer bash

# 在容器内手动执行
python scripts/generate_mock_data.py
cd dbt_project && dbt run && cd ..
python hpf_platform/ml/train.py
```

### 4. 查看结果

```bash
# 训练完成后,模型保存在宿主机
ls -lh hpf-platform/hpf_platform/ml/models/
# overdue_model.pkl

# 查看训练日志
docker-compose -f hpf-platform/docker-compose.ml.yml logs -f
```

### 5. 后台运行

```bash
# 分离模式（后台运行）
docker-compose -f hpf-platform/docker-compose.ml.yml up -d

# 查看进度
docker logs -f hpf-ml-trainer
```

---

## ⚡ 性能优势

| 环境 | XGBoost | LightGBM | 训练速度 |
|------|---------|----------|---------|
| **Mac (本地)** | ❌ 不可用 | ❌ 不可用 | 慢 |
| **Linux (Docker)** | ✅ 可用 | ✅ 可用 | **快 2-3倍** |

**预期提升**：
- F1-Score: 0.62 → **0.64-0.65** (XGBoost/LightGBM 通常更强)
- 训练时间：15分钟 → **5-8分钟** (并行优化)

---

## 🎯 Windows 用户

如果您在 Windows 上有 WSL2 + Docker Desktop：

```powershell
# 以上所有命令同样适用
wsl
cd /mnt/c/your/path/hpf-project
docker-compose -f hpf-platform/docker-compose.ml.yml up
```

---

## 🔧 自定义配置

编辑 `docker-compose.ml.yml`：

```yaml
environment:
  # 调整数据量
  - N_CUSTOMERS=200000  # 生成20万客户

# 限制资源（可选）
deploy:
  resources:
    limits:
      cpus: '8'        # 使用8核
      memory: 16G      # 最大16G内存
```
