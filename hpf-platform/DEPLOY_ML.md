# Linux 服务器部署指南 - ML 训练

## 🚀 GitHub Actions 自动构建（推荐）

### 方案优势
- ✅ 自动构建多平台镜像（amd64/arm64）
- ✅ 缓存优化，构建速度快
- ✅ 镜像托管在 GitHub Container Registry
- ✅ 服务器端直接拉取即用，无需构建

### 1. 启用 GitHub Actions

**一次性配置**：
1. 推送代码到 GitHub
2. Actions 自动触发构建（约 5-10 分钟）
3. 镜像自动发布到 `ghcr.io/你的用户名/仓库名/hpf-ml-trainer:latest`

**查看构建状态**：
```bash
# 访问 GitHub Actions 页面
https://github.com/你的用户名/仓库名/actions
```

### 2. 服务器端部署

```bash
# 克隆代码到服务器
git clone https://github.com/你的用户名/仓库名.git
cd 仓库名/hpf-platform

# 配置镜像地址
cp .env.example .env
# 编辑 .env 文件，设置 GITHUB_REPOSITORY=你的用户名/仓库名

# 登录 GHCR（首次需要）
echo $GITHUB_TOKEN | docker login ghcr.io -u 你的用户名 --password-stdin
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

### 3. 启动容器并训练（推荐方式）

```bash
# 启动容器
docker-compose -f docker-compose.ml.yml up -d

# 进入容器
docker exec -it hpf-ml-trainer bash
```

**在容器内执行完整 Pipeline**：
```bash
# Step 1: 生成 10万条模拟数据
python scripts/generate_mock_data.py

# Step 2: 运行 dbt 构建数仓
cd dbt_project
dbt deps  # 首次需要安装依赖
dbt run
cd ..

# Step 3: 训练模型（RF/CatBoost/XGBoost/LR）
python hpf_platform/ml/train.py
```

**一键执行脚本**（可选）：
```bash
# 在容器内创建一键脚本
cat > run_ml_pipeline.sh << 'EOF'
#!/bin/bash
set -e
echo "🚀 Starting ML Training Pipeline..."
python scripts/generate_mock_data.py
cd dbt_project && dbt deps && dbt run && cd ..
python hpf_platform/ml/train.py
echo "✅ Pipeline completed!"
EOF

chmod +x run_ml_pipeline.sh
./run_ml_pipeline.sh
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
