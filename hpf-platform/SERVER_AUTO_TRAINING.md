# 🤖 服务器自动训练部署指南

## 🎯 目标

在 Linux 服务器上运行自动多轮训练，持续优化模型直到达到 F1-Score 0.70

---

## 📋 准备工作

### 1. 拉取最新代码和镜像

```bash
# 服务器上
cd /your/path/hpf-intelligent-audit
git pull origin master

# 拉取最新 Docker 镜像
cd hpf-platform
docker-compose -f docker-compose.ml.yml pull
```

### 2. 启动容器

```bash
docker-compose -f docker-compose.ml.yml up -d
docker exec -it hpf-ml-trainer bash
```

---

## 🚀 方式一：自动多轮训练（推荐）

**一键启动**，自动训练直到达到目标或达到最大轮数：

```bash
# 在容器内
./scripts/auto_train.sh
```

**脚本会自动**：
1. ✅ 逐轮增加数据量（从10万开始，每轮+2万）
2. ✅ 自动生成数据 → dbt run → 训练模型
3. ✅ 检查是否达到 F1=0.70
4. ✅ 保存每轮日志到 `logs/`
5. ✅ 达到目标后自动停止

**配置参数**（编辑 `scripts/auto_train.sh`）：
```bash
TARGET_F1=0.70          # 目标分数
MAX_ROUNDS=10           # 最大轮数
DATA_INCREMENT=20000    # 每轮数据增量
INITIAL_DATA=100000     # 初始数据量
```

**预期输出**：
```
🚀 启动自动多轮训练...
📊 目标 F1-Score: 0.70
🔄 最大轮数: 10

========================================
🎯 第 1 轮训练
========================================
📊 数据量: 100000
📦 生成数据...
✅ Saved src_customers: 100000 rows
...
🤖 训练模型...
🏆 最佳模型: CatBoost (F1=0.6205)
✅ 第 1 轮完成！F1-Score: 0.6205
📈 距离目标还差: 0.0795

========================================
🎯 第 2 轮训练
========================================
📊 数据量: 120000
...
✅ 第 2 轮完成！F1-Score: 0.6534
🎉 本轮提升: +5.30%
...

🎉🎉🎉 恭喜！达到目标 F1-Score: 0.7012 >= 0.70
📊 总共训练 5 轮
```

---

## 🔄 方式二：手动多轮训练（精细控制）

### 第 1 轮：基线

```bash
python scripts/generate_mock_data.py
cd dbt_project && dbt run && cd ..
python hpf_platform/ml/train.py

# 查看结果
python hpf_platform/ml/compare_models.py
```

### 第 2 轮：增加数据

```bash
# 修改数据量
nano scripts/generate_mock_data.py
# 改为: N_CUSTOMERS = 150000

python scripts/generate_mock_data.py
cd dbt_project && dbt run && cd ..
python hpf_platform/ml/train.py

# 对比两轮
python hpf_platform/ml/compare_models.py --compare 1 2
```

### 第 3 轮：优化特征

```bash
# 修改特征工程
nano hpf_platform/ml/features.py
# 添加新的交叉特征

python hpf_platform/ml/train.py
python hpf_platform/ml/compare_models.py --trend
```

---

## 📊 监控训练进度

### 实时查看日志（另开终端）

```bash
# 查看训练日志
docker exec hpf-ml-trainer tail -f logs/round_1_train.log

# 查看所有轮次
docker exec hpf-ml-trainer python hpf_platform/ml/compare_models.py --trend
```

### 查看模型文件

```bash
# 宿主机上
ls -lh hpf-platform/hpf_platform/ml/models/

# 输出:
# overdue_model_20260119_140530_f1_0.6205.pkl
# overdue_model_20260119_143012_f1_0.6534.pkl
# overdue_model_20260119_150245_f1_0.6789.pkl
# overdue_model_latest.pkl -> 指向最佳
# training_history.json
```

---

## 🎯 优化策略路线图

### 当 F1 卡在 0.62-0.64 时
- ✅ 增加数据量到 20-30万
- ✅ 调整特征工程（添加交叉特征）

### 当 F1 卡在 0.64-0.66 时
- ✅ 尝试 XGBoost/LightGBM（Linux 可用）
- ✅ 实现 Voting Ensemble

### 当 F1 卡在 0.66-0.68 时
- ✅ Stacking（用多模型预测作为新特征）
- ✅ 贝叶斯优化（Optuna）

### 当 F1 达到 0.68+ 时
- 🎉 已达到优秀水平！
- 💡 考虑深度学习（TabNet）

---

## 🛑 后台运行（长时间训练）

```bash
# 使用 nohup 后台运行
nohup docker exec hpf-ml-trainer ./scripts/auto_train.sh > auto_train.log 2>&1 &

# 查看进度
tail -f auto_train.log

# 查看进程
ps aux | grep auto_train
```

**或使用 tmux**：
```bash
# 创建会话
tmux new -s ml-training

# 进入容器并训练
docker exec -it hpf-ml-trainer bash
./scripts/auto_train.sh

# 分离会话: Ctrl+b, 然后按 d
# 重新连接: tmux attach -t ml-training
```

---

## 📈 预期时间线

| 轮次 | 数据量 | 预计F1 | 训练时长 | 累计时长 |
|------|--------|--------|----------|----------|
| 1    | 10万   | 0.62   | 15分钟   | 15分钟   |
| 2    | 12万   | 0.64   | 18分钟   | 33分钟   |
| 3    | 14万   | 0.66   | 20分钟   | 53分钟   |
| 4    | 16万   | 0.68   | 22分钟   | 75分钟   |
| 5    | 18万   | 0.70   | 25分钟   | **100分钟** ✅ |

**预计 1.5-2 小时达到 F1=0.70**

---

## ✅ 完成后

```bash
# 查看最终结果
python hpf_platform/ml/compare_models.py

# 复制最佳模型到宿主机
exit  # 退出容器
docker cp hpf-ml-trainer:/app/hpf-platform/hpf_platform/ml/models ./final_models

# 停止容器
docker-compose -f docker-compose.ml.yml down
```

---

## 🚨 故障排查

### 训练卡住不动
```bash
# 检查容器资源
docker stats hpf-ml-trainer

# 检查磁盘空间
docker exec hpf-ml-trainer df -h
```

### 内存不足
编辑 `docker-compose.ml.yml`：
```yaml
deploy:
  resources:
    limits:
      memory: 16G  # 增加内存限制
```

### 查看详细错误
```bash
docker exec hpf-ml-trainer cat logs/round_X_train.log
```
