#!/bin/bash
# 自动多轮训练脚本 - 持续优化模型直到达到目标

set -e  # 遇到错误立即退出

# 配置
TARGET_F1=0.70          # 目标 F1-Score
MAX_ROUNDS=10           # 最大训练轮数
DATA_INCREMENT=20000    # 每轮增加的数据量
INITIAL_DATA=100000     # 初始数据量

echo "🚀 启动自动多轮训练..."
echo "📊 目标 F1-Score: $TARGET_F1"
echo "🔄 最大轮数: $MAX_ROUNDS"
echo ""

# 确保在正确的目录
cd /app/hpf-platform

# 创建日志目录
mkdir -p logs

# 训练循环
for round in $(seq 1 $MAX_ROUNDS); do
    echo "========================================"
    echo "🎯 第 $round 轮训练"
    echo "========================================"
    
    # 计算本轮数据量
    CURRENT_DATA=$((INITIAL_DATA + (round - 1) * DATA_INCREMENT))
    echo "📊 数据量: $CURRENT_DATA"
    
    # 修改数据生成脚本的数据量
    sed -i "s/N_CUSTOMERS = [0-9]*/N_CUSTOMERS = $CURRENT_DATA/" scripts/generate_mock_data.py
    
    # 生成数据
    echo "📦 生成数据..."
    python scripts/generate_mock_data.py | tee logs/round_${round}_data.log
    
    # 运行 dbt
    echo "🏗️  构建数据仓库..."
    cd dbt_project
    dbt run | tee ../logs/round_${round}_dbt.log
    cd ..
    
    # 训练模型
    echo "🤖 训练模型..."
    python hpf_platform/ml/train.py | tee logs/round_${round}_train.log
    
    # 提取本轮 F1-Score
    CURRENT_F1=$(python -c "
import json
with open('hpf_platform/ml/models/training_history.json') as f:
    history = json.load(f)
print(history[-1]['f1_score'])
")
    
    echo ""
    echo "✅ 第 $round 轮完成！F1-Score: $CURRENT_F1"
    
    # 检查是否达到目标
    REACHED=$(python -c "print('yes' if $CURRENT_F1 >= $TARGET_F1 else 'no')")
    
    if [ "$REACHED" = "yes" ]; then
        echo ""
        echo "🎉🎉🎉 恭喜！达到目标 F1-Score: $CURRENT_F1 >= $TARGET_F1"
        echo "📊 总共训练 $round 轮"
        break
    fi
    
    # 如果还没达到目标，显示进度
    echo "📈 距离目标还差: $(python -c "print(f'{$TARGET_F1 - $CURRENT_F1:.4f}')")"
    
    # 检查是否是最后一轮
    if [ $round -eq $MAX_ROUNDS ]; then
        echo ""
        echo "⚠️  已达到最大轮数 ($MAX_ROUNDS)，但未达到目标"
        echo "📊 最佳 F1-Score: $CURRENT_F1"
        echo "💡 建议: 调整特征工程或尝试不同的模型架构"
    else
        # 等待一会儿再开始下一轮
        echo ""
        echo "⏱️  等待 5 秒后开始下一轮..."
        sleep 5
    fi
    
    echo ""
done

echo ""
echo "========================================"
echo "📊 训练总结"
echo "========================================"

# 显示所有轮次的对比
python hpf_platform/ml/compare_models.py --trend

echo ""
echo "✅ 自动多轮训练完成！"
echo "📁 日志保存在: logs/"
echo "🤖 模型保存在: hpf_platform/ml/models/"
