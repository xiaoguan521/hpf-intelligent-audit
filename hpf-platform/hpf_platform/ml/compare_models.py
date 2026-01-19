"""
模型对比工具 - 对比不同版本模型的性能
"""
import json
import argparse
from pathlib import Path
from tabulate import tabulate


def load_training_history(models_dir="hpf_platform/ml/models"):
    """加载训练历史"""
    history_file = Path(models_dir) / "training_history.json"
    
    if not history_file.exists():
        print(f"❌ 未找到训练历史文件: {history_file}")
        return []
    
    with open(history_file, 'r', encoding='utf-8') as f:
        history = json.load(f)
    
    return history


def display_all_runs(history):
    """显示所有训练轮次"""
    if not history:
        print("📊 暂无训练记录")
        return
    
    print(f"\n📊 共有 {len(history)} 轮训练记录\n")
    
    # 准备表格数据
    table_data = []
    for i, run in enumerate(history, 1):
        table_data.append([
            i,
            run['timestamp'],
            run['model_type'],
            f"{run['f1_score']:.4f}",
            f"{run['precision']:.4f}",
            f"{run['recall']:.4f}",
            f"{run['accuracy']:.4f}",
            run['data_size']
        ])
    
    headers = ['#', '时间', '模型', 'F1', 'Precision', 'Recall', 'Accuracy', '数据量']
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    # 显示最佳模型
    best_run = max(history, key=lambda x: x['f1_score'])
    print(f"\n🏆 历史最佳:")
    print(f"   时间: {best_run['timestamp']}")
    print(f"   模型: {best_run['model_type']}")
    print(f"   F1-Score: {best_run['f1_score']:.4f}")
    print(f"   文件: {best_run['model_path']}")


def compare_two_runs(history, run1_idx, run2_idx):
    """对比两个训练轮次"""
    if run1_idx < 1 or run1_idx > len(history):
        print(f"❌ 无效的轮次编号: {run1_idx}")
        return
    
    if run2_idx < 1 or run2_idx > len(history):
        print(f"❌ 无效的轮次编号: {run2_idx}")
        return
    
    run1 = history[run1_idx - 1]
    run2 = history[run2_idx - 1]
    
    print(f"\n🔍 对比分析: 第{run1_idx}轮 vs 第{run2_idx}轮\n")
    
    # 准备对比数据
    metrics = ['f1_score', 'precision', 'recall', 'accuracy']
    table_data = []
    
    for metric in metrics:
        val1 = run1[metric]
        val2 = run2[metric]
        diff = val2 - val1
        diff_pct = (diff / val1 * 100) if val1 != 0 else 0
        
        diff_str = f"{diff:+.4f} ({diff_pct:+.2f}%)"
        if diff > 0:
            diff_str = f"🟢 {diff_str}"
        elif diff < 0:
            diff_str = f"🔴 {diff_str}"
        else:
            diff_str = "➖ 无变化"
        
        table_data.append([
            metric.replace('_', ' ').title(),
            f"{val1:.4f}",
            f"{val2:.4f}",
            diff_str
        ])
    
    headers = ['指标', f'轮次 {run1_idx}', f'轮次 {run2_idx}', '变化']
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    print(f"\n📅 时间对比:")
    print(f"   轮次 {run1_idx}: {run1['timestamp']}")
    print(f"   轮次 {run2_idx}: {run2['timestamp']}")
   
    print(f"\n🤖 模型对比:")
    print(f"   轮次 {run1_idx}: {run1['model_type']}")
    print(f"   轮次 {run2_idx}: {run2['model_type']}")
    
    print(f"\n📊 数据对比:")
    print(f"   轮次 {run1_idx}: {run1['data_size']} 样本")
    print(f"   轮次 {run2_idx}: {run2['data_size']} 样本")


def show_progress_trend(history):
    """显示性能趋势"""
    if len(history) < 2:
        print("📈 需要至少2轮训练才能显示趋势")
        return
    
    print("\n📈 F1-Score 进步趋势:\n")
    
    print("轮次  |  F1-Score  |  模型类型       |  趋势")
    print("-" * 60)
    
    for i, run in enumerate(history, 1):
        f1 = run['f1_score']
        model = run['model_type']
        
        if i == 1:
            trend = "  —  (基线)"
        else:
            prev_f1 = history[i-2]['f1_score']
            diff = f1 - prev_f1
            if diff > 0.001:
                trend = f"  ⬆️  +{diff:.4f}"
            elif diff < -0.001:
                trend = f"  ⬇️  {diff:.4f}"
            else:
                trend = "  ➡️  持平"
        
        print(f"  {i:2d}  |  {f1:.4f}    |  {model:15s} | {trend}")
    
    # 计算总提升
    first_f1 = history[0]['f1_score']
    last_f1 = history[-1]['f1_score']
    total_improvement = last_f1 - first_f1
    total_pct = (total_improvement / first_f1 * 100) if first_f1 != 0 else 0
    
    print(f"\n✨ 总体提升: {total_improvement:+.4f} ({total_pct:+.2f}%)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="模型版本对比工具")
    parser.add_argument("--models-dir", default="hpf_platform/ml/models", help="模型目录路径")
    parser.add_argument("--compare", nargs=2, type=int, metavar=('RUN1', 'RUN2'), 
                       help="对比两个训练轮次，例如: --compare 1 3")
    parser.add_argument("--trend", action="store_true", help="显示性能趋势")
    
    args = parser.parse_args()
    
    # 加载历史
    history = load_training_history(args.models_dir)
    
    if not history:
        exit(1)
    
    # 根据参数执行不同操作
    if args.compare:
        compare_two_runs(history, args.compare[0], args.compare[1])
    elif args.trend:
        show_progress_trend(history)
    else:
        # 默认显示所有轮次
        display_all_runs(history)
        
        # 如果有多轮，也显示趋势
        if len(history) > 1:
            show_progress_trend(history)
