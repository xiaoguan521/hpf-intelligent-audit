"""
模型训练 - 训练逾期风险预测模型
"""
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import joblib
from pathlib import Path
from .features import load_features, prepare_training_data


def train_model(
    duckdb_path: str,
    target_col: str = "is_overdue",
    model_output_path: str = None,
    n_estimators: int = 100,
    random_state: int = 42
):
    """
    训练逾期风险预测模型
    
    Args:
        duckdb_path: DuckDB 数据库路径
        target_col: 目标列名
        model_output_path: 模型保存路径
        n_estimators: 随机森林树的数量
        random_state: 随机种子
    
    Returns:
        训练好的模型
    
    Example:
        >>> model = train_model("../data/warehouse.duckdb")
    """
    print("=" * 70)
    print("🚀 开始模型训练")
    print("=" * 70)
    
    # 1. 加载特征数据
    print("\n📊 Step 1: 加载特征数据...")
    df = load_features(duckdb_path)
    
    # 2. 准备训练数据
    print("\n🔧 Step 2: 准备训练数据...")
    X_train, X_test, y_train, y_test = prepare_training_data(df, target_col=target_col)
    
    # 3. 训练模型
    print("\n🚀 Step 3: 训练模型...")
    model = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1
    )
    model.fit(X_train, y_train)
    print("✅ 模型训练完成")
    
    # 4. 评估模型
    print("\n✅ Step 4: 评估模型...")
    y_pred = model.predict(X_test)
    
    print("\n" + "=" * 70)
    print("📊 模型评估报告")
    print("=" * 70)
    print(classification_report(y_test, y_pred))
    
    print("\n混淆矩阵:")
    print(confusion_matrix(y_test, y_pred))
    
    # 5. 特征重要性
    print("\n" + "=" * 70)
    print("📊 Top 10 特征重要性")
    print("=" * 70)
    feature_importance = sorted(
        zip(X_train.columns, model.feature_importances_),
        key=lambda x: x[1],
        reverse=True
    )[:10]
    
    for i, (feat, importance) in enumerate(feature_importance, 1):
        print(f"{i:2d}. {feat:30s} {importance:.4f}")
    
    # 6. 保存模型
    if model_output_path is None:
        model_output_path = Path(__file__).parent / "models" / "overdue_model.pkl"
    
    model_output_path = Path(model_output_path)
    model_output_path.parent.mkdir(parents=True, exist_ok=True)
    
    joblib.dump(model, model_output_path)
    print(f"\n💾 模型已保存: {model_output_path}")
    
    return model


if __name__ == "__main__":
    import sys
    
    duckdb_path = sys.argv[1] if len(sys.argv) > 1 else "../../data/warehouse.duckdb"
    
    try:
        train_model(duckdb_path)
    except Exception as e:
        print(f"\n❌ 训练失败: {e}")
        import traceback
        traceback.print_exc()
