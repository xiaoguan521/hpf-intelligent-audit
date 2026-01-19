"""
模型训练 - 训练逾期风险预测模型
"""
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import joblib
import os
import sys
from pathlib import Path
# Fix import for script execution vs module execution
try:
    from .features import load_features, prepare_training_data
except ImportError:
    try:
        from features import load_features, prepare_training_data
    except ImportError:
        # Fallback for when running from root
        sys.path.append(os.path.dirname(__file__))
        from features import load_features, prepare_training_data


def train_model(
    duckdb_path: str,
    target_col: str = "target_label",
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
    
    # 3. 训练模型 + 超参数调优
    print("\n🚀 Step 3: 开始多模型训练与超参数调优...")
    
    from sklearn.ensemble import VotingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.model_selection import GridSearchCV
    from catboost import CatBoostClassifier
    
    # Try to import XGBoost (available on Windows/Linux with proper setup)
    try:
        import xgboost as xgb
        XGBOOST_AVAILABLE = True
    except (ImportError, OSError) as e:
        print("⚠️  XGBoost not available (likely missing libomp). Skipping XGBoost...")
        XGBOOST_AVAILABLE = False
    
    # Define models with hyperparameter grids
    models_with_params = {
        "RandomForest": {
            "model": RandomForestClassifier(random_state=random_state, n_jobs=-1),
            "params": {
                'n_estimators': [50, 100, 200],
                'max_depth': [10, 20, None],
                'class_weight': ['balanced']
            }
        },
        "CatBoost": {
            "model": CatBoostClassifier(random_state=random_state, verbose=0, thread_count=-1),
            "params": {
                'iterations': [50, 100, 200],
                'depth': [4, 6, 8],
                'learning_rate': [0.01, 0.05, 0.1],
                'auto_class_weights': ['Balanced']
            }
        },
        "LogisticRegression": {
            "model": LogisticRegression(random_state=random_state, max_iter=2000),
            "params": {
                'C': [0.1, 1, 10],
                'class_weight': ['balanced']
            }
        }
    }
    
    # Add XGBoost if available
    if XGBOOST_AVAILABLE:
        models_with_params["XGBoost"] = {
            "model": xgb.XGBClassifier(random_state=random_state, n_jobs=-1, eval_metric='logloss'),
            "params": {
                'n_estimators': [50, 100, 200],
                'max_depth': [3, 5, 7],
                'learning_rate': [0.01, 0.05, 0.1],
                'scale_pos_weight': [1, 5]  # Handle imbalance
            }
        }
    
    best_name = None
    best_score = -1
    best_model = None
    tuned_models = {}
    
    for name, config in models_with_params.items():
        print(f"\n🔍 Training {name} with GridSearchCV...")
        
        grid_search = GridSearchCV(
            config['model'],
            config['params'],
            cv=3,
            scoring='f1',
            n_jobs=-1,
            verbose=0
        )
        
        grid_search.fit(X_train, y_train)
        
        y_pred = grid_search.predict(X_test)
        
        # Calculate F1 for minority class
        report = classification_report(y_test, y_pred, output_dict=True)
        f1 = report['1']['f1-score']
        
        print(f"  ✅ {name} Best F1-Score: {f1:.4f}")
        print(f"     Best Params: {grid_search.best_params_}")
        
        tuned_models[name] = grid_search.best_estimator_
        
        if f1 > best_score:
            best_score = f1
            best_name = name
            best_model = grid_search.best_estimator_
    
    print(f"\n🏆 最佳单模型: {best_name} (F1={best_score:.4f})")
    
    # 4. Ensemble: Voting Classifier
    print("\n🤝 Step 4: 尝试集成模型 (Voting)...")
    
    voting_clf = VotingClassifier(
        estimators=[(name, model) for name, model in tuned_models.items()],
        voting='soft'
    )
    voting_clf.fit(X_train, y_train)
    y_pred_voting = voting_clf.predict(X_test)
    
    report_voting = classification_report(y_test, y_pred_voting, output_dict=True)
    f1_voting = report_voting['1']['f1-score']
    
    print(f"  ✅ Voting Ensemble F1-Score: {f1_voting:.4f}")
    
    # Choose final model
    if f1_voting > best_score:
        print(f"\n🎖️ 集成模型胜出！(F1={f1_voting:.4f} > {best_score:.4f})")
        model = voting_clf
        best_name = "VotingEnsemble"
        y_pred = y_pred_voting
    else:
        print(f"\n⭐ {best_name} 单模型更优")
        model = best_model
        y_pred = model.predict(X_test)
    
    print("\n" + "=" * 70)
    print("📊 模型评估报告")
    print("=" * 70)
    print(classification_report(y_test, y_pred))
    
    print("\n混淆矩阵:")
    print(confusion_matrix(y_test, y_pred))
    
    # 5. 特征重要性 (适应不同模型)
    print("\n" + "=" * 70)
    print(f"📊 {best_name} 特征重要性 / 系数")
    print("=" * 70)
    
    feature_importance = []
    
    if hasattr(model, 'feature_importances_'):
        importances = model.feature_importances_
        feature_importance = sorted(zip(X_train.columns, importances), key=lambda x: x[1], reverse=True)[:10]
    elif hasattr(model, 'coef_'):
        # For Logistic Regression
        importances = model.coef_[0]
        feature_importance = sorted(zip(X_train.columns, importances), key=lambda x: abs(x[1]), reverse=True)[:10]
        
    for i, (feat, imp) in enumerate(feature_importance, 1):
        print(f"{i:2d}. {feat:30s} {imp:.4f}")
    
    # 6. 保存模型
    if model_output_path is None:
        model_output_path = Path(__file__).parent / "models" / "overdue_model.pkl"
    
    model_output_path = Path(model_output_path)
    model_output_path.parent.mkdir(parents=True, exist_ok=True)
    
    joblib.dump(model, model_output_path)
    print(f"\n💾 模型已保存: {model_output_path}")
    
    return model


# Fix import for script execution
try:
    from .features import load_features, prepare_training_data
except ImportError:
    from features import load_features, prepare_training_data

if __name__ == "__main__":
    import sys
    
    # 自动推断 DuckDB 路径 (hpf-platform/data/warehouse.duckdb)
    # train.py 位于 hpf_platform/ml/train.py
    # data 位于 ../../data/
    default_db_path = Path(__file__).resolve().parent.parent.parent / "data" / "warehouse.duckdb"
    
    duckdb_path = sys.argv[1] if len(sys.argv) > 1 else str(default_db_path)
    
    if not os.path.exists(duckdb_path):
        print(f"⚠️ Warning: Database not found at {duckdb_path}")
        # Try Current directory as fallback
        duckdb_path = "data/warehouse.duckdb"
    
    print(f"📂 Using Database: {duckdb_path}")

    try:
        train_model(duckdb_path)
    except Exception as e:
        print(f"\n❌ 训练失败: {e}")
        import traceback
        traceback.print_exc()
