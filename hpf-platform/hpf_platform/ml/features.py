"""
特征工程 - 从 DuckDB 加载特征数据
"""
import duckdb
import pandas as pd
from pathlib import Path
from typing import Tuple


def load_features(
    duckdb_path: str, 
    table_name: str = "fct_loan_features",
    schema: str = "analytics"
) -> pd.DataFrame:
    """
    从 DuckDB 加载 ML 特征
    
    Args:
        duckdb_path: DuckDB 数据库路径
        table_name: 特征表名 (dbt gold 层表)
        schema: 数据库 schema
    
    Returns:
        特征 DataFrame
    
    Example:
        >>> df = load_features("../data/warehouse.duckdb")
        >>> df.head()
    """
    conn = duckdb.connect(duckdb_path, read_only=True)
    
    try:
        query = f"SELECT * FROM {schema}.{table_name}"
        df = conn.execute(query).df()
        print(f"✅ 加载特征: {len(df)} 行, {len(df.columns)} 列")
        return df
    finally:
        conn.close()


def prepare_training_data(
    df: pd.DataFrame, 
    target_col: str = 'is_overdue',
    test_size: float = 0.2,
    random_state: int = 42
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    """
    准备训练数据 (分离特征和目标,划分训练/测试集)
    
    Args:
        df: 特征 DataFrame
        target_col: 目标列名
        test_size: 测试集比例
        random_state: 随机种子
    
    Returns:
        (X_train, X_test, y_train, y_test)
    
    Example:
        >>> X_train, X_test, y_train, y_test = prepare_training_data(df)
    """
    from sklearn.model_selection import train_test_split
    
    # 检查目标列是否存在
    if target_col not in df.columns:
        raise ValueError(f"目标列 '{target_col}' 不存在。可用列: {list(df.columns)}")
    
    # 分离特征和目标
    X = df.drop(columns=[target_col])
    y = df[target_col]
    
    print(f"📊 特征数: {X.shape[1]}, 样本数: {X.shape[0]}")
    print(f"📊 目标分布: {y.value_counts().to_dict()}")
    
    # 划分训练/测试集
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    
    print(f"✅ 训练集: {len(X_train)} 样本, 测试集: {len(X_test)} 样本")
    
    return X_train, X_test, y_train, y_test


if __name__ == "__main__":
    import sys
    
    # 测试示例
    if len(sys.argv) > 1:
        duckdb_path = sys.argv[1]
    else:
        duckdb_path = "../../data/warehouse.duckdb"
    
    print(f"🔍 测试特征加载: {duckdb_path}")
    
    try:
        df = load_features(duckdb_path)
        print(f"\n📋 数据预览:\n{df.head()}")
        print(f"\n📋 数据类型:\n{df.dtypes}")
    except Exception as e:
        print(f"❌ 加载失败: {e}")
