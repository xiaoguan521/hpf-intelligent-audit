"""
特征工程 - 从 DuckDB 加载特征数据
"""
import duckdb
import pandas as pd
import numpy as np
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
        
        # --- Preprocessing ---
        print("🧹 Preprocessing features...")
        
        # 1. Drop IDs and Dates (non-features)
        drop_cols = ['contract_id', 'loan_start_date']
        
        # 2. Drop Leakage (loan_status implies target)
        # 关键修正：必须移除所有"未来"或"结果性"指标，只保留"申请时"特征
        # overdue_count 和 total_repayment_periods 都是原来用来定义 Label 的，不能做 Feature
        leakage_cols = ['loan_status', 'overdue_count', 'total_repayment_periods', 'actual_repayment_date', 'has_overdue_history_flag']
        
        for col in leakage_cols:
            if col in df.columns:
                drop_cols.append(col)
            
        df = df.drop(columns=[c for c in drop_cols if c in df.columns])
        
        # 3. Simple Encoding for Categorical
        # Gender: M->0, F->1, U->2
        if 'gender' in df.columns:
            df['gender'] = df['gender'].map({'M': 0, 'F': 1, 'U': 2}).fillna(2)
        
        # Occupation: Encode based on stability (lower number = more stable)
        if 'occupation' in df.columns:
            occupation_map = {
                'civil_servant': 0,  # Most stable
                'teacher': 1,
                'doctor': 2,
                'engineer': 3,
                'worker': 4,
                'business_owner': 5,
                'freelancer': 6      # Least stable
            }
            df['occupation'] = df['occupation'].map(occupation_map).fillna(4)
        
        # Encode new categorical features
        if 'dti_category' in df.columns:
            df['dti_category'] = df['dti_category'].map({'low_risk': 0, 'medium_risk': 1, 'high_risk': 2}).fillna(0)
        
        if 'age_group' in df.columns:
            df['age_group'] = df['age_group'].map({'young': 0, 'prime': 1, 'mature': 2, 'senior': 3}).fillna(1)
        
        if 'income_level' in df.columns:
            df['income_level'] = df['income_level'].map({'low_income': 0, 'middle_income': 1, 'high_income': 2}).fillna(1)
        
        if 'loan_duration_type' in df.columns:
            df['loan_duration_type'] = df['loan_duration_type'].map({'short_term': 0, 'long_term': 1, 'ultra_long': 2}).fillna(0)
            
        # 4. Feature Engineering: Debt-to-Income Ratio (DTI)
        # Avoid division by zero
        if 'loan_amount' in df.columns and 'monthly_income' in df.columns:
            df['dti_ratio'] = df['loan_amount'] / (df['monthly_income'] + 1.0)
            
            # Cross feature: age * dti interaction
            if 'age' in df.columns:
                df['age_dti_interaction'] = df['age'] * df['dti_ratio']
            
            # Log transform income to compress extreme values
            df['log_income'] = np.log1p(df['monthly_income'])
            
            # === 新增高价值特征（提升 F1-Score） ===
            
            # 1. 收入稳定性指标（反向 DTI）
            df['income_loan_ratio'] = df['monthly_income'] / (df['loan_amount'] + 1)
            
            # 2. 信用评分归一化（0-1）
            df['credit_score_norm'] = (df['credit_score'] - 300) / 550
            
            # 3. 综合风险指标（DTI × 信用）
            df['dti_credit_risk'] = df['dti_ratio'] * (1 - df['credit_score_norm'])
            
            # 4. 每月还款负担
            if 'loan_period_months' in df.columns:
                df['monthly_payment'] = df['loan_amount'] / (df['loan_period_months'] + 1)
                df['payment_income_ratio'] = df['monthly_payment'] / (df['monthly_income'] + 1)
            
            # 5. 年龄-信用交叉特征
            if 'age' in df.columns:
                df['age_credit_interaction'] = df['age'] * df['credit_score_norm']
            
            # 6. 职业风险归一化
            df['occupation_risk'] = df['occupation'] / 6.0
            
            # 7. 城市层级风险（反转：一线城市风险低）
            if 'city_tier' in df.columns:
                df['city_risk'] = (4 - df['city_tier']) / 3.0
            
        # Fill NaNs with 0
        df = df.fillna(0)
        
        print(f"✅ 加载特征: {len(df)} 行, {len(df.columns)} 列")
        return df
    finally:
        conn.close()


def prepare_training_data(
    df: pd.DataFrame, 
    target_col: str = 'target_label',  # Updated default
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
