import duckdb
import os

# 确保目录存在
os.makedirs("hpf-platform/data", exist_ok=True)
db_path = "hpf-platform/data/warehouse.duckdb"

conn = duckdb.connect(db_path)

# 1. 创建源数据 Schema (模拟 Oracle 同步过来的原始数据层)
conn.execute("CREATE SCHEMA IF NOT EXISTS oracle_data;")

# 2. 创建表结构 & 插入测试数据

# ----------------------------
# 表 1: GR_DK_HT (贷款合同)
# ----------------------------
print("Creating GR_DK_HT...")
conn.execute("""
    CREATE OR REPLACE TABLE oracle_data.GR_DK_HT (
        CONTRACT_NO VARCHAR,   -- 合同号
        CUST_ID VARCHAR,       -- 客户ID
        LOAN_AMT DECIMAL,      -- 贷款金额
        LOAN_PERIOD INTEGER,   -- 期限(月)
        LOAN_DATE DATE,        -- 放款日期
        STATUS VARCHAR         -- 状态 (01:正常, 02:逾期, 03:结清)
    );
""")
# 插入数据: 3笔正常, 2笔逾期
conn.execute("""
    INSERT INTO oracle_data.GR_DK_HT VALUES
    ('HT2023001', 'CUST001', 500000, 360, '2023-01-01', '01'), -- 正常
    ('HT2023002', 'CUST002', 800000, 240, '2023-02-15', '02'), -- 逾期
    ('HT2023003', 'CUST003', 300000, 120, '2023-03-10', '01'), -- 正常
    ('HT2023004', 'CUST004', 1000000, 360, '2023-04-20', '02'), -- 逾期
    ('HT2023005', 'CUST005', 450000, 180, '2023-05-05', '03'); -- 结清
""")

# ----------------------------
# 表 2: GR_DK_HKMX (还款明细 - 用于计算逾期特征)
# ----------------------------
print("Creating GR_DK_HKMX...")
conn.execute("""
    CREATE OR REPLACE TABLE oracle_data.GR_DK_HKMX (
        CONTRACT_NO VARCHAR,   -- 合同号
        PERIOD INTEGER,        -- 期数
        DUE_DATE DATE,         -- 应还日期
        ACTUAL_DATE DATE,      -- 实还日期
        STATUS VARCHAR         -- 状态 (1:正常, 2:逾期)
    );
""")
# 插入数据: CUST002 和 CUST004 有逾期记录
conn.execute("""
    INSERT INTO oracle_data.GR_DK_HKMX VALUES
    ('HT2023001', 1, '2023-02-01', '2023-02-01', '1'),
    ('HT2023002', 1, '2023-03-15', '2023-03-20', '2'), -- 逾期5天
    ('HT2023002', 2, '2023-04-15', NULL, '2'),         -- 当前逾期未还
    ('HT2023003', 1, '2023-04-10', '2023-04-10', '1'),
    ('HT2023004', 1, '2023-05-20', NULL, '2');         -- 逾期未还
""")

# ----------------------------
# 表 3: GR_JCXX (个人基础信息 - 用于补充画像)
# ----------------------------
print("Creating GR_JCXX...")
conn.execute("""
    CREATE OR REPLACE TABLE oracle_data.GR_JCXX (
        CUST_ID VARCHAR,       -- 客户ID
        NAME VARCHAR,          -- 姓名
        AGE INTEGER,           -- 年龄
        INCOME DECIMAL,        -- 月收入
        GENDER VARCHAR         -- 性别 (1:男, 2:女)
    );
""")
conn.execute("""
    INSERT INTO oracle_data.GR_JCXX VALUES
    ('CUST001', '张三', 30, 8000, '1'),
    ('CUST002', '李四', 45, 12000, '1'),
    ('CUST003', '王五', 28, 6000, '2'),
    ('CUST004', '赵六', 50, 20000, '1'),
    ('CUST005', '孙七', 35, 9500, '2');
""")

conn.close()
print(f"✅ Data initialized in {db_path}")
