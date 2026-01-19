"""
ETL 配置模块
===============
- Oracle 数据源配置
- DuckDB 目标配置
- 表同步配置
"""
import os
from dotenv import load_dotenv

load_dotenv()


# ============================================================
# Oracle 源数据库配置
# ============================================================
ORACLE_CONFIG = {
    "user": os.getenv("ORACLE_USER", ""),
    "password": os.getenv("ORACLE_PASSWORD", ""),
    "host": os.getenv("ORACLE_HOST", "localhost"),
    "port": os.getenv("ORACLE_PORT", "1521"),
    "service_name": os.getenv("ORACLE_SERVICE", "ORCL"),
    "default_schema": os.getenv("ORACLE_SCHEMA", "SHINEYUE40_BZBGJJYW_CS"),
}

def get_oracle_connection_string() -> str:
    """生成 Oracle 连接字符串（SQLAlchemy 格式）"""
    return (
        f"oracle+oracledb://{ORACLE_CONFIG['user']}:{ORACLE_CONFIG['password']}"
        f"@{ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}"
        f"/?service_name={ORACLE_CONFIG['service_name']}"
    )


# ============================================================
# DuckDB 目标数据库配置
# ============================================================
# 获取项目根目录 (config.py 所在目录的上一级)
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
# 如果 config.py 在 etl/ 下，PROJECT_ROOT 就是 etl/，这不对。应该是由 app.py 决定的 root
# 更稳妥的是：
_current_dir = os.path.dirname(os.path.abspath(__file__))
# 假设结构是 project/etl/config.py -> project/
PROJECT_ROOT = os.path.dirname(_current_dir)

DUCKDB_DIR = os.path.join(PROJECT_ROOT, "data")
if not os.path.exists(DUCKDB_DIR):
    os.makedirs(DUCKDB_DIR)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", os.path.join(DUCKDB_DIR, "warehouse.duckdb"))


# ============================================================
# ODS 层表配置
# 定义需要从 Oracle 同步到 DuckDB 的表
# ============================================================
ODS_TABLES = [
    {
        "table_name": "LOAN_APPLY",          # Oracle 表名
        "primary_key": "LOAN_ID",            # 主键（用于 merge/upsert）
        "incremental_field": "UPDATE_TIME",  # 增量字段
        "description": "贷款申请表",
    },
    {
        "table_name": "LOAN_REPAY",
        "primary_key": "REPAY_ID",
        "incremental_field": "UPDATE_TIME",
        "description": "贷款还款表",
    },
    {
        "table_name": "USER_INFO",
        "primary_key": "USER_ID",
        "incremental_field": "UPDATE_TIME",
        "description": "用户基本信息表",
    },
    {
        "table_name": "DEPOSIT_RECORD",
        "primary_key": "DEPOSIT_ID",
        "incremental_field": "UPDATE_TIME",
        "description": "缴存记录表",
    },
    {
        "table_name": "WITHDRAW_RECORD",
        "primary_key": "WITHDRAW_ID",
        "incremental_field": "UPDATE_TIME",
        "description": "提取记录表",
    },
]


# ============================================================
# Pipeline 配置
# ============================================================
PIPELINE_CONFIG = {
    "pipeline_name": os.getenv("PIPELINE_NAME", "oracle_to_duckdb"),
    "dataset_name": os.getenv("DUCKDB_DATASET", "ods"),  # ODS 层 schema 名称
    "default_initial_value": os.getenv("DEFAULT_INITIAL_VALUE", "2020-01-01 00:00:00"),  # 增量同步初始值
}


# ============================================================
# 智能同步配置
# ============================================================
SMART_SYNC_CONFIG = {
    "approval_mode": True,                    # True=审批模式, False=全自动
    "default_sync_interval": "0 2 * * *",     # 默认增量同步 cron (每天凌晨2点)
    "verify_after_sync": True,                # 同步后自动校验行数
}


# ============================================================
# 表同步配置
# * 表示同步 schema 下所有表
# 也可以指定表名列表或详细配置
# ============================================================
SYNC_TABLES = [
    "*",  # 同步全部表
    # 或指定表名列表:
    # "IM_ZJ_LS",
    # "USER_INFO",
    
    # 或详细配置:
    # {
    #     "name": "IM_ZJ_LS",
    #     "schema": "SHINEYUE40_BZBGJJYW_CS",  # 可选，覆盖默认 schema
    #     "incremental_field": "ID",           # 增量字段
    #     "primary_key": "ID",                 # 主键
    #     "sync_interval": "*/30 * * * *",     # 表级 cron 覆盖默认
    #     "priority": "high",                  # high/medium/low
    # }
]


# ============================================================
# 智能 Oracle 驱动配置
# ============================================================
class OracleConfig:
    """
    智能 Oracle 配置管理
    - 自动检测 Oracle 版本
    - 智能选择 Thin/Thick 模式
    - 支持环境变量覆盖
    """
    
    _mode = None
    _version = None
    _initialized = False
    
    @classmethod
    def init_oracle_client(cls) -> str:
        """
        智能初始化 Oracle 客户端
        
        Returns:
            str: "thin" 或 "thick"
        """
        if cls._initialized:
            return cls._mode
        
        import oracledb
        
        # 1. 检查环境变量强制模式
        force_mode = os.getenv("ORACLE_FORCE_MODE", "").lower()
        if force_mode == "thick":
            print("🔧 环境变量强制使用 Thick 模式")
            cls._init_thick_mode()
            cls._initialized = True
            return "thick"
        elif force_mode == "thin":
            print("🔧 环境变量强制使用 Thin 模式 (确保 Oracle >= 12.1)")
            cls._mode = "thin"
            cls._initialized = True
            return "thin"
        
        # 2. 尝试自动检测版本
        try:
            print("🔍 正在检测 Oracle 版本...")
            mode, version = cls._detect_version()
            cls._version = version
            
            if mode == "thick":
                print(f"📊 检测到 Oracle {version} (< 12.1)，使用 Thick 模式")
                cls._init_thick_mode()
            else:
                print(f"📊 检测到 Oracle {version} (>= 12.1)，使用 Thin 模式")
                cls._mode = "thin"
            
            cls._initialized = True
            return mode
        except Exception as e:
            # 3. 版本检测失败，回退到 Thick 模式
            print(f"⚠️  版本检测失败 ({e})，尝试 Thick 模式")
            try:
                cls._init_thick_mode()
                cls._initialized = True
                return "thick"
            except Exception as thick_error:
                print(f"❌ Thick 模式初始化失败: {thick_error}")
                print("ℹ️  回退到 Thin 模式（可能不兼容 Oracle 11g）")
                cls._mode = "thin"
                cls._initialized = True
                return "thin"
    
    @classmethod
    def _detect_version(cls) -> tuple:
        """
        检测 Oracle 版本
        
        Returns:
            tuple: ("thin" 或 "thick", "版本号")
        """
        import oracledb
        
        # 构建临时连接（使用 Thin 模式尝试）
        conn_str = (
            f"{ORACLE_CONFIG['user']}/{ORACLE_CONFIG['password']}"
            f"@{ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}"
            f"/{ORACLE_CONFIG['service_name']}"
        )
        
        conn = oracledb.connect(conn_str)
        cursor = conn.cursor()
        cursor.execute("SELECT version FROM v$instance")
        version = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        # 解析版本号
        major = int(version.split('.')[0])
        
        # Oracle 12.1+ 支持 Thin 模式
        if major >= 12:
            return "thin", version
        else:
            return "thick", version
    
    @classmethod
    def _init_thick_mode(cls):
        """初始化 Thick 模式（需要 Oracle Instant Client）"""
        import oracledb
        from pathlib import Path
        
        client_dir = os.getenv("ORACLE_CLIENT_DIR")
        
        # 如果未指定，尝试自动查找
        if not client_dir:
            client_dir = cls._find_instant_client()
        
        if client_dir and Path(client_dir).exists():
            oracledb.init_oracle_client(lib_dir=client_dir)
            cls._mode = "thick"
            print(f"✅ Thick 模式已启用: {client_dir}")
        else:
            raise RuntimeError(
                "Thick 模式需要 Oracle Instant Client。\n"
                "请设置环境变量: ORACLE_CLIENT_DIR=/path/to/instantclient\n"
                "或将 Instant Client 放在 etl/ 目录下"
            )
    
    @classmethod
    def _find_instant_client(cls) -> str:
        """自动查找 Instant Client 路径"""
        from pathlib import Path
        
        current_dir = Path(__file__).parent
        
        # 常见路径列表
        search_patterns = [
            "instantclient-basic-windows*/instantclient_*",  # Windows 解压格式
            "instantclient_*",                               # 标准格式
            "/opt/oracle/instantclient_*",                   # Linux 标准路径
            "/usr/lib/oracle/*/client64/lib",                # Linux 系统路径
            "C:\\instantclient_*",                           # Windows C盘
        ]
        
        for pattern in search_patterns:
            if pattern.startswith("/") or pattern.startswith("C:"):
                # 绝对路径
                matches = list(Path("/").glob(pattern.lstrip("/")))
            else:
                # 相对当前目录
                matches = list(current_dir.glob(pattern))
            
            if matches:
                return str(matches[0])
        
        return None
    
    @classmethod
    def get_mode(cls) -> str:
        """获取当前模式"""
        if not cls._initialized:
            cls.init_oracle_client()
        return cls._mode
    
    @classmethod
    def get_version(cls) -> str:
        """获取 Oracle 版本"""
        return cls._version or "未知"

