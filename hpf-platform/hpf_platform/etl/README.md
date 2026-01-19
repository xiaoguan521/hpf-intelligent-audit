# Oracle → DuckDB 数据同步工具

使用 dlt 实现 Oracle 到 DuckDB 的增量数据同步，支持千万级数据高速同步。

## 快速开始

```bash
# 进入目录
cd etl

# 🆕 智能同步（LLM 自动分析表并推荐策略）
python app.py --smart

# 全自动智能同步（跳过审批）
python app.py --smart --auto

# 传统模式：首次全量同步
python app.py --fast

# 传统模式：日常增量同步
python app.py
```

## 三种同步模式

| 模式 | 命令 | 说明 | 适用场景 |
|------|------|------|----------|
| **🆕 智能模式** | `python app.py --smart` | LLM 分析表并推荐策略 | 新项目接入、不熟悉表结构 |
| **高速模式** | `python app.py --fast` | PyArrow 直接写入 DuckDB | 首次全量同步、千万级数据 |
| **标准模式** | `python app.py` | dlt pipeline 写入 | 日常增量同步 |

## 智能同步模式 (🆕)

智能同步会自动：
1. 连接 Oracle，获取所有表清单
2. 分析每个表的 DDL、大小、行数、分区信息
3. 使用 LLM 推荐最优同步参数（线程数、批量大小、是否分区）
4. 执行同步后校验数据条数
5. 校验通过后更新 dlt 状态，后续自动增量同步

### 智能模式参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--smart` | 启用智能模式 | - |
| `--auto` | 跳过审批确认 | 需确认 |
| `--schema NAME` | 指定 Oracle Schema | 环境变量 ORACLE_SCHEMA |
| `--tables LIST` | 要同步的表，逗号分隔或 `*` | `*` (全部) |

### 智能模式示例

```bash
# 同步全部表（需确认）
python app.py --smart

# 全自动同步全部表
python app.py --smart --auto

# 只同步指定表
python app.py --smart --tables "IM_ZJ_LS,USER_INFO"

# 指定 Schema
python app.py --smart --schema "MY_SCHEMA"
```

## 传统模式参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--fast` | 启用高速模式 | 关闭 |
| `--partition` | 按分区并行读取 | 关闭 |
| `--workers N` | 并行线程数 | 4 |
| `--batch-size N` | 每批读取行数 | 50000 |
| `--db PATH` | DuckDB 路径 | oracle_sync.duckdb |

## 配置文件

### 环境变量 (.env)

```bash
# Oracle 连接
ORACLE_USER=your_user
ORACLE_PASSWORD=your_password
ORACLE_HOST=your_host
ORACLE_SERVICE=your_service
ORACLE_SCHEMA=SHINEYUE40_BZBGJJYW_CS  # 默认 Schema
```

### 智能同步配置 (config.py)

```python
# 全局同步设置
SMART_SYNC_CONFIG = {
    "approval_mode": True,                # 审批模式
    "default_sync_interval": "0 2 * * *", # cron 表达式
}

# 表级配置
SYNC_TABLES = [
    "*",  # 同步全部表
    # 或指定详细配置:
    {
        "name": "IM_ZJ_LS",
        "sync_interval": "*/30 * * * *",  # 表级 cron 覆盖默认
        "priority": "high",
    }
]
```

## 性能参考

| 数据量 | 模式 | 预估耗时 |
|--------|------|----------|
| 1000万行 | 高速模式 | 15-25分钟 |
| 1000万行 | 标准模式 | 40-60分钟 |

## 依赖

```bash
pip install dlt[duckdb] oracledb sqlalchemy pyarrow
```

## 模块结构

```
etl/
├── app.py              # 命令行入口
├── config.py           # 配置文件
├── smart_sync.py       # 🆕 智能同步代理
├── oracle_inspector.py # 🆕 Oracle 元数据检查
├── sync_verifier.py    # 🆕 数据校验器
└── README.md
```

## 注意事项

1. **首次同步**：建议使用 `--smart` 让 LLM 推荐最优策略
2. **增量同步**：智能模式会自动更新 dlt 状态，后续运行走增量
3. **Oracle 版本**：低于 12.1 需安装 Instant Client 启用 thick 模式
