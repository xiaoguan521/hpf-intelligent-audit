"""
AI Text-to-SQL 引擎
支持使用 LLM 动态生成 SQL 查询
统一使用 skills/llm_client.py 的 LLMClient
动态读取 MCP 标准定义文件获取最新 Schema
"""
import os
import re
from hpf_audit.utils.schema_loader import get_schema_context


def _build_system_prompt() -> str:
    """构建包含动态 Schema 的系统提示词"""
    schema = get_schema_context()
    return f"""你是公积金数据分析专家，精通 SQLite SQL。

{schema}

任务：根据用户问题生成 SQL 查询语句。
规则：
1. 只返回纯 SQL，不要 markdown，不要解释。
2. 优先使用模糊查询 (LIKE) 处理名称搜索。
3. 注意：金额字段通常是 FSE, JE, DKJE, TQJE 等。
4. 关联查询时请使用 JOIN。
"""


def _clean_sql(sql: str) -> str:
    """清理 SQL 中的 markdown 标记"""
    if not sql:
        return None
    sql = re.sub(r'^```sql\s*', '', sql)
    sql = re.sub(r'\s*```$', '', sql)
    return sql.strip()


def generate_sql_with_llm(user_query: str, provider: str = None, model: str = None) -> str:
    """使用统一 LLM 客户端生成 SQL"""
    try:
        from hpf_common.llm import LLMClient
        client = LLMClient(provider=provider, model=model, timeout=15)
        messages = [
            {"role": "system", "content": _build_system_prompt()},
            {"role": "user", "content": user_query}
        ]
        result = client.chat(
            messages=messages,
            temperature=0.1, 
            max_tokens=500
        )
        if result and not result.startswith("LLM 调用失败"):
            return _clean_sql(result)
        return None
    except Exception as e:
        print(f"LLM API 调用失败 ({provider}): {e}")
        return None


def generate_sql_fallback(user_query: str) -> str:
    """规则匹配后备方案 (已更新表名)"""
    q = user_query.lower()
    
    if any(k in q for k in ['风险', '骗提', '异常']):
        if any(k in q for k in ['分布', '统计', '数量']):
            return "SELECT FXLB, COUNT(*) as count FROM FX_SJ_JL GROUP BY FXLB ORDER BY count DESC"
        elif any(k in q for k in ['高', '严重']):
            return "SELECT * FROM FX_SJ_JL WHERE FXFZ > 80 ORDER BY FXFZ DESC LIMIT 20"
        return "SELECT FXLB, COUNT(*) as count FROM FX_SJ_JL GROUP BY FXLB"
    
    if any(k in q for k in ['用户', '个人', '人数', '账户']):
        if '正常' in q:
            return "SELECT COUNT(*) as total FROM GR_JC_JBXX WHERE GRJCZT = '正常'"
        return "SELECT GRJCZT, COUNT(*) as count FROM GR_JC_JBXX GROUP BY GRJCZT"
    
    if any(k in q for k in ['单位', '公司', '企业']):
        return "SELECT DWJJLX, COUNT(*) as count FROM DW_JC_JBXX GROUP BY DWJJLX"
    
    if any(k in q for k in ['贷款', '借款']):
        return "SELECT DKZT, COUNT(*) as count FROM GR_DK_HT GROUP BY DKZT"
    
    # 简单的提取/缴存统计
    if '提取' in q:
        return "SELECT TQLX, COUNT(*) as count, SUM(TQJE) as total FROM GR_TQ_MX GROUP BY TQLX"
        
    return "SELECT FXLB, COUNT(*) as count FROM FX_SJ_JL GROUP BY FXLB LIMIT 10"


async def text_to_sql(user_query: str, prefer_provider: str = None, nvidia_model: str = None) -> dict:
    """
    主入口：将自然语言转换为 SQL
    """
    provider = prefer_provider or os.getenv("DEFAULT_LLM_PROVIDER", "nvidia")
    model = nvidia_model or os.getenv("DEFAULT_LLM_MODEL")
    
    # 尝试 LLM 生成
    sql = generate_sql_with_llm(user_query, provider=provider, model=model)
    if sql:
        return {"sql": sql, "source": provider, "error": None}
    
    # 尝试备选提供商
    for backup in ["openai", "nvidia", "deepseek"]:
        if backup != provider:
            sql = generate_sql_with_llm(user_query, provider=backup)
            if sql:
                return {"sql": sql, "source": backup, "error": None}
    
    # 使用规则后备
    sql = generate_sql_fallback(user_query)
    return {"sql": sql, "source": "fallback", "error": None}
