import json
import re
import os
import sqlite3
from typing import Dict, Any, Optional, Tuple, List
from hpf_audit.skills.mcp_client import MCPClient
from hpf_audit.skills.validator import ConfigurationValidator

from hpf_audit.knowledge.vector_store import VectorStoreManager

from hpf_common.llm import LLMClient

GENERATOR_SYSTEM_PROMPT = """
You are an expert AI Audit Skill Generator.
Your task is to convert a user's natural language audit requirement into a precise **YAML Configuration**.

**CRITICAL: All output MUST be in Chinese (中文), including:**
- meta.name (名称必须用中文)
- meta.description (描述必须用中文)  
- parameters.description (参数描述必须用中文)
- SQL comments (SQL 注释必须用中文)
- risk_logic.message (风险消息必须用中文)

### Successful Examples (Learn from these)
{few_shot_examples}

### Relevant Regulations & Rules
{rag_context}

### Target Format (YAML)
```yaml
skill_id: "逾期_贷款_监测_a1b2"
template_type: "sql_risk_check"
meta:
  name: "简短的中文名称"
  description: "详细的中文描述"
  tags: ["标签1", "标签2"]
parameters:
  - name: "param_name"
    type: "number" # or string
    default: 10000
    description: "中文参数描述"
sql_template: |
  SELECT ... FROM ... WHERE val > {{ param_name }}
  -- 中文注释说明查询逻辑
risk_logic:
  risk_level: "High" # Low, Medium, High
  condition: "len(results) > 0"
  message: "发现 {len(results)} 条异常记录"
```

### Database Schema
{schema_context}

### Rules
1. **Language**: Output ALL text in Chinese (中文), including name, description, comments, messages
2. **Compliance**: Prioritize logic described in "Relevant Regulations"
3. **SQL Safety**: Only use `SELECT`. Use `{{ param }}` for dynamic values
4. **Logic**: `condition` python logic runs on `results`
5. **Output**: Return **ONLY** the YAML block
"""

class SkillGenerator:
    """
    Generates Skill Configuration from natural language using LLM.
    Enhanced with RAG and Feedback Loop.
    """
    def __init__(self, db_path: str = "./housing_provident_fund.db"):
        import os
        self.mcp_client = MCPClient(db_path)
        
        # 直接使用统一的 LLM 客户端
        # 直接使用统一的 LLM 客户端
        from hpf_common.llm import LLMClient
        self.llm = LLMClient(verbose=False)
        self.validator = ConfigurationValidator()
        
        # 初始化向量存储管理器
        try:
            self.vsm = VectorStoreManager(index_path="data/faiss_index")
            self.retriever_available = True
        except Exception as e:
            print(f"⚠️ VectorStoreManager 初始化失败: {e}")
            self.retriever_available = False
            
        self.db_path = db_path
        self._ensure_tables()

    def _ensure_tables(self):
        """确保必要的数据库表存在"""
        try:
            import sqlite3
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 创建 META_SKILL_DEF 表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS META_SKILL_DEF (
                    skill_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    template_type TEXT,
                    configuration TEXT,
                    markdown_content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 0
                )
            """)
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"⚠️ 数据库初始化失败: {e}")

    def _get_schema_context(self) -> str:
        """Fetch schema dynamically from MCP standard definition file"""
        try:
            from hpf_audit.utils.schema_loader import get_schema_context
            return get_schema_context()
        except Exception as e:
            print(f"[SkillGenerator] Schema 加载失败，使用降级方案: {e}")
            # 降级方案：通过 MCP 读取
            tables = ["DW_JC_JBXX", "GR_JC_JBXX", "GR_JC_MX", 
                      "GR_DK_HT", "GR_DK_YQ", "FX_SJ_JL", "GT_JKR_XX", "GR_DK_HK"]
            context = []
            for table in tables:
                ddl = self.mcp_client.read_resource("hpf-db-adapter", f"hpf://schema/tables/{table}")
                context.append(ddl)
            return "\n\n".join(context)

    def _get_feedback_examples(self, limit: int = 2) -> str:
        """Fetch high-quality (active) skills as few-shot examples"""
        try:
            conn = sqlite3.connect(f"file:{self.db_path}?mode=ro", uri=True)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            # Get active skills, sorted by creating time desc
            cursor.execute("""
                SELECT configuration FROM META_SKILL_DEF 
                WHERE is_active = 1 
                AND configuration IS NOT NULL 
                ORDER BY created_at DESC LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            conn.close()
            
            examples = []
            for row in rows:
                try:
                    config = json.loads(row['configuration'])
                    # Convert back to YAML-ish string for prompt
                    import yaml
                    yaml_str = yaml.dump(config, sort_keys=False, allow_unicode=True)
                    examples.append(f"Example:\n```yaml\n{yaml_str}\n```")
                except:
                    continue
                    
            if not examples:
                return "No existing examples available yet."
                
            return "\n\n".join(examples)
        except Exception:
            return "No existing examples available yet."

    def generate(self, user_requirement: str) -> str:
        """
        Generate a skill configuration with RAG and Feedback.
        Returns the raw YAML string.
        """
        # 1. RAG Retrieval (如果可用)
        rag_context = ""
        if self.retriever_available:
            try:
                print(f"      [RAG] 开始检索相关法规...")
                # 使用 VectorStoreManager 进行搜索
                rag_hits = self.vsm.search(user_requirement, top_k=3)
                
                if rag_hits:
                    print(f"      ✅ RAG 检索成功，找到 {len(rag_hits)} 条相关内容")
                    rag_context = "Reference the following knowledge:\n"
                    for hit in rag_hits:
                        content = hit['content'].replace('\n', ' ')
                        rag_context += f"- {content[:200]}... (Score: {hit['score']:.3f})\n"
                else:
                    print(f"      ⚠️  RAG 检索返回空结果")
                    rag_context = "No specific regulations found."
            except Exception as e:
                print(f"      ⚠️  RAG 检索失败: {e}")
                rag_context = "No specific regulations found."
        else:
            print(f"      ⚠️  VectorStore 不可用，跳过 RAG")
            rag_context = "No specific regulations found."

        # 2. Feedback Loop
        examples = self._get_feedback_examples()
        
        # 3. Schema Context
        schema = self._get_schema_context()
        
        # 4. Assemble Prompt
        prompt = GENERATOR_SYSTEM_PROMPT
        prompt = prompt.replace("{few_shot_examples}", examples)
        prompt = prompt.replace("{rag_context}", rag_context)
        prompt = prompt.replace("{schema_context}", schema)
        
        full_prompt = f"{prompt}\n\nUser Request: {user_requirement}\n\nOutput YAML:"
        
        # 5. Call LLM with longer timeout
        print(f"      [LLM] 发送请求 (Timeout=120s)...")
        response = None
        try:
            # 构造标准消息格式
            messages = [{"role": "user", "content": full_prompt}]
            
            # 调用 chat 接口
            response = self.llm.chat(messages, timeout=120)
        except Exception as e:
            print(f"      [LLM] 请求失败: {e}")
            
        # 检查是否调用失败
        if not response or response.startswith("LLM 调用失败"):
            print(f"      ⚠️  LLM生成失败或超时，自动切换到模板降级模式...")
            return self._generate_from_template(user_requirement)
        
        # 6. Extract & Validate
        yaml_content = self._extract_yaml(response)
        is_valid, error, _ = self.validator.validate_yaml(yaml_content)
        
        if is_valid:
            print("      [LLM] 生成并验证成功")
            # 替换为中文友好的 skill_id
            yaml_content = self._replace_skill_id_with_chinese(yaml_content, user_requirement)
            return yaml_content
        else:
            print(f"      [Validator] 生成的YAML无效: {error}")
            print(f"      ⚠️  验证失败，尝试使用模板降级生成...")
            return self._generate_from_template(user_requirement)
    
    def _replace_skill_id_with_chinese(self, yaml_content: str, requirement: str) -> str:
        """
        替换 YAML 中的 skill_id 为中文关键词格式
        """
        import re
        import uuid
        import yaml
        
        try:
            # 解析 YAML
            config = yaml.safe_load(yaml_content)
            
            # 提取中文关键词
            chinese_keywords = re.findall(r'[\u4e00-\u9fff]+', requirement)
            if chinese_keywords:
                # 取前2-3个关键词作为 ID
                key_words = '_'.join(chinese_keywords[:3])
                new_skill_id = f"{key_words}_{uuid.uuid4().hex[:4]}"
            else:
                # 如果没有中文，保持原样
                return yaml_content
            
            # 替换 skill_id
            config['skill_id'] = new_skill_id
            
            # 重新生成 YAML
            new_yaml = yaml.dump(config, allow_unicode=True, sort_keys=False)
            return new_yaml
        except:
            # 如果失败，返回原内容
            return yaml_content

    def _generate_from_template(self, requirement: str) -> str:
        """
        基于规则的模板生成（降级方案）
        """
        import uuid
        import re
        
        # 简单的关键词提取
        is_overdue = "逾期" in requirement
        is_loan = "贷款" in requirement
        is_fund = "公积金" in requirement
        
        # 生成更友好的中文 ID（基于需求关键词）
        # 提取中文关键词
        chinese_keywords = re.findall(r'[\u4e00-\u9fff]+', requirement)
        if chinese_keywords:
            # 取前2-3个关键词作为 ID
            key_words = '_'.join(chinese_keywords[:3])
            skill_id = f"{key_words}_{uuid.uuid4().hex[:4]}"
        else:
            # 降级到随机ID
            skill_id = f"generated_skill_{uuid.uuid4().hex[:8]}"
        
        # 生成和处理描述
        safe_description = requirement[:100].replace(':', '-').replace('\\n', ' ').replace('\n', ' ')
        
        # 默认模板
        yaml_template = f"""skill_id: {skill_id}
template_type: sql_risk_check
meta:
  name: 自动生成Skill ({skill_id})
  description: {safe_description}
  tags:
    - 自动生成
    - 风险监测

sql_template: |
  -- 自动生成的SQL模板（请根据实际需求修改）
  SELECT *
  FROM GR_DK_HT
  WHERE 1=1
  -- AND YQTS > 0 
  LIMIT {{{{ limit }}}}

parameters:
  - name: limit
    type: integer
    description: 返回记录数量
    default: 100
    required: false

risk_logic:
  risk_level: Medium
  condition: "len(results) > 0"
  message: "发现 {{len(results)}} 条异常记录"
"""

        # 针对逾期监测的特定模板
        if is_overdue:
            yaml_template = f"""skill_id: {skill_id}
template_type: sql_risk_check
meta:
  name: 逾期风险监测 (自动生成)
  description: {safe_description}
  tags:
    - 逾期
    - 风险监测
    - 自动生成

sql_template: |
  SELECT 
    l.DKZH,
    l.DKJE,
    o.YQTS,
    o.YQZE,
    o.YQDJ
  FROM GR_DK_YQ o
  JOIN GR_DK_HT l ON o.DKZH = l.DKZH
  WHERE o.SFJQ = '否'
  {{% if min_overdue_days %}}
    AND o.YQTS >= {{{{ min_overdue_days }}}}
  {{% endif %}}
  ORDER BY o.YQTS DESC
  LIMIT {{{{ limit }}}}

parameters:
  - name: min_overdue_days
    type: integer
    description: 最小逾期天数
    default: 0
    required: false
  
  - name: limit
    type: integer
    description: 返回记录数量
    default: 100
    required: false

risk_logic:
  risk_level: High
  condition: "len(results) > 0"
  message: "发现 {{len(results)}} 笔逾期贷款"
"""

        print("      [Template] 已使用模板生成配置")
        return yaml_template

    def _extract_yaml(self, text: str) -> str:
        """Extract YAML block from LLM response"""
        match = re.search(r"```(?:yaml)?\s*(.*?)\s*```", text, re.DOTALL)
        if match:
            return match.group(1).strip()
        return text.strip()

    def save_to_db(self, config_yaml: str = None, config_data: Dict[str, Any] = None, 
                   requirement: str = "", is_active: int = 0) -> int:
        """
        Save generated skill to database.
        
        Args:
            config_yaml: YAML配置字符串（优先使用）
            config_data: 配置字典（如果没有config_yaml）
            requirement: 用户需求描述
            is_active: 是否激活（0=Shadow Mode, 1=Active）
            
        Returns:
            skill_db_id: 数据库中的ID
        """
        import sqlite3
        import json
        import yaml
        
        try:
            # 解析配置
            if config_yaml:
                config_data = yaml.safe_load(config_yaml)
            elif not config_data:
                raise ValueError("必须提供 config_yaml 或 config_data")
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            skill_id = config_data.get("skill_id", "generated_skill")
            name = config_data.get("name") or config_data.get("meta", {}).get("name", "未命名Skill")
            description = config_data.get("description") or config_data.get("meta", {}).get("description", "")
            template_type = config_data.get("template_type", "sql_risk_check")
            
            # 存储原始YAML配置
            config_text = config_yaml if config_yaml else yaml.dump(config_data, allow_unicode=True)
            
            markdown_content = f"""# {name}
{description}

## 用户需求
{requirement}

## 参数
{json.dumps(config_data.get('parameters', []), indent=2, ensure_ascii=False)}

## 风险逻辑
{json.dumps(config_data.get('risk_logic', {}), indent=2, ensure_ascii=False)}

## 状态
{'✅ Active' if is_active else '🔄 Shadow Mode'}
"""
            
            # 检查是否已存在
            cursor.execute("SELECT skill_id FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
            exists = cursor.fetchone()
            
            if exists:
                # 更新
                sql = """
                UPDATE META_SKILL_DEF 
                SET name = ?, description = ?, markdown_content = ?, 
                    configuration = ?, template_type = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE skill_id = ?
                """
                cursor.execute(sql, (
                    name, description, markdown_content, config_text, template_type, is_active, skill_id
                ))
            else:
                # 插入
                sql = """
                INSERT INTO META_SKILL_DEF 
                (skill_id, name, description, markdown_content, configuration, template_type, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """
                cursor.execute(sql, (
                    skill_id, name, description, markdown_content, config_text, template_type, is_active
                ))
            
            skill_db_id = cursor.lastrowid if not exists else None
            conn.commit()
            conn.close()
            
            # ✨ 新增：自动向量化并索引到知识库
            try:
                self._index_skill_to_vector_db(config_data, requirement)
            except Exception as e:
                print(f"⚠️ Skill 向量化失败（不影响主流程）: {e}")
            
            return skill_db_id or 0
        except Exception as e:
            print(f"Error saving to DB: {e}")
            import traceback
            traceback.print_exc()
            return 0
    
    def _index_skill_to_vector_db(self, config: Dict, user_requirement: str):
        """
        将 Skill 索引到向量库（用于语义检索）
        
        Args:
            config: Skill 配置字典
            user_requirement: 用户原始需求
        """
        try:
            import json
            import re
            from hpf_audit.knowledge.vector_store import VectorStoreManager
            
            # 1. 提取关键信息
            skill_id = config.get('skill_id', 'unknown')
            meta = config.get('meta', {})
            name = meta.get('name', '未命名Skill')
            description = meta.get('description', '')
            tags = meta.get('tags', [])
            related_skills = meta.get('related_skills', [])

            
            # 2. 构建内容（用于向量化）
            content_parts = [
                f"名称：{name}",
                f"功能：{description}",
                f"用户需求：{user_requirement}",
            ]
            
            # 添加参数说明
            if 'parameters' in config:
                params_desc = "参数：" + ", ".join([
                    f"{p['name']}({p.get('description', '')})"
                    for p in config['parameters']
                ])
                content_parts.append(params_desc)
            
            # 添加 SQL 涉及的表
            if 'sql_template' in config:
                sql = config['sql_template']
                tables = re.findall(r'FROM\s+(\w+)', sql, re.IGNORECASE)
                if tables:
                    content_parts.append(f"涉及表：{', '.join(set(tables))}")
            
            content = "\n".join(content_parts)
            
            # 3. 构建 metadata
            metadata = {
                "skill_id": skill_id,
                "db_table": "META_SKILL_DEF",
                "skill_type": config.get('template_type', 'sql_risk_check'),
                "parameters": {
                    p['name']: {
                        "type": p.get('type', 'string'),
                        "default": p.get('default'),
                        "required": p.get('required', False)
                    }
                    for p in config.get('parameters', [])
                },
                "related_skills": related_skills  # 存储关联技能 ID
            }

            # 4. 插入或更新 FAISS 向量库
            vsm = VectorStoreManager()
            # 先尝试删除旧的（如果有）
            vsm.delete_skill(skill_id)
            # 添加新的
            vsm.add_skills([{
                "skill_id": skill_id,
                "name": name,
                "description": description,
                "content": content,
                "metadata": metadata
            }])
            
            print(f"✅ Skill '{name}' 已索引到FAISS向量库")
            
        except Exception as e:
            print(f"⚠️ Skill 向量化失败: {e}")
            import traceback
            traceback.print_exc()


