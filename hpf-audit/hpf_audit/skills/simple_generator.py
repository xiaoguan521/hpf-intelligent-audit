"""
简单的 Skill 生成器
不依赖 ChromaDB、向量检索等复杂模块
"""
import uuid
import sqlite3
from typing import Dict, Any


class SimpleSkillGenerator:
    """
    简单的 Skill 生成器
    基于关键词匹配和模板生成
    """
    
    def __init__(self, db_path: str = "./housing_provident_fund.db"):
        self.db_path = db_path
    
    def generate(self, requirement: str) -> str:
        """
        根据需求生成 YAML 配置
        """
        # 生成唯一ID
        skill_id = f"simple_skill_{uuid.uuid4().hex[:8]}"
        
        # 关键词检测
        requirement_lower = requirement.lower()
        
        # 根据关键词选择模板
        if "逾期" in requirement_lower:
            return self._generate_overdue_template(skill_id, requirement)
        elif "贷款" in requirement_lower:
            return self._generate_loan_template(skill_id, requirement)
        elif "提取" in requirement_lower:
            return self._generate_withdrawal_template(skill_id, requirement)
        elif "缴存" in requirement_lower:
            return self._generate_deposit_template(skill_id, requirement)
        else:
            return self._generate_general_template(skill_id, requirement)
    
    def _generate_overdue_template(self, skill_id: str, requirement: str) -> str:
        """生成逾期相关的模板"""
        return f"""skill_id: {skill_id}
template_type: sql_risk_check
meta:
  name: 逾期风险监测
  description: {requirement[:100]}
  tags:
    - 逾期
    - 风险监测
    - 自动生成

sql_template: |
  SELECT 
    DKZH,
    GRZH,
    DKJE,
    DKZT,
    SQZR
  FROM GR_DK_HT
  WHERE DKZT LIKE '%逾期%'
  ORDER BY SQZR DESC
  LIMIT {{{{ limit }}}}

parameters:
  - name: limit
    type: integer
    description: 返回记录数量上限
    default: 100
    required: false

risk_logic:
  risk_level: High
  condition: "len(results) > 0"
  message: "发现 {{len(results)}} 笔逾期贷款，需要重点关注"
"""

    def _generate_loan_template(self, skill_id: str, requirement: str) -> str:
        """生成贷款相关的模板"""
        return f"""skill_id: {skill_id}
template_type: sql_risk_check
meta:
  name: 贷款风险检查
  description: {requirement[:100]}
  tags:
    - 贷款
    - 风险检查
    - 自动生成

sql_template: |
  SELECT 
    DKZH,
    GRZH,
    DKJE,
    DKQX,
    DKZT,
    SQZR
  FROM GR_DK_HT
  WHERE 1=1
  {{% if min_amount %}}
    AND DKJE >= {{{{ min_amount }}}}
  {{% endif %}}
  ORDER BY DKJE DESC
  LIMIT {{{{ limit }}}}

parameters:
  - name: min_amount
    type: number
    description: 最小贷款金额
    default: 0
    required: false
  
  - name: limit
    type: integer
    description: 返回记录数量上限
    default: 100
    required: false

risk_logic:
  risk_level: Medium
  condition: "len(results) > 0 and any(float(r.get('DKJE', 0)) > 500000 for r in results)"
  message: "发现 {{len(results)}} 笔贷款记录，其中包含大额贷款"
"""

    def _generate_withdrawal_template(self, skill_id: str, requirement: str) -> str:
        """生成提取相关的模板"""
        return f"""skill_id: {skill_id}
template_type: sql_risk_check
meta:
  name: 提取行为分析
  description: {requirement[:100]}
  tags:
    - 提取
    - 行为分析
    - 自动生成

sql_template: |
  SELECT 
    GRZH,
    trans_type,
    FSE,
    YWRQ,
    ZY
  FROM GR_JC_MX
  WHERE trans_type LIKE '%提取%'
  ORDER BY YWRQ DESC
  LIMIT {{{{ limit }}}}

parameters:
  - name: limit
    type: integer
    description: 返回记录数量上限
    default: 100
    required: false

risk_logic:
  risk_level: Medium
  condition: "len(results) > 0"
  message: "发现 {{len(results)}} 笔提取记录"
"""

    def _generate_deposit_template(self, skill_id: str, requirement: str) -> str:
        """生成缴存相关的模板"""
        return f"""skill_id: {skill_id}
template_type: sql_risk_check
meta:
  name: 缴存状态检查
  description: {requirement[:100]}
  tags:
    - 缴存
    - 状态检查
    - 自动生成

sql_template: |
  SELECT 
    GRZH,
    XINGMING,
    DWZH,
    GRJCZT,
    GRZHYE,
    YJCJS
  FROM GR_JC_JBXX
  WHERE 1=1
  {{% if status %}}
    AND GRJCZT = '{{{{ status }}}}'
  {{% endif %}}
  ORDER BY GRZHYE DESC
  LIMIT {{{{ limit }}}}

parameters:
  - name: status
    type: string
    description: 缴存状态筛选
    default: ""
    required: false
  
  - name: limit
    type: integer
    description: 返回记录数量上限
    default: 100
    required: false

risk_logic:
  risk_level: Low
  condition: "len(results) > 0"
  message: "查询到 {{len(results)}} 个账户"
"""

    def _generate_general_template(self, skill_id: str, requirement: str) -> str:
        """生成通用模板"""
        return f"""skill_id: {skill_id}
template_type: sql_risk_check
meta:
  name: 通用数据检查
  description: {requirement[:100]}
  tags:
    - 通用检查
    - 自动生成

sql_template: |
  SELECT 
    GRZH,
    XINGMING,
    DWZH,
    GRJCZT,
    GRZHYE
  FROM GR_JC_JBXX
  WHERE GRJCZT = '正常'
  ORDER BY GRZHYE DESC
  LIMIT {{{{ limit }}}}

parameters:
  - name: limit
    type: integer
    description: 返回记录数量上限
    default: 100
    required: false

risk_logic:
  risk_level: Low
  condition: "len(results) > 0"
  message: "查询到 {{len(results)}} 条记录"
"""

    def save_to_db(self, config_yaml: str, requirement: str, is_active: int = 0) -> bool:
        """
        保存配置到数据库
        """
        try:
            import yaml
            config_dict = yaml.safe_load(config_yaml)
            
            skill_id = config_dict.get('skill_id')
            name = config_dict.get('meta', {}).get('name', '未命名 Skill')
            description = config_dict.get('meta', {}).get('description', requirement[:200])
            template_type = config_dict.get('template_type', 'sql_risk_check')
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # 检查是否已存在
            cursor.execute("SELECT skill_id FROM META_SKILL_DEF WHERE skill_id = ?", (skill_id,))
            exists = cursor.fetchone()
            
            if exists:
                # 更新
                cursor.execute("""
                    UPDATE META_SKILL_DEF 
                    SET name = ?, description = ?, configuration = ?, 
                        template_type = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE skill_id = ?
                """, (name, description, config_yaml, template_type, is_active, skill_id))
            else:
                # 插入
                cursor.execute("""
                    INSERT INTO META_SKILL_DEF 
                    (skill_id, name, description, configuration, template_type, is_active)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (skill_id, name, description, config_yaml, template_type, is_active))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"保存到数据库失败: {e}")
            return False