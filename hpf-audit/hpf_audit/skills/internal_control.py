"""
内部操作风险审计 Skill
检测内部人员的异常操作行为
"""
from typing import Dict, Any
from .base import BaseSkill


class InternalControlSkill(BaseSkill):
    """内部操作风险审计 Skill"""
    
    @property
    def name(self) -> str:
        return "internal_control"
    
    @property
    def description(self) -> str:
        return """检测内部人员的异常操作行为，防止内部舞弊。

支持的检查类型（check_type）：
1. dormant_account_activated - 睡眠户突然激活（长期无交易账户的大额提取）
2. off_hours_operation - 非工作时间操作（22:00-08:00或周末的敏感操作）
3. batch_approval - 短时间批量操作（需扩展操作日志表）

使用示例：
- internal_control(check_type="dormant_account_activated")
- internal_control(check_type="off_hours_operation")
"""
    
    @property
    def input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "check_type": {
                    "type": "string",
                    "enum": ["dormant_account_activated", "off_hours_operation", "batch_approval"],
                    "description": "检查类型"
                },
                "dormant_days": {
                    "type": "integer",
                    "description": "睡眠户定义天数（默认365天）",
                    "default": 365
                }
            },
            "required": ["check_type"]
        }
    
    def execute(self, check_type: str, dormant_days: int = 365, **kwargs) -> Dict[str, Any]:
        """执行内部操作风险检查"""
        
        if check_type == "dormant_account_activated":
            return self._check_dormant_account_activated(dormant_days)
        elif check_type == "off_hours_operation":
            return self._check_off_hours_operation()
        elif check_type == "batch_approval":
            return self._check_batch_approval()
        else:
            return {
                "success": False,
                "message": f"不支持的检查类型: {check_type}"
            }
    
    def _check_dormant_account_activated(self, dormant_days: int) -> Dict[str, Any]:
        """检查睡眠户异常激活"""
        
        sql = f"""
        WITH dormant_accounts AS (
            SELECT person_id,
                   MAX(trans_date) as last_trans_date
            FROM t_journal_ledger
            GROUP BY person_id
            HAVING julianday('now') - julianday(last_trans_date) > {dormant_days}
        ),
        recent_activations AS (
            SELECT j.person_id, i.name,
                   j.trans_amount,
                   j.trans_date,
                   d.last_trans_date,
                   julianday(j.trans_date) - julianday(d.last_trans_date) as dormant_period
            FROM t_journal_ledger j
            JOIN dormant_accounts d ON j.person_id = d.person_id
            JOIN t_individual_info i ON j.person_id = i.person_id
            WHERE j.trans_date > d.last_trans_date
              AND j.trans_amount > 50000
        )
        SELECT * FROM recent_activations
        ORDER BY trans_amount DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        activations = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "睡眠户异常激活检查",
                "dormant_days_threshold": dormant_days,
                "total_activations": len(activations),
                "activations": activations[:15]
            },
            "message": f"发现 {len(activations)} 个睡眠户异常激活（沉睡>{dormant_days}天后大额提取）"
        }
    
    def _check_off_hours_operation(self) -> Dict[str, Any]:
        """检查非工作时间操作"""
        
        sql = """
        SELECT ledger_id, person_id, trans_type,
               trans_amount, trans_date,
               strftime('%H', trans_date) as hour,
               strftime('%w', trans_date) as weekday
        FROM t_journal_ledger
        WHERE (CAST(strftime('%H', trans_date) AS INTEGER) < 8 
               OR CAST(strftime('%H', trans_date) AS INTEGER) > 22)
           OR (strftime('%w', trans_date) IN ('0', '6'))
        ORDER BY trans_date DESC
        LIMIT 50
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        operations = result.get("data", [])
        
        # 分类
        night_ops = [op for op in operations if int(op.get("hour", 12)) < 8 or int(op.get("hour", 12)) > 22]
        weekend_ops = [op for op in operations if op.get("weekday") in ['0', '6']]
        
        return {
            "success": True,
            "data": {
                "check_type": "非工作时间操作检查",
                "total_operations": len(operations),
                "night_operations": len(night_ops),
                "weekend_operations": len(weekend_ops),
                "operations": operations[:20]
            },
            "message": f"发现 {len(operations)} 笔非工作时间操作（深夜 {len(night_ops)} 笔，周末 {len(weekend_ops)} 笔）"
        }
    
    def _check_batch_approval(self) -> Dict[str, Any]:
        """检查批量审批（简化版）"""
        
        # 注：实际需要操作员日志表，这里简化处理
        return {
            "success": True,
            "data": {
                "check_type": "批量审批检查",
                "note": "需要扩展操作员日志表（operator_log）才能准确检测"
            },
            "message": "此功能需要操作员日志支持，当前数据库暂不支持"
        }
