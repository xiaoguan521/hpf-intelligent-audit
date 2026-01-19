"""
提取审计 Skill
检测公积金提取业务中的骗提行为
"""
from typing import Dict, Any
from .base import BaseSkill


class WithdrawalAuditSkill(BaseSkill):
    """提取业务审计 Skill"""
    
    @property
    def name(self) -> str:
        return "withdrawal_audit"
    
    @property
    def description(self) -> str:
        return """检测公积金提取业务中的骗提行为。

支持的检查类型（check_type）：
1. frequent_purchase - 购房提取频次异常（6个月内多次购房提取）
2. remote_withdrawal - 异地购房提取异常
3. rental_large_amount - 租房提取金额异常（接近账户余额）

使用示例：
- 检查购房提取频次：withdrawal_audit(check_type="frequent_purchase")
- 检查特定人员：withdrawal_audit(check_type="frequent_purchase", person_id="P001")
"""
    
    @property
    def input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "check_type": {
                    "type": "string",
                    "enum": ["frequent_purchase", "remote_withdrawal", "rental_large_amount"],
                    "description": "检查类型"
                },
                "person_id": {
                    "type": "string",
                    "description": "可选，指定检查的个人ID"
                }
            },
            "required": ["check_type"]
        }
    
    def execute(self, check_type: str, person_id: str = None, **kwargs) -> Dict[str, Any]:
        """执行提取审计"""
        
        if check_type == "frequent_purchase":
            return self._check_frequent_purchase(person_id)
        elif check_type == "remote_withdrawal":
            return self._check_remote_withdrawal(person_id)
        elif check_type == "rental_large_amount":
            return self._check_rental_large_amount(person_id)
        else:
            return {
                "success": False,
                "message": f"不支持的检查类型: {check_type}"
            }
    
    def _check_frequent_purchase(self, person_id: str = None) -> Dict[str, Any]:
        """检查购房提取频次异常"""
        
        sql = """
        SELECT person_id, i.name, COUNT(*) as times,
               SUM(trans_amount) as total_amount
        FROM t_journal_ledger j
        JOIN t_individual_info i USING(person_id)
        WHERE trans_type = '提取'
          AND abstract LIKE '%购房%'
          AND trans_date >= DATE('now', '-6 months')
        """
        
        if person_id:
            sql += f" AND person_id = '{person_id}'"
        
        sql += """
        GROUP BY person_id
        HAVING times >= 2
        ORDER BY times DESC
        LIMIT 50
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {
                "success": False,
                "message": f"查询失败: {result['error']}"
            }
        
        risk_accounts = result.get("data", [])
        
        # 分级
        high_risk = [a for a in risk_accounts if a.get("times", 0) >= 3]
        medium_risk = [a for a in risk_accounts if a.get("times", 0) == 2]
        
        return {
            "success": True,
            "data": {
                "check_type": "购房提取频次检查",
                "total_risk_accounts": len(risk_accounts),
                "high_risk": {
                    "count": len(high_risk),
                    "accounts": high_risk[:10]  # 只返回前10个
                },
                "medium_risk": {
                    "count": len(medium_risk),
                    "accounts": medium_risk[:10]
                }
            },
            "message": f"发现 {len(risk_accounts)} 个疑似骗提账户（{len(high_risk)}个高风险，{len(medium_risk)}个中风险）"
        }
    
    def _check_remote_withdrawal(self, person_id: str = None) -> Dict[str, Any]:
        """检查异地购房提取异常"""
        
        sql = """
        SELECT j.person_id, i.name,
               json_extract(j.ext_info, '$.location') as withdraw_location,
               j.trans_amount
        FROM t_journal_ledger j
        JOIN t_individual_info i ON j.person_id = i.person_id
        WHERE j.abstract LIKE '%异地购房%'
          AND j.trans_date >= DATE('now', '-1 year')
        """
        
        if person_id:
            sql += f" AND j.person_id = '{person_id}'"
        
        sql += " LIMIT 50"
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        accounts = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "异地购房提取检查",
                "total_accounts": len(accounts),
                "accounts": accounts[:20]
            },
            "message": f"发现 {len(accounts)} 个异地购房提取记录"
        }
    
    def _check_rental_large_amount(self, person_id: str = None) -> Dict[str, Any]:
        """检查租房提取金额异常"""
        
        sql = """
        SELECT j.person_id, i.name,
               j.trans_amount,
               i.account_balance,
               ROUND(j.trans_amount * 100.0 / i.account_balance, 2) as ratio
        FROM t_journal_ledger j
        JOIN t_individual_info i ON j.person_id = i.person_id
        WHERE j.abstract LIKE '%租房%'
          AND j.trans_date >= DATE('now', '-3 months')
          AND ratio > 80
        """
        
        if person_id:
            sql += f" AND j.person_id = '{person_id}'"
        
        sql += " LIMIT 50"
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        accounts = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "租房提取金额检查",
                "total_accounts": len(accounts),
                "accounts": accounts[:20]
            },
            "message": f"发现 {len(accounts)} 个租房提取金额异常（超过余额80%）"
        }
