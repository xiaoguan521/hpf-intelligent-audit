"""
贷款合规检查 Skill
检测公积金贷款业务中的合规风险
"""
from typing import Dict, Any
from .base import BaseSkill


class LoanComplianceSkill(BaseSkill):
    """贷款业务合规检查 Skill"""
    
    @property
    def name(self) -> str:
        return "loan_compliance"
    
    @property
    def description(self) -> str:
        return """检测公积金贷款业务中的合规风险。

支持的检查类型（check_type）：
1. down_payment_ratio - 首付比例检查（是否符合首套房≥20%、二套房≥30%）
2. dti_check - 债务收入比检查（月还款/月收入是否>50%）
3. collateral_duplicate - 抵押物重复检查（一房多贷）
4. base_salary_surge - 缴存基数异常增长检查（申贷前突然大幅提高）

使用示例：
- loan_compliance(check_type="down_payment_ratio")
- loan_compliance(check_type="dti_check", threshold=0.5)
"""
    
    @property
    def input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "check_type": {
                    "type": "string",
                    "enum": ["down_payment_ratio", "dti_check", "collateral_duplicate", "base_salary_surge"],
                    "description": "检查类型"
                },
                "threshold": {
                    "type": "number",
                    "description": "阈值（用于 dti_check，默认 0.5）"
                }
            },
            "required": ["check_type"]
        }
    
    def execute(self, check_type: str, threshold: float = 0.5, **kwargs) -> Dict[str, Any]:
        """执行贷款合规检查"""
        
        if check_type == "down_payment_ratio":
            return self._check_down_payment_ratio()
        elif check_type == "dti_check":
            return self._check_dti(threshold)
        elif check_type == "collateral_duplicate":
            return self._check_collateral_duplicate()
        elif check_type == "base_salary_surge":
            return self._check_base_salary_surge()
        else:
            return {
                "success": False,
                "message": f"不支持的检查类型: {check_type}"
            }
    
    def _check_down_payment_ratio(self) -> Dict[str, Any]:
        """检查首付比例"""
        
        sql = """
        SELECT DKZH, GRZH,
               DKJE,
               FWJZ,
               ROUND(DKJE * 100.0 / FWJZ, 2) as loan_ratio,
               ROUND(100 - DKJE * 100.0 / FWJZ, 2) as down_payment_ratio
        FROM GR_DK_HT
        WHERE DKZT IN ('审批中', '已发放')
          AND loan_ratio > 80
        ORDER BY loan_ratio DESC
        LIMIT 50
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        loans = result.get("data", [])
        
        # 分级
        high_risk = [l for l in loans if l.get("down_payment_ratio", 100) < 20]  # 首付<20%
        medium_risk = [l for l in loans if 20 <= l.get("down_payment_ratio", 100) < 30]  # 首付20-30%
        
        return {
            "success": True,
            "data": {
                "check_type": "首付比例检查",
                "total_risk_loans": len(loans),
                "high_risk": {
                    "count": len(high_risk),
                    "loans": high_risk[:10]
                },
                "medium_risk": {
                    "count": len(medium_risk),
                    "loans": medium_risk[:10]
                }
            },
            "message": f"发现 {len(loans)} 笔首付比例不足（{len(high_risk)}笔<20%，{len(medium_risk)}笔20-30%）"
        }
    
    def _check_dti(self, threshold: float) -> Dict[str, Any]:
        """检查债务收入比（简化版）"""
        
        # 注：实际 DTI 计算需要月还款额，这里简化处理
        sql = """
        SELECT l.DKZH, l.GRZH, i.XINGMING,
               l.DKJE,
               i.YJCJS,
               ROUND(l.DKJE / (i.YJCJS * 12), 2) as estimated_dti
        FROM GR_DK_HT l
        JOIN GR_JC_JBXX i ON l.GRZH = i.GRZH
        WHERE l.DKZT IN ('审批中', '已发放')
          AND estimated_dti > 0.5
        ORDER BY estimated_dti DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        loans = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "债务收入比检查",
                "threshold": threshold,
                "total_risk_loans": len(loans),
                "loans": loans[:15]
            },
            "message": f"发现 {len(loans)} 笔贷款 DTI 偏高（估算值 > {threshold}）"
        }
    
    def _check_collateral_duplicate(self) -> Dict[str, Any]:
        """检查抵押物重复"""
        
        sql = """
        SELECT FWDZ,
               COUNT(*) as loan_count,
               GROUP_CONCAT(DKZH) as loan_ids,
               SUM(DKJE) as total_loan
        FROM GR_DK_HT
        WHERE DKZT IN ('审批中', '已发放')
        GROUP BY FWDZ
        HAVING loan_count > 1
        ORDER BY loan_count DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        duplicates = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "抵押物重复检查",
                "total_duplicates": len(duplicates),
                "duplicates": duplicates[:15]
            },
            "message": f"发现 {len(duplicates)} 个重复抵押物地址（疑似一房多贷）"
        }
    
    def _check_base_salary_surge(self) -> Dict[str, Any]:
        """检查缴存基数异常增长（简化版）"""
        
        # 注：实际需要历史数据表，这里简化处理
        sql = """
        SELECT l.DKZH, l.GRZH, i.XINGMING,
               i.YJCJS,
               l.SQZR
        FROM GR_DK_HT l
        JOIN GR_JC_JBXX i ON l.GRZH = i.GRZH
        WHERE l.DKZT IN ('审批中', '已发放')
          AND l.SQZR >= DATE('now', '-6 months')
          AND i.YJCJS > 15000
        ORDER BY i.YJCJS DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        accounts = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "缴存基数异常增长检查",
                "total_high_base": len(accounts),
                "accounts": accounts[:15]
            },
            "message": f"发现 {len(accounts)} 个高缴存基数账户（> 15000，需进一步核查历史变化）"
        }
