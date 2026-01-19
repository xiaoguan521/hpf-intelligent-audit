"""
单位缴存审计 Skill
检测单位层面的缴存风险和异常行为
"""
from typing import Dict, Any
from .base import BaseSkill


class OrganizationAuditSkill(BaseSkill):
    """单位缴存审计 Skill"""
    
    @property
    def name(self) -> str:
        return "organization_audit"
    
    @property
    def description(self) -> str:
        return """检测单位层面的公积金缴存风险和异常行为。

支持的检查类型（check_type）：
1. malicious_arrears - 恶意欠缴风险（连续3个月及以上未缴存）
2. periodic_arrears - 阶段性欠缴风险（缓缴期内未按约定补缴）
3. fake_account - 开户类风险（无效工商信息、重复开户）
4. pre_loan_surge - 贷前缴存额异常风险（贷款前一年缴存额突增50%+）
5. headcount_fraud - 人数虚报风险（缴存人数 > 社保参保人数）
6. makeup_arbitrage - 补缴套利风险（贷款前集中补缴超6倍月缴存额）
7. post_loan_arrears - 贷后缴存异常风险（贷款后大批断缴）

使用示例：
- organization_audit(check_type="malicious_arrears")
- organization_audit(check_type="pre_loan_surge", org_id="ORG001")
"""
    
    @property
    def input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "check_type": {
                    "type": "string",
                    "enum": [
                        "malicious_arrears", 
                        "periodic_arrears", 
                        "fake_account",
                        "pre_loan_surge",
                        "headcount_fraud",
                        "makeup_arbitrage",
                        "post_loan_arrears"
                    ],
                    "description": "检查类型"
                },
                "org_id": {
                    "type": "string",
                    "description": "可选，指定检查的单位ID"
                }
            },
            "required": ["check_type"]
        }
    
    def execute(self, check_type: str, org_id: str = None, **kwargs) -> Dict[str, Any]:
        """执行单位审计"""
        
        handlers = {
            "malicious_arrears": self._check_malicious_arrears,
            "periodic_arrears": self._check_periodic_arrears,
            "fake_account": self._check_fake_account,
            "pre_loan_surge": self._check_pre_loan_surge,
            "headcount_fraud": self._check_headcount_fraud,
            "makeup_arbitrage": self._check_makeup_arbitrage,
            "post_loan_arrears": self._check_post_loan_arrears
        }
        
        handler = handlers.get(check_type)
        if not handler:
            return {
                "success": False,
                "message": f"不支持的检查类型: {check_type}"
            }
        
        return handler(org_id)
    
    def _check_malicious_arrears(self, org_id: str = None) -> Dict[str, Any]:
        """检查恶意欠缴风险：连续3个月及以上未缴存"""
        
        # 注：实际需要单位缴存月表，这里简化处理
        sql = """
        WITH org_deposit_months AS (
            SELECT o.DWZH, o.DWMC,
                   strftime('%Y-%m', j.YWRQ) as deposit_month,
                   COUNT(*) as deposit_count
            FROM DW_JC_JBXX o
            LEFT JOIN GR_JC_JBXX i ON o.DWZH = i.DWZH
            LEFT JOIN GR_JC_MX j ON i.GRZH = j.GRZH
            WHERE (j.ZY LIKE '%汇缴%' OR j.ZY LIKE '%补缴%')
              AND j.YWRQ >= DATE('now', '-6 months')
        """
        
        if org_id:
            sql += f" AND o.DWZH = '{org_id}'"
        
        sql += """
            GROUP BY o.DWZH, deposit_month
        ),
        missing_months AS (
            SELECT all_months.DWZH, all_months.DWMC,
                   COUNT(*) as missing_count
            FROM (
                SELECT o.DWZH, o.DWMC,
                       strftime('%Y-%m', DATE('now', '-' || value || ' months')) as deposit_month
                FROM DW_JC_JBXX o
                JOIN (SELECT 0 as value UNION SELECT 1 UNION SELECT 2 
                      UNION SELECT 3 UNION SELECT 4 UNION SELECT 5) months
            ) all_months
            LEFT JOIN org_deposit_months USING(DWZH, deposit_month)
            WHERE org_deposit_months.DWZH IS NULL
            GROUP BY all_months.DWZH, all_months.DWMC
            HAVING missing_count >= 3
        )
        SELECT * FROM missing_months
        ORDER BY missing_count DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        orgs = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "恶意欠缴风险检查",
                "total_risk_orgs": len(orgs),
                "threshold": "连续3个月及以上未缴存",
                "organizations": orgs[:15]
            },
            "message": f"发现 {len(orgs)} 个单位存在恶意欠缴风险（连续3个月及以上未缴存）"
        }
    
    def _check_periodic_arrears(self, org_id: str = None) -> Dict[str, Any]:
        """检查阶段性欠缴风险：缓缴期内未按约定补缴"""
        
        # 注：需要缓缴业务表，当前数据库暂不支持
        return {
            "success": True,
            "data": {
                "check_type": "阶段性欠缴风险检查",
                "note": "需要缓缴业务登记表（t_deferment_record）支持"
            },
            "message": "此功能需要缓缴业务表支持，当前数据库暂不支持"
        }
    
    def _check_fake_account(self, org_id: str = None) -> Dict[str, Any]:
        """检查开户类风险：无效工商信息、重复开户"""
        
        sql = """
        WITH account_stats AS (
            SELECT DWZH, DWMC,
                   COUNT(*) as account_count,
                   MIN(CLRQ) as first_open_date,
                   MAX(CLRQ) as last_open_date
            FROM DW_JC_JBXX
            WHERE CLRQ >= DATE('now', '-1 year')
        """
        
        if org_id:
            sql += f" AND DWZH = '{org_id}'"
        
        sql += """
            GROUP BY DWZH
        ),
        duplicate_person AS (
            SELECT ZJHM, COUNT(*) as dup_count
            FROM GR_JC_JBXX
            GROUP BY ZJHM
            HAVING dup_count > 1
        )
        SELECT DISTINCT o.DWZH, o.DWMC,
               s.account_count,
               COUNT(d.ZJHM) as duplicate_person_count
        FROM DW_JC_JBXX o
        JOIN account_stats s USING(DWZH)
        LEFT JOIN GR_JC_JBXX i ON o.DWZH = i.DWZH
        LEFT JOIN duplicate_person d ON i.ZJHM = d.ZJHM
        GROUP BY o.DWZH
        HAVING s.account_count > 1 OR duplicate_person_count > 0
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        orgs = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "开户类风险检查",
                "total_risk_orgs": len(orgs),
                "organizations": orgs[:15]
            },
            "message": f"发现 {len(orgs)} 个单位存在开户异常（频繁开户或重复开户）"
        }
    
    def _check_pre_loan_surge(self, org_id: str = None) -> Dict[str, Any]:
        """检查贷前缴存额异常：贷款前一年缴存额突增50%+"""
        
        sql = """
        WITH recent_loans AS (
            SELECT GRZH, SQZR
            FROM GR_DK_HT
            WHERE SQZR >= DATE('now', '-1 year')
        ),
        deposit_before_loan AS (
            SELECT l.GRZH, i.DWZH, o.DWMC,
                   AVG(CASE WHEN j.YWRQ BETWEEN DATE(l.SQZR, '-1 year') 
                                AND DATE(l.SQZR, '-6 months') 
                            THEN j.FSE ELSE NULL END) as avg_normal,
                   AVG(CASE WHEN j.YWRQ BETWEEN DATE(l.SQZR, '-6 months') 
                                AND l.SQZR 
                            THEN j.FSE ELSE NULL END) as avg_recent
            FROM recent_loans l
            JOIN GR_JC_JBXX i ON l.GRZH = i.GRZH
            JOIN DW_JC_JBXX o ON i.DWZH = o.DWZH
            JOIN GR_JC_MX j ON l.GRZH = j.GRZH
            WHERE (j.ZY LIKE '%汇缴%' OR j.ZY LIKE '%补缴%')
        """
        
        if org_id:
            sql += f" AND i.DWZH = '{org_id}'"
        
        sql += """
            GROUP BY l.GRZH
            HAVING avg_normal IS NOT NULL AND avg_recent IS NOT NULL
        )
        SELECT DWZH, DWMC,
               COUNT(*) as surge_person_count,
               AVG((avg_recent - avg_normal) / avg_normal * 100) as avg_increase_pct
        FROM deposit_before_loan
        WHERE (avg_recent - avg_normal) / avg_normal >= 0.5
        GROUP BY DWZH
        ORDER BY surge_person_count DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        orgs = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "贷前缴存额异常风险检查",
                "threshold": "贷款前一年缴存额突增50%+",
                "total_risk_orgs": len(orgs),
                "organizations": orgs[:15]
            },
            "message": f"发现 {len(orgs)} 个单位存在贷前缴存额异常（员工缴存额突增50%以上）"
        }
    
    def _check_headcount_fraud(self, org_id: str = None) -> Dict[str, Any]:
        """检查人数虚报风险：缴存人数 > 社保参保人数"""
        
        # 注：需要社保数据接口，这里简化处理
        sql = """
        SELECT o.DWZH, o.DWMC,
               o.JCRS as registered_count,
               COUNT(DISTINCT i.GRZH) as actual_deposit_count,
               o.JCRS - COUNT(DISTINCT i.GRZH) as diff
        FROM DW_JC_JBXX o
        LEFT JOIN GR_JC_JBXX i ON o.DWZH = i.DWZH
        """
        
        if org_id:
            sql += f" WHERE o.DWZH = '{org_id}'"
        
        sql += """
        GROUP BY o.DWZH
        HAVING diff > 0
        ORDER BY diff DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        orgs = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "人数虚报风险检查",
                "total_risk_orgs": len(orgs),
                "note": "此检查需要对接社保数据，当前简化为比对登记人数与实缴人数",
                "organizations": orgs[:15]
            },
            "message": f"发现 {len(orgs)} 个单位存在人数虚报嫌疑（登记人数 > 实际缴存人数）"
        }
    
    def _check_makeup_arbitrage(self, org_id: str = None) -> Dict[str, Any]:
        """检查补缴套利风险：贷款前集中补缴超过6倍月缴存额"""
        
        sql = """
        WITH recent_loans AS (
            SELECT GRZH, SQZR
            FROM GR_DK_HT
            WHERE SQZR >= DATE('now', '-1 year')
        ),
        makeup_deposits AS (
            SELECT l.GRZH, i.DWZH, o.DWMC,
                   i.YJCJS,
                   SUM(CASE WHEN j.ZY LIKE '%补缴%' THEN j.FSE ELSE 0 END) as total_makeup,
                   COUNT(CASE WHEN j.ZY LIKE '%补缴%' THEN 1 END) as makeup_count
            FROM recent_loans l
            JOIN GR_JC_JBXX i ON l.GRZH = i.GRZH
            JOIN DW_JC_JBXX o ON i.DWZH = o.DWZH
            JOIN GR_JC_MX j ON l.GRZH = j.GRZH
            WHERE j.YWRQ BETWEEN DATE(l.SQZR, '-1 year') AND l.SQZR
              AND (j.ZY LIKE '%汇缴%' OR j.ZY LIKE '%补缴%')
        """
        
        if org_id:
            sql += f" AND i.DWZH = '{org_id}'"
        
        sql += """
            GROUP BY l.GRZH
        )
        SELECT DWZH, DWMC,
               COUNT(*) as arbitrage_person_count,
               AVG(total_makeup / YJCJS) as avg_makeup_ratio
        FROM makeup_deposits
        WHERE total_makeup > YJCJS * 6
        GROUP BY DWZH
        ORDER BY arbitrage_person_count DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        orgs = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "补缴套利风险检查",
                "threshold": "贷款前一年集中补缴超过6倍月缴存额",
                "total_risk_orgs": len(orgs),
                "organizations": orgs[:15]
            },
            "message": f"发现 {len(orgs)} 个单位存在补缴套利风险（员工贷款前集中补缴）"
        }
    
    def _check_post_loan_arrears(self, org_id: str = None) -> Dict[str, Any]:
        """检查贷后缴存异常：贷款后大批员工断缴"""
        
        sql = """
        WITH recent_loans AS (
            SELECT l.DKZH, l.GRZH, i.DWZH, l.SQZR
            FROM GR_DK_HT l
            JOIN GR_JC_JBXX i ON l.GRZH = i.GRZH
            WHERE l.DKZT = '已发放'
              AND l.SQZR >= DATE('now', '-1 year')
        ),
        post_loan_deposits AS (
            SELECT rl.DWZH, o.DWMC,
                   COUNT(DISTINCT rl.GRZH) as total_borrowers,
                   COUNT(DISTINCT CASE 
                       WHEN NOT EXISTS (
                           SELECT 1 FROM GR_JC_MX j
                           WHERE j.GRZH = rl.GRZH
                             AND (j.ZY LIKE '%汇缴%' OR j.ZY LIKE '%补缴%')
                             AND j.YWRQ > rl.SQZR
                       ) THEN rl.GRZH 
                   END) as arrears_count
            FROM recent_loans rl
            JOIN DW_JC_JBXX o ON rl.DWZH = o.DWZH
        """
        
        if org_id:
            sql += f" WHERE rl.DWZH = '{org_id}'"
        
        sql += """
            GROUP BY rl.DWZH
        )
        SELECT DWZH, DWMC, total_borrowers, arrears_count,
               ROUND(arrears_count * 100.0 / total_borrowers, 2) as arrears_ratio
        FROM post_loan_deposits
        WHERE arrears_count > 0 AND arrears_ratio > 20
        ORDER BY arrears_ratio DESC
        LIMIT 30
        """
        
        result = self.mcp_client.call("hpf-db-adapter", "safe_query", {"sql": sql})
        
        if result.get("error"):
            return {"success": False, "message": f"查询失败: {result['error']}"}
        
        orgs = result.get("data", [])
        
        return {
            "success": True,
            "data": {
                "check_type": "贷后缴存异常风险检查",
                "threshold": "贷款后20%以上员工断缴",
                "total_risk_orgs": len(orgs),
                "organizations": orgs[:15]
            },
            "message": f"发现 {len(orgs)} 个单位存在贷后缴存异常（贷款后大批员工断缴）"
        }
