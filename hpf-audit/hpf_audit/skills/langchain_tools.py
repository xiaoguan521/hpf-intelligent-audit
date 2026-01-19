"""
将现有Skills包装为LangChain Tools
"""
from langchain_core.tools import tool
from typing import Dict


@tool
def withdrawal_audit(check_type: str) -> Dict:
    """
    提取审计工具
    
    检查提取业务的异常风险，包括骗提、频繁提取等。
    
    Args:
        check_type: 检查类型
            - frequent_purchase: 频繁购房提取
            - large_amount: 大额异常提取
            - retired_withdrawal: 退休提取异常
    
    Returns:
        审计结果字典，包含success, data, message
    """
    from hpf_audit.skills.withdrawal_audit import WithdrawalAuditSkill
    skill = WithdrawalAuditSkill()
    return skill.execute(check_type=check_type)


@tool
def loan_compliance(check_type: str) -> Dict:
    """
    贷款合规审计工具
    
    检查贷款业务的合规性问题。
    
    Args:
        check_type: 检查类型
            - down_payment: 首付比例检查
            - dti_check: DTI债务收入比检查
            - mortgage_check: 抵押物检查
    """
    from hpf_audit.skills.loan_compliance import LoanComplianceSkill
    skill = LoanComplianceSkill()
    return skill.execute(check_type=check_type)


@tool
def internal_control_audit(check_type: str) -> Dict:
    """
    内控审计工具
    
    检查内部控制相关风险。
    
    Args:
        check_type: 检查类型
            - dormant_reactivation: 睡眠户激活
            - after_hours: 非工作时间操作
            - permission_abuse: 权限滥用
    """
    from hpf_audit.skills.internal_control import InternalControlSkill
    skill = InternalControlSkill()
    return skill.execute(check_type=check_type)


@tool
def organization_audit(check_type: str) -> Dict:
    """
    单位审计工具
    
    检查缴存单位相关风险。
    
    Args:
        check_type: 检查类型
            - malicious_arrears: 恶意欠缴
            - fake_accounts: 虚假开户
            - subsidy_arbitrage: 补缴套利
    """
    from hpf_audit.skills.organization_audit import OrganizationAuditSkill
    skill = OrganizationAuditSkill()
    return skill.execute(check_type=check_type)


@tool
def data_analysis(query: str) -> Dict:
    """
    数据分析工具
    
    执行统计分析、趋势分析等数据查询。
    
    Args:
        query: 分析需求描述
    """
    from hpf_audit.skills.data_analysis import DataAnalysisSkill
    skill = DataAnalysisSkill()
    return skill.execute(query=query)


# 导出所有工具
ALL_TOOLS = [
    withdrawal_audit,
    loan_compliance,
    internal_control_audit,
    organization_audit,
    data_analysis
]
