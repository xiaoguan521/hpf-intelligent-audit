"""
数据分析 Skill
对查询结果进行统计分析、趋势识别、异常检测
"""
from typing import Dict, Any, List
from .base import BaseSkill


class DataAnalysisSkill(BaseSkill):
    """数据分析 Skill"""
    
    @property
    def name(self) -> str:
        return "data_analysis"
    
    @property
    def description(self) -> str:
        return """对数据进行统计分析、趋势识别和异常检测。

支持的分析类型（analysis_type）：
1. summary_statistics - 汇总统计（计算均值、中位数、最大/最小值）
2. trend_analysis - 趋势分析（识别上升/下降趋势）
3. anomaly_detection - 异常检测（识别离群值）
4. distribution_analysis - 分布分析（分组统计）

重要：data 参数必须是实际的数据列表，不能是字符串描述。

使用示例：
- 分析账户状态分布：data_analysis(analysis_type="distribution_analysis", data=[{"status": "normal"}, {"status": "suspended"}])
- 分析数值统计：data_analysis(analysis_type="summary_statistics", data=[{"amount": 1000}, {"amount": 2000}])
- 分析简单值列表：data_analysis(analysis_type="distribution_analysis", data=["normal", "normal", "suspended"])

注意：如果你没有实际数据，请先使用其他工具获取数据，然后再进行分析。
"""
    
    @property
    def input_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "analysis_type": {
                    "type": "string",
                    "enum": ["summary_statistics", "trend_analysis", "anomaly_detection", "distribution_analysis"],
                    "description": "分析类型"
                },
                "data": {
                    "type": "array",
                    "description": "待分析的数据（字典列表）"
                },
                "field": {
                    "type": "string",
                    "description": "要分析的字段名"
                },
                "threshold": {
                    "type": "number",
                    "description": "异常检测阈值（标准差倍数，默认2.0）",
                    "default": 2.0
                }
            },
            "required": ["analysis_type", "data"]
        }
    
    def execute(self, analysis_type: str, data, field: str = None, threshold: float = 2.0, **kwargs) -> Dict[str, Any]:
        """执行数据分析"""
        
        # 处理不同类型的输入数据
        if isinstance(data, str):
            return {
                "success": False,
                "message": f"数据参数错误：期望数据列表，但收到字符串 '{data}'。请提供实际的数据列表。"
            }
        
        if not data:
            return {
                "success": False,
                "message": "数据为空，无法分析"
            }
        
        # 如果数据是简单值列表，转换为字典列表
        if data and not isinstance(data[0], dict):
            # 将简单值列表转换为字典列表
            data = [{"value": item, "index": i} for i, item in enumerate(data)]
            if not field:
                field = "value"
        
        if analysis_type == "summary_statistics":
            return self._summary_statistics(data, field)
        elif analysis_type == "trend_analysis":
            return self._trend_analysis(data, field)
        elif analysis_type == "anomaly_detection":
            return self._anomaly_detection(data, field, threshold)
        elif analysis_type == "distribution_analysis":
            return self._distribution_analysis(data, field)
        else:
            return {
                "success": False,
                "message": f"不支持的分析类型: {analysis_type}"
            }
    
    def _summary_statistics(self, data: List[Dict], field: str = None) -> Dict[str, Any]:
        """汇总统计"""
        
        if not field:
            # 自动选择第一个数值字段
            for key in data[0].keys():
                if isinstance(data[0][key], (int, float)):
                    field = key
                    break
        
        if not field:
            return {"success": False, "message": "未找到可分析的数值字段"}
        
        try:
            values = [row[field] for row in data if field in row and row[field] is not None]
            
            if not values:
                return {"success": False, "message": f"字段 {field} 无有效数据"}
            
            values_sorted = sorted(values)
            n = len(values)
            
            stats = {
                "field": field,
                "count": n,
                "sum": sum(values),
                "mean": sum(values) / n,
                "min": min(values),
                "max": max(values),
                "median": values_sorted[n//2] if n % 2 == 1 else (values_sorted[n//2-1] + values_sorted[n//2]) / 2,
                "range": max(values) - min(values)
            }
            
            # 计算标准差
            mean = stats["mean"]
            variance = sum((x - mean) ** 2 for x in values) / n
            stats["std_dev"] = variance ** 0.5
            
            return {
                "success": True,
                "data": {
                    "analysis_type": "汇总统计",
                    "statistics": stats
                },
                "message": f"已完成 {field} 字段的汇总统计分析"
            }
        except Exception as e:
            return {"success": False, "message": f"分析失败: {str(e)}"}
    
    def _trend_analysis(self, data: List[Dict], field: str = None) -> Dict[str, Any]:
        """趋势分析（简化版）"""
        
        if len(data) < 3:
            return {"success": False, "message": "数据点太少，无法识别趋势"}
        
        # 简化：比较前后半段的平均值
        mid = len(data) // 2
        first_half = data[:mid]
        second_half = data[mid:]
        
        if not field:
            for key in data[0].keys():
                if isinstance(data[0][key], (int, float)):
                    field = key
                    break
        
        try:
            first_avg = sum(row[field] for row in first_half if field in row) / len(first_half)
            second_avg = sum(row[field] for row in second_half if field in row) / len(second_half)
            
            change = second_avg - first_avg
            change_pct = (change / first_avg * 100) if first_avg != 0 else 0
            
            trend = "上升" if change > 0 else ("下降" if change < 0 else "平稳")
            
            return {
                "success": True,
                "data": {
                    "analysis_type": "趋势分析",
                    "field": field,
                    "trend": trend,
                    "first_half_avg": round(first_avg, 2),
                    "second_half_avg": round(second_avg, 2),
                    "change": round(change, 2),
                    "change_percent": round(change_pct, 2)
                },
                "message": f"{field} 呈 {trend} 趋势（变化 {change_pct:.1f}%）"
            }
        except Exception as e:
            return {"success": False, "message": f"趋势分析失败: {str(e)}"}
    
    def _anomaly_detection(self, data: List[Dict], field: str = None, threshold: float = 2.0) -> Dict[str, Any]:
        """异常检测（基于标准差）"""
        
        stats_result = self._summary_statistics(data, field)
        if not stats_result.get("success"):
            return stats_result
        
        stats = stats_result["data"]["statistics"]
        mean = stats["mean"]
        std_dev = stats["std_dev"]
        
        # 识别离群值（超过 threshold 个标准差）
        anomalies = []
        for row in data:
            if field in row and row[field] is not None:
                value = row[field]
                z_score = abs((value - mean) / std_dev) if std_dev > 0 else 0
                if z_score > threshold:
                    anomalies.append({
                        **row,
                        "z_score": round(z_score, 2),
                        "deviation": round(value - mean, 2)
                    })
        
        return {
            "success": True,
            "data": {
                "analysis_type": "异常检测",
                "field": field,
                "threshold": threshold,
                "total_anomalies": len(anomalies),
                "anomalies": anomalies[:10],  # 只返回前10个
                "baseline": {
                    "mean": round(mean, 2),
                    "std_dev": round(std_dev, 2)
                }
            },
            "message": f"发现 {len(anomalies)} 个异常值（偏离均值超过 {threshold} 个标准差）"
        }
    
    def _distribution_analysis(self, data: List[Dict], field: str = None) -> Dict[str, Any]:
        """分布分析"""
        
        if not field:
            # 选择第一个字段
            field = list(data[0].keys())[0]
        
        # 统计分布
        distribution = {}
        total_count = 0
        for row in data:
            if field in row:
                value = row[field]
                distribution[value] = distribution.get(value, 0) + 1
                total_count += 1
        
        # 排序
        sorted_dist = sorted(distribution.items(), key=lambda x: x[1], reverse=True)
        
        # 计算百分比
        dist_with_pct = []
        for value, count in sorted_dist:
            percentage = (count / total_count * 100) if total_count > 0 else 0
            dist_with_pct.append({
                "value": value,
                "count": count,
                "percentage": round(percentage, 1)
            })
        
        return {
            "success": True,
            "data": {
                "analysis_type": "分布分析",
                "field": field,
                "total_count": total_count,
                "unique_values": len(distribution),
                "distribution": dist_with_pct[:20],  # 只返回前20个
                "summary": f"共 {total_count} 条记录，{len(distribution)} 种不同值"
            },
            "message": f"{field} 字段分布：{sorted_dist[0][0]}({sorted_dist[0][1]}条) 最多，共{len(distribution)}种值"
        }
