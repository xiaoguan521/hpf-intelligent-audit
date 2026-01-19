"""
ReAct Agent 核心引擎
实现基于 Reasoning + Acting 框架的多步推理
"""
import json
import re
from typing import List, Dict, Any, Optional
from hpf_audit.skills.base import BaseSkill


class ReActAgent:
    """ReAct Agent 引擎"""
    
    def __init__(
        self,
        llm_client,
        skills: List[BaseSkill],
        max_iterations: int = 5,
        verbose: bool = True,
        schema_context: str = "",  # 数据库 Schema 上下文
        db_path: str = "./housing_provident_fund.db"  # 新增：数据库路径
    ):
        """
        初始化 ReAct Agent
        
        Args:
            llm_client: LLM 客户端（支持 generate 方法）
            skills: 可用的 Skills 列表
            max_iterations: 最大推理轮数
            verbose: 是否打印推理过程
            schema_context: 数据库 Schema 描述（可选）
            db_path: 数据库路径（用于向量检索）
        """
        self.llm = llm_client
        self.skills = {skill.name: skill for skill in skills}
        self.max_iterations = max_iterations
        self.verbose = verbose
        self.schema_context = schema_context
        
        # 新增：初始化向量检索器（用于 Skill 语义检索）
        self.retriever = None
        try:
            from hpf_audit.skills.vector_retriever import VectorRetriever
            self.retriever = VectorRetriever(db_path)
            if self.verbose:
                print(f"✅ Skill 语义检索已启用")
        except Exception as e:
            if self.verbose:
                print(f"⚠️ Skill 语义检索不可用: {e}")

    
    def run(self, user_query: str) -> Dict[str, Any]:
        """
        执行 ReAct 推理循环
        
        Args:
            user_query: 用户问题
        
        Returns:
            {
                "answer": 最终答案,
                "reasoning_chain": 推理链,
                "iterations": 实际迭代次数
            }
        """
        reasoning_chain = []
        tool_call_history = []  # 记录工具调用历史 [(tool_name, tool_input), ...]
        
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"用户问题: {user_query}")
            print(f"{'='*60}\n")
            
        # 0. 预先检索相关 Skills (只检索一次)
        relevant_skills = self._find_relevant_skills(user_query, top_k=3)
        
        for i in range(self.max_iterations):
            if self.verbose:
                print(f"--- 第 {i+1} 轮推理 ---")
            
            # 1. 构建 Prompt
            prompt = self._build_prompt(user_query, reasoning_chain, relevant_skills)
            
            # 打印 Prompt（在 verbose 模式下）
            if self.verbose:
                print(f"📝 构建的 Prompt (前500字符):")
                print(prompt[:500] + "..." if len(prompt) > 500 else prompt)
                print()
            
            # 2. LLM 推理
            response = self.llm.generate(prompt)
            
            if self.verbose:
                print(f"LLM 响应:\n{response}\n")
            
            # 3. 解析响应
            parsed = self._parse_response(response)
            
            # 4. 判断是否得出最终答案
            if parsed["type"] == "final_answer":
                reasoning_chain.append({
                    "iteration": i + 1,
                    "thought": parsed.get("thought", ""),
                    "type": "final_answer",
                    "answer": parsed["content"]
                })
                
                return {
                    "answer": parsed["content"],
                    "reasoning_chain": reasoning_chain,
                    "iterations": i + 1
                }
            
            # 5. 执行工具调用
            if parsed["type"] == "action":
                tool_name = parsed["tool"]
                tool_input = parsed["input"]
                
                # 检测重复调用
                current_call = (tool_name, json.dumps(tool_input, sort_keys=True))
                if current_call in tool_call_history:
                    # 检测到重复调用，强制给出最终答案
                    summary = self._generate_summary_from_history(reasoning_chain, user_query)
                    reasoning_chain.append({
                        "iteration": i + 1,
                        "thought": f"检测到重复调用工具 {tool_name}，根据已收集的信息给出最终答案",
                        "type": "final_answer",
                        "answer": summary
                    })
                    
                    if self.verbose:
                        print(f"⚠️ 检测到重复调用，自动生成最终答案\n")
                    
                    return {
                        "answer": summary,
                        "reasoning_chain": reasoning_chain,
                        "iterations": i + 1,
                        "recommended_skills": relevant_skills
                    }
                
                tool_call_history.append(current_call)
                
                observation = self._execute_tool(tool_name, tool_input)
                
                reasoning_chain.append({
                    "iteration": i + 1,
                    "thought": parsed.get("thought", ""),
                    "type": "action",
                    "action": tool_name,
                    "action_input": tool_input,
                    "observation": observation
                })
                
                if self.verbose:
                    print(f"工具执行结果:\n{json.dumps(observation, ensure_ascii=False, indent=2)}\n")
            else:
                # 解析失败，记录并继续
                reasoning_chain.append({
                    "iteration": i + 1,
                    "type": "parse_error",
                    "raw_response": response
                })
        
        # 达到最大迭代次数，自动生成总结
        summary = self._generate_summary_from_history(reasoning_chain, user_query)
        return {
            "answer": summary,
            "reasoning_chain": reasoning_chain,
            "iterations": self.max_iterations
        }

    
    def run_stream(self, user_query: str):
        """
        执行 ReAct 推理循环（流式版本）
        每轮推理后 yield 结果
        
        Args:
            user_query: 用户问题
        
        Yields:
            每轮推理的结果字典
        """
        reasoning_chain = []
        
        if self.verbose:
            print(f"\n{'='*60}")
            print(f"用户问题: {user_query}")
            print(f"{'='*60}\n")
        
        # 发送开始事件
        yield {
            "type": "start",
            "query": user_query,
            "max_iterations": self.max_iterations
        }
        
        # 0. 预先检索相关 Skills
        relevant_skills = self._find_relevant_skills(user_query, top_k=3)
        
        # 建立 Skill ID -> Score 的映射
        skill_scores = {}
        if relevant_skills:
            for skill in relevant_skills:
                if skill.get("skill_id"):
                    skill_scores[skill["skill_id"]] = skill["score"]
        
        # 发送推荐 Skills 事件
        if relevant_skills:
            yield {
                "type": "recommended_skills",
                "skills": relevant_skills
            }
        
        for i in range(self.max_iterations):
            if self.verbose:
                print(f"--- 第 {i+1} 轮推理 ---")
            
            # 发送推理开始事件
            yield {
                "type": "iteration_start",
                "iteration": i + 1
            }
            
            # 1. 构建 Prompt
            prompt = self._build_prompt(user_query, reasoning_chain, relevant_skills)
            
            # 打印 Prompt（在 verbose 模式下）
            if self.verbose:
                print(f"📝 构建的 Prompt (前500字符):")
                print(prompt[:500] + "..." if len(prompt) > 500 else prompt)
                print()
            
            # 2. LLM 推理
            response = self.llm.generate(prompt)
            
            if self.verbose:
                print(f"LLM 响应:\n{response}\n")
            
            # 3. 解析响应
            parsed = self._parse_response(response)
            
            # 4. 判断是否得出最终答案
            if parsed["type"] == "final_answer":
                step = {
                    "iteration": i + 1,
                    "thought": parsed.get("thought", ""),
                    "type": "final_answer",
                    "answer": parsed["content"]
                }
                reasoning_chain.append(step)
                
                # 发送最终答案
                yield {
                    "type": "final_answer",
                    "data": step
                }
                
                # 发送完成事件
                yield {
                    "type": "complete",
                    "answer": parsed["content"],
                    "reasoning_chain": reasoning_chain,
                    "iterations": i + 1
                }
                return
            
            # 5. 执行工具调用
            if parsed["type"] == "action":
                tool_name = parsed["tool"]
                tool_input = parsed["input"]
                
                # 获取该工具的推荐分数（如果有）
                tool_score = skill_scores.get(tool_name)
                
                # 发送工具调用开始
                yield {
                    "type": "tool_call_start",
                    "iteration": i + 1,
                    "tool": tool_name,
                    "score": tool_score,  # 添加分数
                    "input": tool_input
                }
                
                observation = self._execute_tool(tool_name, tool_input)
                
                step = {
                    "iteration": i + 1,
                    "thought": parsed.get("thought", ""),
                    "type": "action",
                    "action": tool_name,
                    "score": tool_score,  # 添加分数到 step
                    "action_input": tool_input,
                    "observation": observation
                }
                reasoning_chain.append(step)
                
                if self.verbose:
                    print(f"工具执行结果:\n{json.dumps(observation, ensure_ascii=False, indent=2)}\n")
                
                # 发送工具执行结果
                yield {
                    "type": "tool_call_complete",
                    "data": step
                }
            else:
                # 解析失败
                step = {
                    "iteration": i + 1,
                    "type": "parse_error",
                    "raw_response": response
                }
                reasoning_chain.append(step)
                
                yield {
                    "type": "error",
                    "data": step
                }
        
        # 达到最大迭代次数
        final_result = {
            "answer": "抱歉，经过多轮推理仍未得出结论。请尝试简化问题或提供更多信息。",
            "reasoning_chain": reasoning_chain,
            "iterations": self.max_iterations
        }
        
        yield {
            "type": "complete",
            **final_result
        }

    
    
    def _build_prompt(self, user_query: str, history: List[Dict], recommended_skills_list: List[Dict] = None) -> str:
        """构建 Prompt，使用语义检索推荐 Skill"""
        
        # 工具描述
        tools_desc = self._format_tools_description()
        
        # ✨ 新增：语义检索推荐 Skills
        recommended_skills = ""
        # relevant = self._find_relevant_skills(user_query, top_k=3)  <- 移除内部调用
        if recommended_skills_list:
            recommended_skills = "\n**推荐使用以下 Skills**（根据问题语义相似度排序）:\n"
            for i, skill in enumerate(recommended_skills_list, 1):
                # ✨ 如果是关联技能，添加标记
                tag = "[关联推荐] " if skill.get("is_related") else ""
                score_display = "相关" if skill.get("is_related") else f"相似度: {skill['score']:.2f}"
                
                recommended_skills += f"{i}. **{tag}{skill['name']}** ({score_display})\n"
                recommended_skills += f"   ID: `{skill['skill_id']}`\n"
                recommended_skills += f"   功能: {skill['description']}\n\n"
        
        # 历史记录
        history_text = self._format_history(history)
        
        # 计算当前轮次
        current_iteration = len(history) + 1
        max_iterations = self.max_iterations
        
        prompt = f"""你是公积金业务审计专家。请按照 ReAct 框架思考和行动。

可用工具：
{tools_desc}
{recommended_skills}
用户问题：{user_query}

**重要说明**：
1. **优先使用推荐的 Skills**：上方列出的 Skills 是根据问题语义匹配度推荐的，优先考虑使用
2. **数据查询流程**：
   - 如果需要统计分析数据（如"有多少个正常账户"），必须先用 safe_query 查询实际数据
   - 然后用 data_analysis 分析查询结果
   - 例如：查询正常账户 → safe_query("SELECT deposit_status FROM t_individual_info") → data_analysis(分析结果)

2. **错误处理**：
   - 如果工具执行失败，仔细阅读错误信息并调整策略
   - 常见错误：参数类型错误、SQL语法错误、数据格式不匹配
   - 遇到错误时，解释错误原因并尝试修正，或向用户说明问题

3. **工具使用规则**：
   - data_analysis 工具需要实际数据，不能传递字符串描述或字段名
   - safe_query 用于从数据库获取数据，支持 SQLite 语法
   - 其他审计工具用于特定的风险检查

4. **常见问题类型映射**：
   - "有多少个XX" → 先 safe_query 统计，再 data_analysis 分析
   - "分析某人/某账户" → 调用 withdrawal_audit 或 loan_compliance
   - "检查购房/贷款/提取" → 对应的审计工具
   - "查找异常" → 相关检查工具

5. **何时给出最终答案（CRITICAL）**：
   - 当你已经收集了足够的信息来回答用户问题时，必须立即输出 FinalAnswer
   - 对于统计问题：完成数据查询和分析后就应该给出结论
   - 对于"分析某人"的问题：完成 2-3 个主要维度的检查后就应该给出结论
   - 如果遇到无法解决的错误，向用户说明问题并给出建议
   - 不要重复调用相同的工具检查已经检查过的内容
   - 当前已进行 {current_iteration}/{max_iterations} 轮推理，请注意推理效率

6. **严格的输出格式**：

如果需要调用工具：
Thought: [你的思考过程，说明为什么选择这个工具]
Action: [工具名称]
ActionInput: {{"param": "value"}}

如果已有足够信息回答问题（必须严格按此格式）：
Thought: [总结已收集的信息，说明为什么现在可以给出答案]
FinalAnswer: [你的最终答案，包含完整的分析结论]

**数据库表结构参考**：
- GR_JC_JBXX: 个人账户信息（GRJCZT: 缴存状态）
- GR_JC_MX: 业务流水明细
- GR_DK_HT: 贷款申请信息
- DW_JC_JBXX: 单位基础信息

历史记录：
{history_text}

现在开始推理（第 {current_iteration} 轮）："""
        
        return prompt

    
    def _find_relevant_skills(self, query: str, top_k: int = 3) -> List[Dict]:
        """
        通过语义检索找到相关 Skill
        
        Args:
            query: 用户查询
            top_k: 返回数量
        
        Returns:
            [
                {
                    "skill_id": "逾期_贷款_监测_a1b2",
                    "name": "逾期贷款风险监测",
                    "description": "...",
                    "score": 0.85,
                    "metadata": {...}
                },
                ...
            ]
        """
        if not self.retriever:
            return []
        
        try:
            import json
            # 检索 skill_catalog 分类
            # 检索 skill_catalog 分类
            skill_hits = self.retriever.search(
                query, 
                top_k=top_k, 
                filter={"category": "skill_catalog"}
            )
            
            if self.verbose:
                print(f"DEBUG: VectorRetriever returned {len(skill_hits)} hits for query '{query}'")
            
            
            results = []
            for hit in skill_hits:
                try:
                    metadata_str = hit.get('metadata', '{}') or '{}'
                    # Handle case where metadata might already be a dict
                    if isinstance(metadata_str, dict):
                        metadata = metadata_str
                    else:
                        metadata = json.loads(metadata_str)
                        
                    if self.verbose:
                        print(f"DEBUG: Processing hit: {hit.get('title')} (Score: {hit.get('score')})")
                        
                    results.append({
                        "skill_id": metadata.get('skill_id'),
                        "name": hit['title'],
                        "description": hit['content'][:150],
                        "score": hit['score'],
                        "is_related": metadata.get('is_related', False), # ✨ 传递关联标记
                        "metadata": metadata
                    })
                except Exception as e:
                    if self.verbose:
                        print(f"DEBUG: Error parsing hit metadata: {e}")
                    continue
            
            return results
        except Exception as e:
            if self.verbose:
                print(f"⚠️ Skill 语义检索失败: {e}")
            return []
    
    def _format_tools_description(self) -> str:
        """格式化工具描述"""
        desc_list = []
        for name, skill in self.skills.items():
            desc_list.append(f"- {name}: {skill.description}")
        return "\n".join(desc_list)
    
    def _format_history(self, history: List[Dict]) -> str:
        """格式化历史记录"""
        if not history:
            return "(无)"
        
        lines = []
        for step in history:
            lines.append(f"第{step['iteration']}轮:")
            if step["type"] == "action":
                lines.append(f"  Thought: {step['thought']}")
                lines.append(f"  Action: {step['action']}")
                lines.append(f"  Observation: {json.dumps(step['observation'], ensure_ascii=False)}")
            lines.append("")
        
        return "\n".join(lines)
    
    def _parse_response(self, response: str) -> Dict[str, Any]:
        """
        解析 LLM 响应
        """
        result = {
            "type": "unknown",
            "thought": "",
            "raw": response
        }
        
        lines = response.strip().split('\n')
        
        # 1. 提取 Thought
        for line in lines:
            if line.strip().startswith("Thought:"):
                result["thought"] = line.replace("Thought:", "").strip()
                break
        
        # 2. 提取 FinalAnswer
        if "FinalAnswer:" in response:
            result["type"] = "final_answer"
            # 找到 FinalAnswer 之后的所有内容
            try:
                content = response.split("FinalAnswer:", 1)[1].strip()
                result["content"] = content
            except IndexError:
                result["content"] = ""
            return result
            
        # 3. 提取 Action 和 ActionInput
        action_match = re.search(r"Action:\s*(.+)", response)
        if action_match:
            result["type"] = "action"
            result["tool"] = action_match.group(1).strip()
            
            # 尝试提取 ActionInput，支持多行和 markdown
            # 先找 ActionInput: 标记
            input_start_match = re.search(r"ActionInput:\s*(.*)", response, re.DOTALL)
            input_data = {}
            
            if input_start_match:
                raw_input = input_start_match.group(1).strip()
                
                # 情况A: ```json ... ``` 包裹
                json_block_match = re.search(r"```(?:json)?\s*(.*?)\s*```", raw_input, re.DOTALL)
                if json_block_match:
                    json_str = json_block_match.group(1).strip()
                    try:
                        input_data = json.loads(json_str)
                    except:
                        pass
                
                # 情况B: 直接是 JSON 字符串，可能跨行
                if not input_data:
                    try:
                        # 尝试直接解析剩余部分
                        input_data = json.loads(raw_input)
                    except json.JSONDecodeError:
                        # 尝试逐行累加解析（处理只有部分是 JSON 的情况）
                        current_json = ""
                        for char in raw_input:
                            current_json += char
                            try:
                                if char == '}': # 有可能是结尾
                                    input_data = json.loads(current_json)
                                    break
                            except:
                                continue
            
            # 如果实在解析不出来，且 raw_input 看起来像字典字符串（单引号）
            if not input_data and input_start_match:
                try:
                    import ast
                    # 危险操作，但在受控环境下作为 fallback
                    val = ast.literal_eval(raw_input.split('\n')[0]) 
                    if isinstance(val, dict):
                        input_data = val
                except:
                    pass

            result["input"] = input_data
            return result
        
        # 4. 隐式 FinalAnswer 检测
        summary_keywords = [
            '综合评估', '风险评估总结', '分析结论', '审计结论', 
            '最终结论', '总结如下', '评估如下', '已完成', '根据上述'
        ]
        if any(kw in response.lower() for kw in summary_keywords):
            result["type"] = "final_answer"
            # 去掉 Thought 部分
            if "Thought:" in response:
                result["content"] = response.split("Thought:")[-1].split('\n', 1)[-1].strip()
            else:
                result["content"] = response
            return result
            
        return result
    
    def _execute_tool(self, tool_name: str, tool_input: Dict) -> Dict[str, Any]:
        """执行工具"""
        if tool_name not in self.skills:
            error_result = {
                "success": False,
                "error": f"工具 '{tool_name}' 不存在",
                "available_tools": list(self.skills.keys()),
                "message": f"❌ 错误：工具 '{tool_name}' 不存在。可用工具：{', '.join(list(self.skills.keys()))}"
            }
            if self.verbose:
                print(f"❌ 工具错误: {error_result['message']}")
            return error_result
        
        try:
            skill = self.skills[tool_name]
            result = skill.execute(**tool_input)
            
            # 确保结果包含必要的字段
            if not isinstance(result, dict):
                result = {"success": False, "error": "工具返回格式错误", "raw_result": result}
            
            # 如果工具执行失败，增强错误信息
            if not result.get("success", True):  # 默认认为成功，除非明确标记失败
                if self.verbose:
                    error_msg = result.get("message") or result.get("error", "未知错误")
                    print(f"❌ 工具执行失败: {tool_name} - {error_msg}")
            
            return result
        except Exception as e:
            import traceback
            error_detail = traceback.format_exc()
            
            error_result = {
                "success": False,
                "error": f"工具执行异常: {str(e)}",
                "tool": tool_name,
                "input": tool_input,
                "traceback": error_detail,
                "message": f"❌ 工具 '{tool_name}' 执行时发生异常：{str(e)}"
            }
            
            if self.verbose:
                print(f"❌ 工具异常: {tool_name}")
                print(f"   输入参数: {tool_input}")
                print(f"   异常信息: {str(e)}")
                print(f"   详细堆栈:\n{error_detail}")
            
            return error_result
    
    def _generate_summary_from_history(self, history: List[Dict], user_query: str) -> str:
        """
        根据推理历史自动生成总结
        用于在达到最大迭代次数或检测到重复调用时给出最终答案
        """
        if not history:
            return "未收集到任何信息，无法回答问题。"
        
        # 提取所有成功的工具调用结果
        findings = []
        for step in history:
            if step.get("type") == "action" and "observation" in step:
                obs = step["observation"]
                tool = step["action"]
                
                # 提取关键信息
                if obs.get("success"):
                    data = obs.get("data", {})
                    message = obs.get("message", "")
                    
                    # 根据不同工具类型提取信息
                    if "check_type" in data:
                        check_type = data.get("check_type", tool)
                        
                        # 检查是否有风险发现
                        total_risk = (
                            data.get("total_risk_accounts", 0) or 
                            data.get("total_risk_loans", 0) or
                            data.get("total_activations", 0) or
                            data.get("total_operations", 0) or
                            0
                        )
                        
                        if total_risk > 0:
                            findings.append(f"✗ {check_type}: {message}")
                        else:
                            findings.append(f"✓ {check_type}: 未发现异常")
        
        if not findings:
            return "经过多轮检查，未发现明显异常风险。"
        
        # 生成格式化的总结
        summary = f"根据对「{user_query}」的审计分析，结果如下：\n\n"
        
        # 分类显示发现的问题和正常项
        risks = [f for f in findings if f.startswith("✗")]
        normals = [f for f in findings if f.startswith("✓")]
        
        if risks:
            summary += "**发现的风险**：\n"
            for r in risks:
                summary += f"  {r}\n"
            summary += "\n"
        
        if normals:
            summary += "**正常检查项**：\n"
            for n in normals:
                summary += f"  {n}\n"
        
        if not risks:
            summary += "\n**综合评估**：所有检查项均正常，未发现风险。"
        else:
            summary += "\n**综合评估**：发现部分风险项，建议进一步核查。"
        
        return summary


# 兼容旧代码的别名已移除，请直接使用 LLMClient
