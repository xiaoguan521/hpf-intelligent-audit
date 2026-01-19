import { useState } from 'react'
import { Card, Input, Button, List, Tag, Space, Switch, Divider, Spin } from 'antd'
import { SendOutlined, RobotOutlined, UserOutlined } from '@ant-design/icons'
import ReactMarkdown from 'react-markdown'
import { agentApi, ChatResponse } from '../api/client'

const { TextArea } = Input

interface Message {
    role: 'user' | 'assistant'
    content: string
    reasoning?: ChatResponse['reasoning_chain']
    agentType?: string
}

export default function AgentPage() {
    const [messages, setMessages] = useState<Message[]>([])
    const [input, setInput] = useState('')
    const [loading, setLoading] = useState(false)
    const [useLangGraph, setUseLangGraph] = useState(true)

    const handleSend = async () => {
        if (!input.trim() || loading) return

        const userMessage: Message = { role: 'user', content: input }
        setMessages(prev => [...prev, userMessage])
        setInput('')
        setLoading(true)

        try {
            const { data } = await agentApi.chat({
                query: input,
                use_langgraph: useLangGraph
            })

            const assistantMessage: Message = {
                role: 'assistant',
                content: data.answer,
                reasoning: data.reasoning_chain,
                agentType: data.agent_type
            }

            setMessages(prev => [...prev, assistantMessage])
        } catch (error: any) {
            const errorMessage: Message = {
                role: 'assistant',
                content: `执行出错: ${error.response?.data?.detail || error.message}`
            }
            setMessages(prev => [...prev, errorMessage])
        } finally {
            setLoading(false)
        }
    }

    return (
        <div>
            <Card
                title={
                    <Space>
                        <RobotOutlined /> Agent 智能对话
                        <Divider type="vertical" />
                        <span style={{ fontSize: 14, fontWeight: 'normal' }}>
                            引擎:
                            <Switch
                                checked={useLangGraph}
                                onChange={setUseLangGraph}
                                checkedChildren="LangGraph"
                                unCheckedChildren="ReAct"
                                style={{ marginLeft: 8 }}
                            />
                        </span>
                    </Space>
                }
                style={{ height: 'calc(100vh - 200px)', display: 'flex', flexDirection: 'column' }}
                bodyStyle={{ flex: 1, overflow: 'auto', display: 'flex', flexDirection: 'column' }}
            >
                <List
                    style={{ flex: 1, overflow: 'auto', marginBottom: 16 }}
                    dataSource={messages}
                    renderItem={(msg) => (
                        <List.Item style={{ border: 'none', padding: '12px 0' }}>
                            <Space align="start" style={{ width: '100%' }}>
                                {msg.role === 'user' ? (
                                    <UserOutlined style={{ fontSize: 24, color: '#1890ff' }} />
                                ) : (
                                    <RobotOutlined style={{ fontSize: 24, color: '#52c41a' }} />
                                )}
                                <div style={{ flex: 1 }}>
                                    <div style={{ marginBottom: 8 }}>
                                        <Tag color={msg.role === 'user' ? 'blue' : 'green'}>
                                            {msg.role === 'user' ? '用户' : 'Agent'}
                                        </Tag>
                                        {msg.agentType && (
                                            <Tag color="purple">{msg.agentType}</Tag>
                                        )}
                                    </div>
                                    <ReactMarkdown>{msg.content}</ReactMarkdown>

                                    {msg.reasoning && msg.reasoning.length > 0 && (
                                        <details style={{ marginTop: 12, fontSize: 12, color: '#666' }}>
                                            <summary style={{ cursor: 'pointer' }}>
                                                推理链 ({msg.reasoning.length}步)
                                            </summary>
                                            <pre style={{ background: '#f5f5f5', padding: 8, borderRadius: 4, marginTop: 8 }}>
                                                {JSON.stringify(msg.reasoning, null, 2)}
                                            </pre>
                                        </details>
                                    )}
                                </div>
                            </Space>
                        </List.Item>
                    )}
                />

                <Space.Compact style={{ width: '100%' }}>
                    <TextArea
                        value={input}
                        onChange={(e) => setInput(e.target.value)}
                        placeholder="输入问题，例如: 检查购房提取频次异常"
                        autoSize={{ minRows: 2, maxRows: 4 }}
                        onPressEnter={(e) => {
                            if (!e.shiftKey) {
                                e.preventDefault()
                                handleSend()
                            }
                        }}
                    />
                    <Button
                        type="primary"
                        icon={<SendOutlined />}
                        loading={loading}
                        onClick={handleSend}
                        style={{ height: 'auto' }}
                    >
                        发送
                    </Button>
                </Space.Compact>

                {loading && (
                    <div style={{ textAlign: 'center', marginTop: 12 }}>
                        <Spin tip="Agent思考中..." />
                    </div>
                )}
            </Card>
        </div>
    )
}
