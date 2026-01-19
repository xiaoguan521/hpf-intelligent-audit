import { useState, useEffect } from 'react'
import {
    Layout, Card, Tabs, Button, Input, List, Tag,
    Space, Typography, Modal, Form, Select, message,
    Drawer, Tooltip, Badge
} from 'antd'
import {
    PlusOutlined, SearchOutlined, ReloadOutlined,
    RocketOutlined, CloudServerOutlined, RobotOutlined,
    CodeOutlined, DeleteOutlined, PlayCircleOutlined
} from '@ant-design/icons'
import { skillsApi, agentApi, Skill } from '../api/client'

const { TabPane } = Tabs
const { TextArea } = Input
const { Title, Text, Paragraph } = Typography

export default function SkillsPage() {
    // State
    const [skills, setSkills] = useState<Skill[]>([])
    const [loading, setLoading] = useState(false)
    const [currentTab, setCurrentTab] = useState('all')
    const [searchText, setSearchText] = useState('')

    // Create Modal State
    const [createModalVisible, setCreateModalVisible] = useState(false)
    const [createLoading, setCreateLoading] = useState(false)
    const [createMode, setCreateMode] = useState<'ai' | 'manual'>('ai')

    // Detail/Edit Drawer State
    const [drawerVisible, setDrawerVisible] = useState(false)
    const [currentSkill, setCurrentSkill] = useState<Skill | null>(null)
    const [detailTab, setDetailTab] = useState('info')
    const [testParams, setTestParams] = useState('{}')
    const [testResult, setTestResult] = useState<any>(null)
    const [testLoading, setTestLoading] = useState(false)

    // Forms
    const [aiForm] = Form.useForm()
    const [manualForm] = Form.useForm()
    const [editForm] = Form.useForm()

    // --- Data Fetching ---
    const fetchSkills = async () => {
        setLoading(true)
        try {
            const status = currentTab === 'all' ? undefined : currentTab
            const res = await skillsApi.list({ status, page_size: 100 })
            setSkills(res.data.skills)
        } catch (error) {
            message.error('加载 Skills 失败')
            console.error(error)
        } finally {
            setLoading(false)
        }
    }

    useEffect(() => {
        fetchSkills()
    }, [currentTab])

    // --- Actions ---
    const handleDelete = async (id: string) => {
        Modal.confirm({
            title: '确认删除',
            content: '确定要删除这个 Skill 吗？此操作不可恢复。',
            okType: 'danger',
            onOk: async () => {
                try {
                    await skillsApi.delete(id)
                    message.success('删除成功')
                    fetchSkills()
                    setDrawerVisible(false)
                } catch (error) {
                    message.error('删除失败')
                }
            }
        })
    }

    const handleToggleStatus = async (skill: Skill) => {
        try {
            if (skill.is_active === 1) {
                await skillsApi.unpublish(skill.skill_id)
                message.success('已下线 (Shadow Mode)')
            } else {
                await skillsApi.publish(skill.skill_id)
                message.success('已发布激活')
            }
            fetchSkills()
            // 如果在详情页，也要更新 currentSkill
            if (currentSkill && currentSkill.skill_id === skill.skill_id) {
                const updated = { ...skill, is_active: skill.is_active === 1 ? 0 : 1 }
                setCurrentSkill(updated)
                editForm.setFieldsValue(updated)
            }
        } catch (error) {
            message.error('状态更新失败')
        }
    }

    // --- Create Logic ---
    const handleCreateAI = async () => {
        try {
            const values = await aiForm.validateFields()
            setCreateLoading(true)
            message.loading({ content: 'AI 正在生成配置...', key: 'generate' })

            const res = await agentApi.generateSkill(values.requirement, values.mode)

            if (res.data.success) {
                message.success({ content: '生成成功！', key: 'generate' })
                setCreateModalVisible(false)
                aiForm.resetFields()
                fetchSkills()
            } else {
                message.error({ content: `生成失败: ${res.data.error} `, key: 'generate' })
            }
        } catch (error) {
            console.error(error)
        } finally {
            setCreateLoading(false)
        }
    }

    const handleCreateManual = async () => {
        try {
            const values = await manualForm.validateFields()
            setCreateLoading(true)

            // Extract Skill ID from YAML (simple match) or require explicit input?
            // The API expects skill_id in the body.
            // For simplicity, let's parse YAML simply here to get ID, or ask user to ensure it's in YAML
            // Actually API `save_manual` needs explicit ID. 
            // Let's assume the user puts everything in YAML and we parse it, 
            // OR we add an ID field. Let's try to parse ID from YAML first.

            // Quick regex to find skill_id: xxx
            const match = values.configuration.match(/^skill_id:\s*(.+)$/m)
            if (!match) {
                message.error('无法从 YAML 中识别 skill_id，请确保第一行是 skill_id: your_id')
                setCreateLoading(false)
                return
            }
            const skill_id = match[1].trim()

            const payload = {
                skill_id,
                configuration: values.configuration,
                is_active: values.mode === 'active' ? 1 : 0
            }

            const res = await skillsApi.saveManual(payload)
            if (res.data.success) {
                message.success('保存成功')
                setCreateModalVisible(false)
                manualForm.resetFields()
                fetchSkills()
            } else {
                message.error(res.data.error || '保存失败')
            }
        } catch (error) {
            message.error('保存失败')
        } finally {
            setCreateLoading(false)
        }
    }

    // --- Edit/Detail Logic ---
    const openDetail = (skill: Skill) => {
        setCurrentSkill(skill)
        editForm.setFieldsValue({
            name: skill.name,
            description: skill.description,
            configuration: skill.configuration
        })
        setTestParams('{}')
        setTestResult(null)
        setDetailTab('info')
        setDrawerVisible(true)
    }

    const handleUpdate = async () => {
        if (!currentSkill) return
        try {
            const values = await editForm.validateFields()
            await skillsApi.update(currentSkill.skill_id, values)
            message.success('更新成功')
            // Refresh local state
            const updated = { ...currentSkill, ...values }
            setCurrentSkill(updated)
            fetchSkills()
        } catch (error) {
            message.error('更新失败')
        }
    }

    const handleRunTest = async () => {
        if (!currentSkill) return
        try {
            let params = {}
            try {
                params = JSON.parse(testParams)
            } catch (e) {
                message.error('测试参数必须是有效的 JSON')
                return
            }

            setTestLoading(true)
            const res = await skillsApi.test(currentSkill.skill_id, params)
            setTestLoading(false)

            if (res.data.success) {
                setTestResult(res.data.result)
                message.success(`测试完成(${res.data.execution_time_ms}ms)`)
            } else {
                setTestResult({ error: res.data.error, ...res.data.result })
                message.error('测试执行出错')
            }
        } catch (error) {
            setTestLoading(false)
            message.error('调用测试接口失败')
        }
    }

    const renderSkillCard = (skill: Skill) => (
        <List.Item>
            <Card
                hoverable
                className="skill-card"
                onClick={() => openDetail(skill)}
                actions={[
                    <Tooltip title="状态切换">
                        <div onClick={(e) => { e.stopPropagation(); handleToggleStatus(skill) }}>
                            {skill.is_active ? <CloudServerOutlined style={{ color: '#52c41a' }} /> : <RocketOutlined />}
                        </div>
                    </Tooltip>,
                    <Tooltip title="查看/编辑">
                        <CodeOutlined key="edit" onClick={(e) => { e.stopPropagation(); openDetail(skill) }} />
                    </Tooltip>,
                    <Tooltip title="删除">
                        <DeleteOutlined key="delete" style={{ color: '#ff4d4f' }} onClick={(e) => { e.stopPropagation(); handleDelete(skill.skill_id) }} />
                    </Tooltip>
                ]}
            >
                <Card.Meta
                    title={
                        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <span>{skill.name}</span>
                            <Badge status={skill.is_active ? "success" : "warning"} text={skill.is_active ? "Active" : "Shadow"} />
                        </div>
                    }
                    description={
                        <>
                            <Text type="secondary" style={{ fontSize: 12 }}>{skill.skill_id}</Text>
                            <Paragraph ellipsis={{ rows: 2 }} style={{ marginTop: 8, minHeight: 44 }}>
                                {skill.description || '暂无描述'}
                            </Paragraph>
                            <Space size={[0, 4]} wrap>
                                {skill.tags?.map(tag => (
                                    <Tag key={tag} color="blue">{tag}</Tag>
                                ))}
                            </Space>
                        </>
                    }
                />
            </Card>
        </List.Item>
    )

    return (
        <Layout style={{ padding: '24px', background: 'transparent' }}>
            <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Title level={2} style={{ margin: 0 }}>Skill 管理中心</Title>
                <Space>
                    <Button icon={<ReloadOutlined />} onClick={fetchSkills}>刷新</Button>
                    <Button type="primary" icon={<PlusOutlined />} onClick={() => setCreateModalVisible(true)}>新建 Skill</Button>
                </Space>
            </div>

            <Card bordered={false}>
                <Tabs activeKey={currentTab} onChange={setCurrentTab}>
                    <TabPane tab="全部 Skills" key="all" />
                    <TabPane tab="已激活 (Active)" key="active" />
                    <TabPane tab="测试中 (Shadow)" key="shadow" />
                </Tabs>

                <div style={{ marginBottom: 16 }}>
                    <Input
                        placeholder="搜索 Skill 名称或 ID..."
                        prefix={<SearchOutlined />}
                        value={searchText}
                        onChange={e => setSearchText(e.target.value)}
                        allowClear
                    />
                </div>

                <List
                    grid={{ gutter: 16, xs: 1, sm: 2, md: 2, lg: 3, xl: 3, xxl: 4 }}
                    dataSource={skills.filter(s =>
                        s.name.includes(searchText) || s.skill_id.includes(searchText)
                    )}
                    loading={loading}
                    renderItem={renderSkillCard}
                />
            </Card>

            {/* Create Modal */}
            <Modal
                title="新建 Skill"
                open={createModalVisible}
                onCancel={() => setCreateModalVisible(false)}
                footer={null}
                width={700}
            >
                <Tabs activeKey={createMode} onChange={(k: any) => setCreateMode(k)}>
                    <TabPane tab={<span><RobotOutlined />AI 生成</span>} key="ai">
                        <Form form={aiForm} layout="vertical" onFinish={handleCreateAI}>
                            <Form.Item name="requirement" label="需求描述" rules={[{ required: true, message: '请输入需求' }]}>
                                <TextArea rows={6} placeholder="例如：创建一个逾期风险监测 Skill，查询所有逾期超过30天且未清的贷款..." />
                            </Form.Item>
                            <Form.Item name="mode" label="初始模式" initialValue="shadow">
                                <Select>
                                    <Select.Option value="shadow">Shadow Mode (测试)</Select.Option>
                                    <Select.Option value="active">Active (直接激活)</Select.Option>
                                </Select>
                            </Form.Item>
                            <Form.Item>
                                <Button type="primary" htmlType="submit" loading={createLoading} block>生成 Skill</Button>
                            </Form.Item>
                        </Form>
                    </TabPane>
                    <TabPane tab={<span><CodeOutlined />手动配置</span>} key="manual">
                        <Form form={manualForm} layout="vertical" onFinish={handleCreateManual}>
                            <Form.Item name="configuration" label="YAML 配置" rules={[{ required: true }]}>
                                <TextArea rows={12} style={{ fontFamily: 'monospace' }} placeholder="skill_id: my_skill..." />
                            </Form.Item>
                            <Form.Item name="mode" label="初始模式" initialValue="shadow">
                                <Select>
                                    <Select.Option value="shadow">Shadow Mode (测试)</Select.Option>
                                    <Select.Option value="active">Active (直接激活)</Select.Option>
                                </Select>
                            </Form.Item>
                            <Form.Item>
                                <Button type="primary" htmlType="submit" loading={createLoading} block>保存 Skill</Button>
                            </Form.Item>
                        </Form>
                    </TabPane>
                </Tabs>
            </Modal>

            {/* Detail Drawer */}
            <Drawer
                title={currentSkill?.name || 'Skill 详情'}
                width={800}
                open={drawerVisible}
                onClose={() => setDrawerVisible(false)}
                extra={
                    <Space>
                        <Button danger onClick={() => currentSkill && handleDelete(currentSkill.skill_id)}>删除</Button>
                        <Button type="primary" onClick={handleUpdate}>保存修改</Button>
                    </Space>
                }
            >
                <Tabs activeKey={detailTab} onChange={setDetailTab}>
                    <TabPane tab="基本信息" key="info">
                        <Form form={editForm} layout="vertical">
                            <Form.Item label="Skill ID">
                                <Input value={currentSkill?.skill_id} disabled />
                            </Form.Item>
                            <Form.Item name="name" label="名称" rules={[{ required: true }]}>
                                <Input />
                            </Form.Item>
                            <Form.Item name="description" label="描述">
                                <TextArea rows={4} />
                            </Form.Item>
                            <Form.Item label="当前状态">
                                <Tag color={currentSkill?.is_active ? 'green' : 'orange'}>
                                    {currentSkill?.is_active ? '已激活 (Active)' : '测试中 (Shadow)'}
                                </Tag>
                                <Button type="link" onClick={() => currentSkill && handleToggleStatus(currentSkill)}>
                                    切换状态
                                </Button>
                            </Form.Item>
                        </Form>
                    </TabPane>
                    <TabPane tab="YAML 配置" key="config">
                        <Form form={editForm} layout="vertical">
                            <Form.Item name="configuration" noStyle>
                                <TextArea rows={24} style={{ fontFamily: 'monospace' }} />
                            </Form.Item>
                        </Form>
                    </TabPane>
                    <TabPane tab="测试执行" key="test">
                        <Space direction="vertical" style={{ width: '100%' }}>
                            <div>
                                <Text strong>测试参数 (JSON)</Text>
                                <TextArea
                                    value={testParams}
                                    onChange={e => setTestParams(e.target.value)}
                                    rows={4}
                                    style={{ fontFamily: 'monospace', marginTop: 8 }}
                                />
                            </div>
                            <Button type="primary" icon={<PlayCircleOutlined />} onClick={handleRunTest} loading={testLoading}>
                                执行测试
                            </Button>
                            {testResult && (
                                <Card title="测试结果" size="small" style={{ marginTop: 16, background: '#f5f5f5' }}>
                                    <pre style={{ maxHeight: 400, overflow: 'auto' }}>
                                        {JSON.stringify(testResult, null, 2)}
                                    </pre>
                                </Card>
                            )}
                        </Space>
                    </TabPane>
                </Tabs>
            </Drawer>
        </Layout>
    )
}
